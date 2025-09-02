"""
Hermes SkyPortal Synchronization Service

This service consumes source data from Hermes Kafka topics
and synchronizes it with SkyPortal by creating sources and uploading photometry.

Configuration is loaded from config.yaml file in the service directory.
"""

import json
import logging
import os
import time
from datetime import datetime, timedelta, timezone
from typing import Any

import requests
import yaml
from confluent_kafka import Consumer

from baselayer.app.env import load_env
from baselayer.app.models import init_db
from baselayer.log import make_log

env, cfg = load_env()
init_db(**cfg["database"])

log = make_log("hermes_sync")

service_config_path = os.path.join(os.path.dirname(__file__), "config.yaml")
if os.path.exists(service_config_path):
    with open(service_config_path) as f:
        service_cfg = yaml.safe_load(f)
    hermes_config = service_cfg.get("hermes_sync", {})
else:
    log("Warning: config.yaml not found, using default configuration")
    hermes_config = {}

SERVER_URL = hermes_config.get("server_url", "kafka.scimma.org")
TOPIC = hermes_config.get("topic", "skyportal.skyportal_test")
FROM_START = hermes_config.get("from_start", False)
MAX_AGE_DAYS = hermes_config.get("max_age_days")
USERNAME = hermes_config.get("username")
PASSWORD = hermes_config.get("password")
SKYPORTAL_API_URL = hermes_config.get("skyportal_url", "http://localhost:5000")
SKYPORTAL_TOKEN = hermes_config.get("skyportal_token")
DEFAULT_GROUP_IDS = hermes_config.get("group_ids", [1])
DEFAULT_INSTRUMENT_ID = hermes_config.get("instrument_id", 1)
LOG_LEVEL = hermes_config.get("log_level", "INFO")
DRY_RUN = hermes_config.get("dry_run", False)


class SkyPortalAPI:
    """SkyPortal API client for Kafka monitor service.
    Handles source existence checks, source creation, and photometry data upload."""

    def __init__(self, api_url: str, token: str):
        self.api_url = api_url.rstrip("/")
        if not self.api_url.endswith("/api"):
            self.api_url += "/api"

        self.token = token
        self.session = requests.Session()
        self.session.headers.update(
            {"Authorization": f"token {token}", "Content-Type": "application/json"}
        )

        logging.info(f"Initializing SkyPortal API client for: {self.api_url}")
        self._test_connection()

    def _test_connection(self) -> None:
        """Test the connection to SkyPortal API."""
        try:
            response = self.session.get(
                f"{self.api_url}/sources?numPerPage=1", timeout=10
            )
            if response.status_code == 200:
                logging.info("Connected to SkyPortal API successfully")
            else:
                logging.error(
                    f"SkyPortal API connection failed with status {response.status_code}"
                )
                raise requests.exceptions.RequestException(
                    f"API returned status {response.status_code}"
                )
        except requests.exceptions.RequestException as e:
            logging.error(f"Failed to connect to SkyPortal API: {e}")
            raise

    def get_source(self, obj_id: str) -> dict[str, Any] | None:
        """Get source information from SkyPortal."""
        try:
            response = self.session.get(f"{self.api_url}/sources/{obj_id}", timeout=10)

            if response.status_code == 200:
                json_data = response.json()
                return json_data.get("data")
            elif response.status_code == 404:
                return None
            else:
                logging.error(
                    f"Error getting source {obj_id}: HTTP {response.status_code}"
                )
                return None

        except requests.exceptions.RequestException as e:
            logging.error(f"Network error getting source {obj_id}: {e}")
            return None
        except (ValueError, KeyError) as e:
            logging.error(f"Error parsing response for {obj_id}: {e}")
            return None

    def create_source(
        self, source_data: dict[str, Any], group_ids: list[int] = None
    ) -> bool:
        """Create a new source in SkyPortal."""
        try:
            payload = {
                "id": source_data["id"],
                "ra": source_data["ra"],
                "dec": source_data["dec"],
            }

            if group_ids:
                payload["group_ids"] = group_ids

            response = self.session.post(
                f"{self.api_url}/sources", json=payload, timeout=30
            )

            if response.status_code == 200:
                logging.info(f"Successfully created source {source_data['id']}")
                return True
            else:
                logging.error(
                    f"Failed to create source {source_data['id']}: HTTP {response.status_code}"
                )
                return False

        except requests.exceptions.RequestException as e:
            logging.error(f"Error creating source {source_data['id']}: {e}")
            return False

    def add_photometry(
        self,
        obj_id: str,
        photometry_data: list[dict[str, Any]],
        instrument_id: int,
        group_ids: list[int],
    ) -> bool:
        """Add photometry points to an existing source."""
        try:
            photometry_payload = {
                "obj_id": obj_id,
                "instrument_id": instrument_id,
                "group_ids": group_ids,
                "mjd": [],
                "mag": [],
                "magerr": [],
                "limiting_mag": [],
                "filter": [],
                "origin": [],
                "magsys": "ab",
            }

            # Convert each photometry point from hermes payload to skyportal payload
            for point in photometry_data:
                date_obs = point.get("date_obs")
                brightness = point.get("brightness")
                brightness_error = point.get("brightness_error")
                bandpass = point.get("bandpass")
                limiting_brightness = point.get("limiting_brightness")
                origin = point.get("origin")

                # Only add points with required data
                if date_obs is not None and brightness is not None and bandpass:
                    jd = float(date_obs)
                    mjd = round(jd - 2400000.5, 7)

                    photometry_payload["mjd"].append(mjd)
                    photometry_payload["mag"].append(float(brightness))
                    photometry_payload["magerr"].append(
                        float(brightness_error) if brightness_error else None
                    )
                    photometry_payload["limiting_mag"].append(
                        float(limiting_brightness) if limiting_brightness else None
                    )
                    photometry_payload["filter"].append(str(bandpass))
                    photometry_payload["origin"].append(origin if origin else "")

            valid_points = len(photometry_payload["mjd"])
            if valid_points == 0:
                logging.warning(f"No valid photometry points found for {obj_id}")
                return False

            logging.debug(
                f"Sending {valid_points} photometry points for {obj_id} "
                f"(JD converted to MJD with 7 decimal precision, ignore_flux=True)"
            )

            response = self.session.put(
                f"{self.api_url}/photometry",
                json=photometry_payload,
                params={
                    "refresh": True,
                    "duplicate_ignore_flux": True,
                    "overwrite_flux": False,
                },
                timeout=30,
            )

            if response.status_code == 200:
                response_data = response.json() if response.content else {}
                num_processed = response_data.get("data", {}).get("ids", [])
                actual_count = (
                    len(num_processed)
                    if isinstance(num_processed, list)
                    else valid_points
                )

                logging.info(
                    f"✓ Successfully processed {actual_count}/{valid_points} photometry points for {obj_id}"
                )

                if actual_count < valid_points:
                    skipped = valid_points - actual_count
                    logging.info(
                        f"  → {skipped} points were likely duplicates (ignoring flux differences)"
                    )

                return True
            else:
                logging.error(
                    f"Failed to add photometry to {obj_id}: HTTP {response.status_code}"
                )
                logging.error(f"Response text: {response.text}")
                return False

        except requests.exceptions.RequestException as e:
            logging.error(f"Network error adding photometry to {obj_id}: {e}")
            return False


class SourceProcessor:
    """Handles processing of astronomical source data from Kafka messages."""

    def __init__(
        self,
        skyportal_api: SkyPortalAPI | None = None,
        default_group_ids: list[int] = None,
        default_instrument_id: int = 1,
    ):
        self.skyportal_api = skyportal_api
        self.default_group_ids = default_group_ids or []
        self.default_instrument_id = default_instrument_id
        self.processed_sources = set()
        self.message_count = 0
        self.created_sources = 0
        self.errors = 0

    def process_message(self, data: dict[str, Any]) -> None:
        """Process a single Kafka message containing source data."""
        self.message_count += 1

        logging.info(f"Processing message #{self.message_count}")

        # Extract metadata
        submitter = data.get("submitter", "Unknown")
        authors = data.get("authors", "Unknown")
        title = data.get("title", "No title")

        logging.info(f"Title: {title}")
        logging.info(f"Submitter: {submitter}")
        logging.info(f"Authors: {authors}")

        # Extract targets and photometry
        targets = data.get("data", {}).get("targets", [])
        photometry = data.get("data", {}).get("photometry", [])

        logging.info(f"Number of targets: {len(targets)}")

        # Process each target
        for target in targets:
            self._process_target(target, photometry)

    def _process_target(
        self, target: dict[str, Any], photometry: list[dict[str, Any]]
    ) -> None:
        """Process a single astronomical target."""
        name = target.get("name", "Unknown_target")
        ra = target.get("ra")
        dec = target.get("dec")

        logging.info(f"  Target: {name}")
        logging.info(f"    RA: {ra}")
        logging.info(f"    Dec: {dec}")

        # Filter photometry for this specific target
        target_photometry = [p for p in photometry if p.get("target_name") == name]

        if target_photometry:
            self._display_photometry_summary(target_photometry)
        else:
            logging.info("    No photometry data for this target")

        # Add to processed sources
        self.processed_sources.add(name)

        # Process with SkyPortal if API is available
        if self.skyportal_api:
            self._sync_with_skyportal(name, target, target_photometry)
        else:
            logging.debug(
                f"    → Skipping SkyPortal sync for {name} (API not available)"
            )

    def _sync_with_skyportal(
        self, obj_id: str, target: dict[str, Any], photometry: list[dict[str, Any]]
    ) -> None:
        """Synchronize source data with SkyPortal."""
        try:
            # Check if source exists in SkyPortal
            existing_source = self.skyportal_api.get_source(obj_id)

            if existing_source is None:
                # Source doesn't exist, create it
                logging.info(f"    → Creating new source in SkyPortal: {obj_id}")

                source_data = {
                    "id": obj_id,
                    "ra": target.get("ra"),
                    "dec": target.get("dec"),
                }

                success = self.skyportal_api.create_source(
                    source_data, self.default_group_ids
                )
                if success:
                    self.created_sources += 1
                else:
                    self.errors += 1
                    return

            else:
                logging.info(f"    → Source exists in SkyPortal: {obj_id}")

            # Always try to add photometry - let SkyPortal handle duplicates
            if photometry:
                logging.info(
                    f"    → Adding {len(photometry)} photometry points "
                    f"(instrument_id={self.default_instrument_id}, SkyPortal will filter duplicates)"
                )
                success = self.skyportal_api.add_photometry(
                    obj_id,
                    photometry,
                    self.default_instrument_id,
                    self.default_group_ids,
                )
                if not success:
                    self.errors += 1

        except Exception as e:
            logging.error(f"    ✗ Error syncing {obj_id} with SkyPortal: {e}")
            self.errors += 1

    def _display_photometry_summary(self, photometry: list[dict[str, Any]]) -> None:
        """Display a summary of photometry data."""
        if not photometry:
            return

        instruments = {}
        for p in photometry:
            telescope = p.get("telescope", "Unknown")
            instrument = p.get("instrument", "Unknown")
            key = f"{telescope}/{instrument}"
            if key not in instruments:
                instruments[key] = []
            instruments[key].append(p)

        for inst_key, points in instruments.items():
            logging.info(f"      {inst_key}: {len(points)} points")

            # Show filters used
            filters = {p.get("bandpass") for p in points if p.get("bandpass")}
            if filters:
                logging.info(f"        Filters: {', '.join(sorted(filters))}")

    def get_stats(self) -> dict[str, Any]:
        """Return processing statistics."""
        return {
            "messages_processed": self.message_count,
            "unique_sources": len(self.processed_sources),
            "sources_created": self.created_sources,
            "errors": self.errors,
            "source_names": list(self.processed_sources),
        }


class HermesSyncService:
    """Main service for consuming Hermes messages and synchronizing with SkyPortal."""

    def __init__(
        self,
        username: str,
        password: str,
        from_start: bool = False,
        max_age_days: float | None = None,
        skyportal_url: str | None = None,
        skyportal_token: str | None = None,
        group_ids: list[int] | None = None,
        instrument_id: int = 1,
    ):
        self.username = username
        self.password = password
        self.from_start = from_start
        self.max_age_days = max_age_days
        self.consumer = None

        # Initialize SkyPortal API if credentials provided
        self.skyportal_api = None
        if skyportal_url and skyportal_token:
            try:
                self.skyportal_api = SkyPortalAPI(skyportal_url, skyportal_token)
                logging.info("SkyPortal API integration enabled")
            except Exception as e:
                logging.error(f"Failed to initialize SkyPortal API: {e}")
                logging.warning("Continuing without SkyPortal integration...")
                self.skyportal_api = None

        # Initialize processor with SkyPortal integration
        self.processor = SourceProcessor(
            skyportal_api=self.skyportal_api,
            default_group_ids=group_ids or [],
            default_instrument_id=instrument_id,
        )

    def build_consumer(self) -> Consumer:
        """Build and configure the Kafka consumer."""
        group_id = f"{self.username}-{TOPIC}-monitor"
        if self.from_start:
            group_id += f"-{int(time.time())}"

        conf = {
            "bootstrap.servers": SERVER_URL,
            "group.id": group_id,
            "security.protocol": "SASL_SSL",
            "sasl.mechanisms": "SCRAM-SHA-512",
            "sasl.username": self.username,
            "sasl.password": self.password,
            "auto.offset.reset": "earliest",
            "enable.partition.eof": False,
            "log_level": 2,
        }
        return Consumer(conf)

    def is_too_old(self, ts_ms: int) -> bool:
        """Check if message is too old based on MAX_AGE_DAYS."""
        if self.max_age_days is None:
            return False

        msg_dt = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
        max_age = timedelta(days=self.max_age_days)
        return datetime.now(timezone.utc) - msg_dt > max_age

    def run(self) -> None:
        """Main monitoring loop."""
        logging.info("Starting Hermes SkyPortal Synchronization Service")
        logging.info(f"Topic: {TOPIC}")
        logging.info(f"Username: {self.username}")
        logging.info(f"Read from start: {self.from_start}")
        logging.info(f"Max age (days): {self.max_age_days}")

        self.consumer = self.build_consumer()
        self.consumer.subscribe([TOPIC])
        logging.info(f"Subscribed to {TOPIC}")
        logging.info("Waiting for messages... (Press Ctrl+C to stop)")

        try:
            while True:
                msg = self.consumer.poll(1.0)  # 1 second timeout

                if msg is None:
                    continue

                if msg.error():
                    logging.error(f"Kafka error: {msg.error()}")
                    continue

                # Check message age
                ts = msg.timestamp()[1]
                if ts > 0 and self.is_too_old(ts):
                    logging.debug(f"Skipping old message (offset {msg.offset()})")
                    continue

                self._process_kafka_message(msg)

        except KeyboardInterrupt:
            logging.info("\nInterrupted by user")
        finally:
            self._cleanup()

    def _process_kafka_message(self, msg) -> None:
        """Process a single Kafka message."""
        try:
            ts = msg.timestamp()[1]
            if ts > 0:
                msg_timestamp = datetime.fromtimestamp(ts / 1000, tz=timezone.utc)
                logging.info("=" * 60)
                logging.info(f"Message timestamp: {msg_timestamp.isoformat()}")

            payload = msg.value()
            if not payload:
                logging.warning("Empty payload received")
                return

            try:
                data = json.loads(payload)
            except json.JSONDecodeError as e:
                logging.error(f"JSON parse error: {e}")
                logging.error(f"Raw payload: {payload[:200]}...")
                return

            self.processor.process_message(data)

        except Exception as e:
            logging.error(f"Error processing message: {e}")

    def _cleanup(self) -> None:
        """Clean up resources and print final statistics."""
        if self.consumer:
            self.consumer.close()

        stats = self.processor.get_stats()
        logging.info("\n" + "=" * 60)
        logging.info("FINAL STATISTICS")
        logging.info(f"Messages processed: {self.processor.message_count}")
        logging.info(f"Unique sources found: {len(self.processor.processed_sources)}")

        if self.skyportal_api:
            logging.info(
                f"Sources created in SkyPortal: {self.processor.created_sources}"
            )
            logging.info(
                f"Sources that may have been concerned by an update: {sorted(self.processor.processed_sources)}"
            )

        if self.skyportal_api and self.processor.errors > 0:
            logging.info(
                f"There were {self.processor.errors} errors during processing. Check logs above for details."
            )


def main():
    """Main entry point."""
    logging.basicConfig(
        level=getattr(logging, LOG_LEVEL),
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    if not USERNAME or not PASSWORD:
        logging.error(
            "Missing required SCiMMA credentials. Please configure 'username' and 'password' in config.yaml"
        )
        return

    skyportal_url = None
    skyportal_token = None

    if DRY_RUN:
        logging.info("Running in dry-run mode (no SkyPortal synchronization)")
    else:
        if SKYPORTAL_TOKEN:
            skyportal_url = SKYPORTAL_API_URL
            skyportal_token = SKYPORTAL_TOKEN
            logging.info("SkyPortal credentials configured, attempting connection...")
        else:
            logging.error(
                "Missing SkyPortal token. Configure 'skyportal_token' in config.yaml or set 'dry_run: true'"
            )
            return

    # Create and run sync service
    sync_service = HermesSyncService(
        username=USERNAME,
        password=PASSWORD,
        from_start=FROM_START,
        max_age_days=MAX_AGE_DAYS,
        skyportal_url=skyportal_url,
        skyportal_token=skyportal_token,
        group_ids=DEFAULT_GROUP_IDS,
        instrument_id=DEFAULT_INSTRUMENT_ID,
    )

    sync_service.run()


if __name__ == "__main__":
    main()
