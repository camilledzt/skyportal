"""SkyPortal sink: push normalized AlertData objects to a SkyPortal instance."""

import time

import requests
from astropy.time import Time

from .models import AlertData


def _api(method: str, endpoint: str, data=None, token: str = None):
    headers = {"Authorization": f"token {token}"}
    for attempt in range(5):
        response = requests.request(method, endpoint, json=data, headers=headers)
        if response.status_code != 429:
            return response
        time.sleep(2**attempt)
    return response


class SkyPortalSink:
    """Manages SkyPortal state for one connector and pushes alerts.

    Each connector gets its own SkyPortalSink so that group, stream, filter,
    and (optionally) taxonomy IDs are initialized once and reused.
    """

    def __init__(
        self,
        url: str,
        token: str,
        group: str,
        stream: str = None,
        filter: str = None,
        taxonomy: dict = None,
        whitelisted: bool = False,
        log=None,
    ):
        self.url = url
        self.token = token
        self.group = group
        self.stream_name = stream
        self.filter_name = filter
        self.taxonomy_cfg = taxonomy
        self.whitelisted = whitelisted
        self.log = log or print

        self.group_id = None
        self.stream_id = None
        self.filter_id = None
        self.taxonomy_id = None
        self.instruments = {}  # name → id, populated once in init()

    # ------------------------------------------------------------------
    # Initialisation helpers
    # ------------------------------------------------------------------

    def init(self):
        """Create or retrieve all SkyPortal entities needed for this connector."""
        self.group_id, self.stream_id, self.filter_id = self._init_group()
        if self.taxonomy_cfg:
            self.taxonomy_id = self._init_taxonomy()
        r = _api("GET", f"{self.url}/api/instrument", token=self.token)
        self.instruments = {i["name"]: i["id"] for i in r.json().get("data") or []}
        self.log(
            f"SkyPortal sink ready: group={self.group!r} "
            f"(id={self.group_id}), stream={self.stream_id}, "
            f"filter={self.filter_id}, taxonomy={self.taxonomy_id}, "
            f"instruments={list(self.instruments.keys())}"
        )

    def _init_group(self):
        group_id = self._get_group()
        stream_id = self._get_stream(self.stream_name) if self.stream_name else None
        filter_id = self._get_filter(self.filter_name) if self.filter_name else None
        return group_id, stream_id, filter_id

    def _get_group(self) -> int:
        r = _api("GET", f"{self.url}/api/groups", token=self.token)
        groups = {
            g["name"]: g["id"] for g in r.json()["data"]["user_accessible_groups"]
        }
        if self.group not in groups:
            raise RuntimeError(
                f"Group {self.group!r} not found. Create it in SkyPortal before starting the service."
            )
        return groups[self.group]

    def _get_stream(self, name: str) -> int:
        r = _api("GET", f"{self.url}/api/streams", token=self.token)
        for s in r.json()["data"]:
            if s["name"] == name:
                return s["id"]
        raise RuntimeError(
            f"Stream {name!r} not found. Create it in SkyPortal and link it to group {self.group!r} before starting the service."
        )

    def _get_filter(self, name: str) -> int:
        r = _api("GET", f"{self.url}/api/filters", token=self.token)
        for f in r.json()["data"]:
            if f["name"] == name:
                return f["id"]
        raise RuntimeError(
            f"Filter {name!r} not found. Create it in SkyPortal before starting the service."
        )

    def _init_taxonomy(self) -> int | None:
        name = self.taxonomy_cfg["name"]
        version = self.taxonomy_cfg["version"]
        hierarchy = self.taxonomy_cfg["hierarchy"]

        r = _api("GET", f"{self.url}/api/taxonomy", token=self.token)
        for t in r.json()["data"]:
            if t["name"] == name and t["version"] == version:
                return t["id"]

        r = _api(
            "POST",
            f"{self.url}/api/taxonomy",
            {
                "name": name,
                "hierarchy": hierarchy,
                "version": version,
                "group_ids": [self.group_id],
            },
            token=self.token,
        )
        return r.json()["data"]["taxonomy_id"]

    # ------------------------------------------------------------------
    # Push
    # ------------------------------------------------------------------

    def push(self, alert: AlertData):
        """Post a single normalized alert to SkyPortal. Returns the HTTP status."""
        if not self.whitelisted:
            time.sleep(1)

        object_id = alert.object_id
        if hasattr(object_id, "item"):
            object_id = object_id.item()

        instrument_id = next(
            (
                self.instruments[k]
                for k in self.instruments
                if any(n.lower() in k.lower() for n in alert.instruments)
            ),
            None,
        )
        if instrument_id is None:
            self.log(
                f"No matching instrument for {alert.instruments!r} — skipping {object_id}"
            )
            return 422

        passed_at = Time(alert.mjd, format="mjd").isot

        status = _api(
            "POST",
            f"{self.url}/api/sources",
            {
                "ra": alert.ra,
                "dec": alert.dec,
                "id": object_id,
                "group_ids": [self.group_id],
            },
            token=self.token,
        ).status_code
        if status not in (200, 409):
            self.log(f"post_source {object_id} → {status}")

        _api(
            "POST",
            f"{self.url}/api/candidates",
            {
                "ra": alert.ra,
                "dec": alert.dec,
                "id": object_id,
                "filter_ids": [self.filter_id],
                "passed_at": passed_at,
            },
            token=self.token,
        )

        phot = {
            "ra": alert.ra,
            "dec": alert.dec,
            "obj_id": object_id,
            "mjd": alert.mjd,
            "filter": alert.filter,
            "magsys": alert.magsys,
            "instrument_id": instrument_id,
            "group_ids": [self.group_id],
            "stream_ids": [self.stream_id],
        }
        if alert.flux is not None:
            phot["flux"] = alert.flux
            phot["fluxerr"] = alert.fluxerr
            phot["zp"] = alert.zp
        else:
            phot["mag"] = alert.mag
            phot["magerr"] = alert.magerr
            if alert.limiting_mag is not None:
                phot["limiting_mag"] = alert.limiting_mag

        r = _api("PUT", f"{self.url}/api/photometry", phot, token=self.token)
        if r.status_code != 200:
            self.log(f"post_photometry {object_id} → {r.status_code}: {r.text}")

        if alert.classification and self.taxonomy_id:
            _api(
                "POST",
                f"{self.url}/api/classification",
                {
                    "classification": alert.classification,
                    "taxonomy_id": self.taxonomy_id,
                    "obj_id": object_id,
                    "group_ids": [self.group_id],
                    **(
                        {"probability": alert.probability}
                        if alert.probability is not None
                        else {}
                    ),
                },
                token=self.token,
            )

        self.log(f"Pushed {object_id} → group {self.group!r}")
        return 200
