"""Broker connector service entry point.

Reads the connector list from config and spawns one thread per connector.
Two connectors on the same broker are fine as long as they have different
``broker.group_id`` values (enforced at startup).
"""

import threading
import time

from baselayer.app.env import load_env
from baselayer.app.models import init_db
from baselayer.log import make_log
from skyportal.utils.services import check_loaded

from .connector import KafkaConnector
from .parsers import get_parser
from .sink import SkyPortalSink

env, cfg = load_env()

init_db(**cfg["database"])

log = make_log("broker_connector")
log_verbose = make_log("broker_connector_verbose")


def _validate_connectors(connectors: list):
    """Fail fast on duplicate (broker_servers, group_id) pairs."""
    seen = {}
    for c in connectors:
        key = (c["broker"]["servers"], c["broker"]["group_id"])
        if key in seen:
            raise ValueError(
                f"Connectors {seen[key]!r} and {c['name']!r} share the same "
                f"broker+group_id {key!r}. Use distinct group_ids for different "
                "use-cases on the same broker."
            )
        seen[key] = c["name"]


def build_connector(cfg: dict, log=None) -> KafkaConnector:
    """Construct a KafkaConnector from a single connector config dict."""
    name = cfg["name"]

    parser = get_parser(cfg["parser"])

    sp_cfg = cfg["skyportal"]
    token = sp_cfg["token"]

    taxonomy_cfg = None
    if "taxonomy" in cfg:
        tax_path = cfg["taxonomy"]
        if isinstance(tax_path, str):
            import yaml

            with open(tax_path) as f:
                taxonomy_cfg = yaml.safe_load(f)
        else:
            taxonomy_cfg = tax_path

    sink = SkyPortalSink(
        url=sp_cfg["url"],
        token=token,
        group=sp_cfg["group"],
        stream=sp_cfg.get("stream"),
        filter=sp_cfg.get("filter"),
        taxonomy=taxonomy_cfg,
        whitelisted=sp_cfg.get("whitelisted", False),
        log=log,
    )

    return KafkaConnector(
        name=name,
        broker=cfg["broker"],
        topics=cfg["topics"],
        parser=parser,
        sink=sink,
        poll_timeout=cfg.get("poll_timeout", 5.0),
        log=log,
    )


def run_service(connectors_cfg: list):
    """Spawn one thread per connector and block until all exit."""
    _validate_connectors(connectors_cfg)

    threads = []
    for cfg in connectors_cfg:
        connector = build_connector(cfg, log=log)
        t = threading.Thread(target=connector.run, name=cfg["name"], daemon=True)
        threads.append(t)

    for t in threads:
        t.start()

    try:
        while any(t.is_alive() for t in threads):
            time.sleep(1)
    except KeyboardInterrupt:
        pass


@check_loaded(logger=log)
def service(*args, **kwargs):
    """Entry point when run as a SkyPortal service."""

    connectors_cfg = cfg.get("app.broker_connectors", [])
    if not connectors_cfg:
        log("No broker_connectors defined in config — nothing to do.")
        return

    run_service(connectors_cfg)


if __name__ == "__main__":
    try:
        service()
    except Exception as e:
        log(f"Error starting broker connector: {str(e)}")
        raise e
