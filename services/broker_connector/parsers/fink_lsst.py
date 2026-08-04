import io

import fastavro
import numpy as np

from ..models import AlertData
from .base import BaseParser

_BAND_TO_FILTER = {
    "u": "lsstu",
    "g": "lsstg",
    "r": "lsstr",
    "i": "lssti",
    "z": "lsstz",
    "y": "lssty",
}


def _topic_to_classification(topic: str) -> str:
    name = topic
    if name.startswith("fink_"):
        name = name[5:]
    for suffix in ("_ztf", "_lsst"):
        if name.endswith(suffix):
            name = name[: -len(suffix)]
            break
    return " ".join(word.capitalize() for word in name.split("_"))


class FinkLSSTParser(BaseParser):
    """Parse LSST/Rubin alerts from the Fink broker (Avro-encoded)."""

    def decode(self, raw: bytes, key: bytes = None) -> dict | None:
        if raw is None:
            return None
        try:
            # Fink sends the Avro schema as JSON in the message key.
            # The value is schemaless binary — must decode with schemaless_reader.
            if key is not None:
                import json

                schema = fastavro.parse_schema(json.loads(key))
                return fastavro.schemaless_reader(io.BytesIO(raw), schema)
            # Fallback: try self-contained Avro container (e.g. ZTF-style or testing).
            return next(fastavro.reader(io.BytesIO(raw)), None)
        except Exception:
            return None

    def parse(self, topic: str, alert: dict) -> AlertData | None:
        if alert is None:
            return None

        dia = alert.get("diaSource")
        if dia is None:
            return None

        required = ["midpointMjdTai", "band", "psfFlux", "psfFluxErr"]
        if any(dia.get(k) is None for k in required):
            return None
        if dia["psfFlux"] <= 0:
            return None

        diaobj = alert.get("diaObject")
        if diaobj is not None:
            object_id = str(np.int64(diaobj["diaObjectId"]))
            ra = diaobj.get("ra") or dia.get("ra")
            dec = diaobj.get("dec") or dia.get("dec")
        elif alert.get("mpc_orbits") is not None:
            object_id = alert["mpc_orbits"]["designation"]
            ra = dia.get("ra")
            dec = dia.get("dec")
        else:
            return None

        if ra is None or dec is None:
            return None

        filter_ = _BAND_TO_FILTER.get(dia["band"])
        if filter_ is None:
            return None

        return AlertData(
            object_id=object_id,
            ra=ra,
            dec=dec,
            mjd=dia["midpointMjdTai"],
            instruments=["LSSTCam", "LSST"],
            filter=filter_,
            magsys="ab",
            flux=dia["psfFlux"],
            fluxerr=dia["psfFluxErr"],
            zp=31.4,
            classification=_topic_to_classification(topic),
        )
