import io

import fastavro
import pandas as pd
from astropy.time import Time

from ..models import AlertData
from .base import BaseParser

_KN_TOPICS = {
    "fink_kn_candidates_ztf",
    "fink_early_kn_candidates_ztf",
    "fink_rate_based_kn_candidates_ztf",
}

_FID_TO_FILTER = {1: "ztfg", 2: "ztfr", 3: "ztfi"}


class FinkZTFParser(BaseParser):
    """Parse ZTF alerts from the Fink broker (Avro-encoded)."""

    def decode(self, raw: bytes, key: bytes = None) -> dict | None:
        if raw is None:
            return None
        try:
            reader = fastavro.reader(io.BytesIO(raw))
            return next(reader, None)
        except Exception:
            return None

    def parse(self, topic: str, alert: dict) -> AlertData | None:
        if alert is None:
            return None
        if "objectId" not in alert or "candidate" not in alert:
            return None

        cand = alert["candidate"]
        required = ["jd", "fid", "magpsf", "sigmapsf", "diffmaglim", "ra", "dec"]
        if any(cand.get(k) is None for k in required):
            return None

        try:
            from fink_filters.ztf.classification import (
                extract_fink_classification_from_pdf,
            )

            alert_pd = pd.DataFrame([alert])
            alert_pd["tracklet"] = ""
            classification = extract_fink_classification_from_pdf(alert_pd)[0]
        except Exception:
            classification = None

        if topic in _KN_TOPICS and (
            classification is None or "kilonova" not in classification.lower()
        ):
            classification = "Kilonova candidate"

        return AlertData(
            object_id=alert["objectId"],
            ra=cand["ra"],
            dec=cand["dec"],
            mjd=Time(cand["jd"], format="jd").mjd,
            instruments=["CFH12k", "ZTF"],
            filter=_FID_TO_FILTER.get(cand["fid"]),
            magsys="ab",
            mag=cand["magpsf"],
            magerr=cand["sigmapsf"],
            limiting_mag=cand["diffmaglim"],
            classification=classification,
        )
