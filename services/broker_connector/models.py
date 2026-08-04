from dataclasses import dataclass, field


@dataclass
class AlertData:
    """Normalized, broker-agnostic representation of a single astronomical alert."""

    object_id: str
    ra: float
    dec: float
    mjd: float
    instruments: list
    filter: str
    magsys: str
    # Magnitude-space photometry (e.g. ZTF)
    mag: float = None
    magerr: float = None
    limiting_mag: float = None
    # Flux-space photometry (e.g. LSST, in nJy)
    flux: float = None
    fluxerr: float = None
    zp: float = None
    classification: str = None
    probability: float = None
