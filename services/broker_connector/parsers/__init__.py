from .base import BaseParser
from .fink_lsst import FinkLSSTParser
from .fink_ztf import FinkZTFParser

_REGISTRY = {
    "fink_ztf": FinkZTFParser,
    "fink_lsst": FinkLSSTParser,
}


def get_parser(name: str) -> BaseParser:
    """Resolve a parser name to an instance.

    Raises ValueError for unknown names so the error surfaces at startup,
    not mid-run.
    """
    cls = _REGISTRY.get(name)
    if cls is None:
        raise ValueError(f"Unknown parser {name!r}. Available: {list(_REGISTRY)}")
    return cls()
