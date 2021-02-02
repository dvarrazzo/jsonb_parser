"""
jsonb_parser -- parse the jsonb PostgreSQL binary format to Python objects.
"""

# Copyright (C) 2021 Daniele Varrazzo


import logging

from .jsonb import parse_jsonb

logger = logging.getLogger(__name__)
logging.basicConfig()

# Try to import the optimised extension
try:
    from . import _parser  # type: ignore[attr-defined]
except ImportError:
    logger.warning("c extension not available")
else:
    parse_jsonb = _parser.parse_jsonb  # noqa[F811]
