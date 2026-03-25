"""Parsing utilities: file-system report parsing, unzipping, and field-extraction helpers."""
# NOTE: parser.py is intentionally NOT imported here at package level to avoid a
# circular import (parser → domain.report → domain.page → parsing.utils → parsing.__init__).
# Import directly from the sub-modules when needed:
#   from pbi_insights.parsing.parser import parse_reports
#   from pbi_insights.parsing.unzip  import Unzipper
from .unzip import Unzipper
from .utils import _recursive_find_fields

__all__ = [
    "Unzipper",
    "_recursive_find_fields",
]
