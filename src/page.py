import json
from typing import List, Dict, Any, Optional, TYPE_CHECKING, Set

if TYPE_CHECKING:
    from .report import Report
    from .visual import Visual
    from .measure import Measure


class Page:
    """Represents a single page (or 'section') within a Power BI report."""

    def __init__(self, section_data: Dict[str, Any], report: 'Report'):
        """
        Initializes a Page object from its JSON section data.

        Args:
            section_data: The dictionary representing a 'section' from the report layout.
            report: A back-reference to the parent Report object.
        """
        # --- Core Attributes ---
        self.id: str = section_data.get("name", "")
        self.name: str = section_data.get("displayName", "Untitled Page")
        self.section: str = section_data.get("name", "ReportSection")
        self.ordinal: Optional[int] = section_data.get("ordinal")

        # --- Sizing and Display Attributes ---
        self.width: Optional[float] = section_data.get("width")
        self.height: Optional[float] = section_data.get("height")
        self.is_visible: Optional[bool] = section_data.get("displayOption") == 1

        config_str: Optional[str] = section_data.get("config")
        self.config: Dict[str, Any] = json.loads(config_str) if config_str else {}

        # --- Parent Reference ---
        self.report: 'Report' = report

        # --- Contained Objects (to be populated by the Report parser) ---
        self.visuals: List['Visual'] = []

        # --- Page-level Filters ---
        # The 'filters' attribute is a JSON string that needs to be parsed.
        filters_str: Optional[str] = section_data.get("filters")
        self.filters: List[Dict[str, Any]] = json.loads(filters_str) if filters_str else []

    def __repr__(self) -> str:
        """Provides a developer-friendly string representation of the Page object."""
        return f"Page(name='{self.name}', ordinal={self.ordinal}, visuals={len(self.visuals)})"

    def add_visual(self, visual: 'Visual'):
        """Adds a visual to this page's collection of visuals."""
        self.visuals.append(visual)

    def get_used_measures(self) -> Set['Measure']:
        """
        Aggregates and returns a unique set of all measures used across all visuals on this page.

        Returns:
            A set of unique Measure objects used on this page.
        """
        page_measures: Set['Measure'] = set()
        for visual in self.visuals:
            page_measures.update(visual.used_measures)
        return page_measures