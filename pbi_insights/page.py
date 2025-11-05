from __future__ import annotations
import json
from typing import List, Dict, Any, Optional, TYPE_CHECKING, Set
from .utils import _recursive_find_fields
from .visual import Visual

if TYPE_CHECKING:
    from .report import Report
    from .measure import Measure


class Page:
    """Represents a single page (or 'section') within a Power BI report."""

    def __init__(self, section_data: Dict[str, Any], report: Report):
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
        self.description: Optional[str] = None

        # --- Sizing and Display Attributes ---
        self.width: Optional[float] = section_data.get("width")
        self.height: Optional[float] = section_data.get("height")
        self.is_visible: Optional[bool] = section_data.get("displayOption") == 1

        config_str: Optional[str] = section_data.get("config")
        self.config: Dict[str, Any] = json.loads(config_str) if config_str else {}

        # --- Page-level Filters ---
        # The 'filters' attribute is a JSON string that needs to be parsed.
        filters_str: Optional[str] = section_data.get("filters")
        self.filters: List[Dict[str, Any]] = json.loads(filters_str) if filters_str else []

        # --- Parent Reference ---
        self.report: Report = report

        # --- Contained Objects ---
        self.visuals: List[Visual] = []
        self._load_visuals(section_data)
        self.used_fields: Set[str] = set()
        self._find_used_fields()
        self._reformat_used_fields()
        self.used_measures: Set[Measure] = set()
        self.visual_titles: List[str] = []
        self._find_all_visual_titles()

    def _load_visuals(self, section_data: Dict[str, Any]):
        """Parses visual containers from section data and populates self.visuals."""
        visual_containers = section_data.get("visualContainers",[])
        if not visual_containers:
            return

        for container in visual_containers:
            visual = Visual(container, self)
            self.visuals.append(visual)


    def _find_used_fields(self):
        """
        Aggregates and saves a unique set of all measures used across all visuals on this page.
        """

        for visual in self.visuals:
            self.used_fields.update(visual.used_fields)

        if self.filters:
            self.used_fields.update(_recursive_find_fields(self.filters))

    def _find_all_visual_titles(self):
        """Collects the titles of all visuals within the page"""
        for visual in self.visuals:
            if visual.title is not None and visual.title != "":
                self.visual_titles.append(visual.title)


    def _reformat_used_fields(self):
        """
        Converts field names from 'Entity.Property' format to 'Entity[Property]'.

        This standardizes the format for easier lookups against the report's
        central measures dictionary, which uses the 'Entity[Property]' format as keys.
        """
        reformatted = set()
        for field in self.used_fields:
            parts = field.split('.', 1)
            if len(parts) == 2:
                reformatted.add(f"{parts[0]}[{parts[1]}]")
            else:
                print(f"We have a wrong field format with {field}")
        self.used_fields = reformatted


    def __repr__(self) -> str:
        """Provides a developer-friendly string representation of the Page object."""
        return f"Page(name='{self.name}', ordinal={self.ordinal}, visuals={len(self.visuals)})"

    def __hash__(self) -> int:
        """The hash is based on the unique combination of page's name and ordinal."""
        return hash((self.ordinal, self.name))