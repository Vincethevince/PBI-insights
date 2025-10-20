from __future__ import annotations
import json
from typing import List, Dict, Any, Optional, TYPE_CHECKING
from pathlib import Path

if TYPE_CHECKING:
    from .page import Page
    from .measure import Measure


class Report:
    """Represents a Power BI report, parsed from its unzipped file structure."""

    def __init__(self, name: str, layout_data: Dict[str, Any]):
        """
        Initializes a Report object from its layout data.

        Args:
            name: The name of the report (derived from its folder name).
            layout_data: The parsed JSON data from the 'Layout' file.
        """
        # --- Core Attributes ---
        self.name: str = name

        # --- Attributes from Layout file ---
        config_str: Optional[str] = layout_data.get("config")
        self.config: Dict[str, Any] = json.loads(config_str) if config_str else {}

        global_filters_str: Optional[str] = layout_data.get("filters")
        self.global_filters: List[Dict[str, Any]] = json.loads(global_filters_str) if global_filters_str else []

        # --- Child Objects ---
        self.pages: List[Page] = []
        self._load_pages(layout_data)

        # --- Other Attributes ---
        self.id: Optional[int] = layout_data.get("id")
        self.resourcePackages: Optional[List[Dict[str, Any]]] = layout_data.get("resourcePackages")
        self.layoutOptimization: Optional[int] = layout_data.get("layoutOptimization")

    @classmethod
    def from_unzipped_report(cls, report_path: Path | str) -> 'Report':
        """
        Factory method to create a Report instance from the path to an unzipped .pbix folder.

        Args:
            report_path: The path to the directory containing the unzipped report files.

        Returns:
            A fully initialized Report instance.
        """
        if isinstance(report_path, str):
            report_path = Path(report_path)

        report_name = report_path.name
        layout_file = report_path / "Report" / "Layout"

        if not layout_file.exists():
            raise FileNotFoundError(f"Layout file not found for report '{report_name}' at {layout_file}")

        try:
            with open(layout_file, 'r', encoding='utf-16-le') as f:
                layout_data = json.load(f)
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            raise ValueError(f"Error reading or parsing Layout file for report '{report_name}': {e}") from e

        return cls(name=report_name, layout_data=layout_data)

    def _load_pages(self, layout_data: Dict[str, Any]):
        """
        Parses the 'sections' (pages) from the layout data and populates self.pages.
        """
        page_sections = layout_data.get("sections", [])
        for section_data in page_sections:
            page = Page(section_data, self)
            self.pages.append(page)

    def __repr__(self) -> str:
        """
        Provides a developer-friendly string representation of the Report object.
        """
        return f"Report(name='{self.name}', pages={len(self.pages)})"

