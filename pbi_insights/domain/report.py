from __future__ import annotations
import json
import re
from typing import List, Dict, Any, Optional, Set, TYPE_CHECKING
from pathlib import Path

from .page import Page
from .measure import Measure, UsageState

if TYPE_CHECKING:
    from .page import Page


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

        self.bookmarks: List[Dict] = self.config.get("bookmarks", [])
        self.modelExtension: List[Dict] = self.config.get("modelExtensions", [])

        global_filters_str: Optional[str] = layout_data.get("filters")
        self.global_filters: List[Dict[str, Any]] = json.loads(global_filters_str) if global_filters_str else []

        # --- Child Objects ---
        self.pages: List[Page] = []
        self._load_pages(layout_data)

        # --- Contained Objects ---
        self.measures: Dict[str, Measure] = {}  # fullname: Measure object
        self._load_measures()
        self._resolve_measure_dependencies()
        self._resolve_usage_dependencies()
        self._resolve_measure_usage_states()

        # --- Other Attributes ---
        self.id: Optional[int] = layout_data.get("id")
        self.resourcePackages: Optional[List[Dict[str, Any]]] = layout_data.get("resourcePackages")
        self.layoutOptimization: Optional[int] = layout_data.get("layoutOptimization")

    @classmethod
    def from_unzipped_report(cls, report_path: "Path | str") -> "Report":
        """
        Factory method to create a Report instance from the path to an unzipped .pbix folder.

        Automatically detects old-format (Report/Layout) vs new-format (Report/definition/)
        and delegates accordingly.

        Args:
            report_path: The path to the directory containing the unzipped report files.

        Returns:
            A fully initialized Report instance.
        """
        if isinstance(report_path, str):
            report_path = Path(report_path)

        report_name = report_path.name

        # New pbix format: Report/definition/ directory exists
        definition_dir = report_path / "Report" / "definition"
        if definition_dir.exists():
            return cls.from_definition_dir(definition_dir, report_name)

        # Old pbix format: Report/Layout file
        layout_file = report_path / "Report" / "Layout"
        if not layout_file.exists():
            raise FileNotFoundError(f"Neither 'Report/definition/' nor 'Report/Layout' found for report '{report_name}' at {report_path}")

        try:
            with open(layout_file, 'r', encoding='utf-16-le') as f:
                layout_data = json.load(f)
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            raise ValueError(f"Error reading or parsing Layout file for report '{report_name}': {e}") from e

        return cls(name=report_name, layout_data=layout_data)

    @classmethod
    def from_pbir_folder(cls, pbir_path: "Path | str") -> "Report":
        """
        Factory method to create a Report instance from a .pbir extracted folder.

        In a .pbir folder the definition/ directory sits at the root level (not under Report/).
        The folder name typically ends with '.Report'; that suffix is stripped for the report name.

        Args:
            pbir_path: The path to the root of the extracted .pbir folder.

        Returns:
            A fully initialized Report instance.
        """
        if isinstance(pbir_path, str):
            pbir_path = Path(pbir_path)

        raw_name = pbir_path.name
        report_name = raw_name.removesuffix(".Report") if raw_name.endswith(".Report") else raw_name

        definition_dir = pbir_path / "definition"
        if not definition_dir.exists():
            raise FileNotFoundError(f"'definition/' directory not found in pbir folder '{raw_name}' at {pbir_path}")

        return cls.from_definition_dir(definition_dir, report_name)

    @classmethod
    def from_definition_dir(cls, definition_dir: Path, report_name: str) -> "Report":
        """
        Factory method to create a Report instance from a definition/ directory.

        This is the shared parsing path for both new-format .pbix files and .pbir folders.

        Args:
            definition_dir: Path to the definition/ directory.
            report_name: The human-readable name of the report.

        Returns:
            A fully initialized Report instance.
        """
        report_json_path = definition_dir / "report.json"
        if not report_json_path.exists():
            raise FileNotFoundError(f"report.json not found in definition directory: {definition_dir}")

        try:
            with open(report_json_path, "r", encoding="utf-8") as f:
                report_json = json.load(f)
        except (json.JSONDecodeError, OSError) as e:
            raise ValueError(f"Error reading report.json for '{report_name}': {e}") from e

        extensions_data: Dict[str, Any] = {}
        extensions_path = definition_dir / "reportExtensions.json"
        if extensions_path.exists():
            try:
                with open(extensions_path, "r", encoding="utf-8") as f:
                    extensions_data = json.load(f)
            except (json.JSONDecodeError, OSError) as e:
                print(f"Warning: Could not read reportExtensions.json for '{report_name}': {e}")

        layout_data: Dict[str, Any] = {
            "sections": [],
            "config": json.dumps({
                "bookmarks": report_json.get("bookmarks", []),
                "modelExtensions": [],
            }),
            "filters": json.dumps(report_json.get("filterConfig", {}).get("filters", [])),
            "resourcePackages": report_json.get("resourcePackages"),
        }

        instance = cls(name=report_name, layout_data=layout_data)

        instance.pages = []
        pages_dir = definition_dir / "pages"
        if pages_dir.exists():
            instance._load_pages_from_definition(pages_dir)

        instance.measures = {}
        if extensions_data:
            instance._load_measures_from_extensions(extensions_data)

        instance._resolve_measure_dependencies()
        instance._resolve_usage_dependencies()
        instance._resolve_measure_usage_states()

        return instance

    def _load_pages_from_definition(self, pages_dir: Path):
        """
        Loads pages from the definition/pages/ directory structure.
        """
        page_order: List[str] = []
        pages_json_path = pages_dir / "pages.json"
        if pages_json_path.exists():
            try:
                with open(pages_json_path, "r", encoding="utf-8") as f:
                    page_order = json.load(f).get("pageOrder", [])
            except (json.JSONDecodeError, OSError) as e:
                print(f"Warning: Could not read pages.json for '{self.name}': {e}")

        ordinal_map: Dict[str, int] = {page_id: idx for idx, page_id in enumerate(page_order)}

        for page_dir in pages_dir.iterdir():
            if not page_dir.is_dir():
                continue
            page_json_path = page_dir / "page.json"
            if not page_json_path.exists():
                continue
            try:
                with open(page_json_path, "r", encoding="utf-8") as f:
                    page_json = json.load(f)
                ordinal = ordinal_map.get(page_dir.name, 9999)
                page = Page.from_definition(page_json, page_dir, self, ordinal)
                self.pages.append(page)
            except (json.JSONDecodeError, OSError) as e:
                print(f"Warning: Could not parse page '{page_dir.name}' in '{self.name}': {e}")

        self.pages.sort(key=lambda p: p.ordinal if p.ordinal is not None else 9999)

    def _load_measures_from_extensions(self, extensions_data: Dict[str, Any]):
        """
        Loads measures from parsed reportExtensions.json data.
        """
        all_entities = extensions_data.get("entities", [])

        for entity in all_entities:
            entity_name = entity.get("name", "Unknown")
            all_measures = entity.get("measures", [])

            for measure in all_measures:
                name = measure["name"]
                expression = measure["expression"]
                new_measure = Measure(name, entity_name, expression, self)

                comment = re.search(r"/\*.*? Author:.*?\*/", expression, re.DOTALL)
                if comment is not None:
                    comment = comment.group()
                    author_match = re.search(r"Author: ([a-zA-Z ]*)", comment, re.DOTALL)
                    if author_match is not None:
                        new_measure.author = author_match.group(1)
                    description_match = re.search(r'Description: ([a-zA-Z0-9 .\-"]*)', comment)
                    if description_match is not None:
                        new_measure.description = description_match.group(1)
                    last_change_match = re.search(r"Last change: ([0-9./-]*)", comment)
                    if last_change_match is not None:
                        new_measure.last_change = last_change_match.group(1)

                references = set(result.strip() for result in re.findall(r"[a-zA-Z0-9_ '\"]+\[[a-zA-ZΑ-Ωα-ω0-9_ &]*]{1}", expression))

                if "references" in measure:
                    if "measures" in measure["references"]:
                        references.update(set(f'{ref["entity"]}[{ref["name"]}]' for ref in measure["references"]["measures"]))

                new_measure.referenced_measures = references
                self.measures[new_measure.full_name] = new_measure

    def _load_pages(self, layout_data: Dict[str, Any]):
        """Parses the 'sections' (pages) from the layout data and populates self.pages."""
        page_sections = layout_data.get("sections", [])
        for section_data in page_sections:
            page = Page(section_data, self)
            self.pages.append(page)

    def _load_measures(self):
        """
        Converts all measures found inside the report data into Measure objects.

        The comment block in a measure expression is optional:
        /*
        * Author: John Doe
        * Description: This measure calculates the Sales per Month
        * Last change: 2025/10/23
        */
        """
        if not self.modelExtension:
            return

        all_entities = self.modelExtension[0]["entities"]

        for entity in all_entities:
            entity_name = entity.get("name", "Unknown")
            all_measures = entity.get("measures", [])

            for measure in all_measures:
                name = measure["name"]
                expression = measure["expression"]
                new_measure = Measure(name, entity_name, expression, self)

                comment = re.search(r"/\*.*? Author:.*?\*/", expression, re.DOTALL)
                if comment is not None:
                    comment = comment.group()
                    author_match = re.search(r"Author: ([a-zA-Z ]*)", comment, re.DOTALL)
                    if author_match is not None:
                        new_measure.author = author_match.group(1)
                    description_match = re.search(r'Description: ([a-zA-Z0-9 .\-"]*)', comment)
                    if description_match is not None:
                        new_measure.description = description_match.group(1)
                    last_change_match = re.search(r"Last change: ([0-9./-]*)", comment)
                    if last_change_match is not None:
                        new_measure.last_change = last_change_match.group(1)

                references = set(result.strip() for result in re.findall(r"[a-zA-Z0-9_ '\"]+\[[a-zA-ZΑ-Ωα-ω0-9_ &]*]{1}", expression))

                if "references" in measure:
                    if "measures" in measure["references"]:
                        references.update(set(f'{ref["entity"]}[{ref["name"]}]' for ref in measure["references"]["measures"]))

                new_measure.referenced_measures = references
                self.measures[new_measure.full_name] = new_measure

    def _resolve_measure_dependencies(self):
        """Builds the bi-directional dependency graph for measures."""
        for measure in self.measures.values():
            for referenced_name in measure.referenced_measures:
                referenced_measure = self.measures.get(referenced_name)
                if referenced_measure:
                    referenced_measure.referenced_by_measures.add(measure)

    def _resolve_usage_dependencies(self):
        """Links visuals and pages to the measures they use."""
        for page in self.pages:
            for field_name in page.used_fields:
                measure = self.measures.get(field_name)
                if measure:
                    page.used_measures.add(measure)
                    measure.used_in_pages.add(page)
                    measure.usage_state = UsageState.DIRECTLY_USED

    def _resolve_measure_usage_states(self):
        """Calculates the final usage state for all measures."""
        for measure in self.measures.values():
            if measure.usage_state == UsageState.DIRECTLY_USED:
                self._propagate_indirect_usage(measure)

        for measure in self.measures.values():
            if measure.usage_state == UsageState.UNREFERENCED:
                if measure.referenced_by_measures:
                    measure.usage_state = UsageState.DANGLING

    def _propagate_indirect_usage(self, measure: Measure):
        """
        Recursively sets the usage state of parent measures to INDIRECTLY_USED.

        Args:
            measure: The (in-)directly used parent measure.
        """
        for referenced_name in measure.referenced_measures:
            referenced_measure = self.measures.get(referenced_name)
            if referenced_measure:
                if referenced_measure.usage_state == UsageState.UNREFERENCED:
                    referenced_measure.usage_state = UsageState.INDIRECTLY_USED
                    self._propagate_indirect_usage(referenced_measure)

    def __repr__(self) -> str:
        return f"Report(name='{self.name}', pages={len(self.pages)})"


