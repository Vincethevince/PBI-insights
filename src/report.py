from __future__ import annotations
import json
from typing import List, Dict, Any, Optional, Set, TYPE_CHECKING
from pathlib import Path
import re
from page import Page
from measure import Measure, UsageState

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

        self.bookmarks: List[Dict] = self.config.get("bookmarks",[]) # Not used in any way for now
        self.modelExtension: List[Dict] = self.config.get("modelExtensions",[])

        global_filters_str: Optional[str] = layout_data.get("filters")
        self.global_filters: List[Dict[str, Any]] = json.loads(global_filters_str) if global_filters_str else []

        # --- Child Objects ---
        self.pages: List[Page] = []
        self._load_pages(layout_data)

        # --- Contained Objects ---
        self.measures: Dict[str,Measure] = {} # fullname: Measure object
        self._load_measures()
        self._resolve_measure_dependencies()
        self._resolve_usage_dependencies()
        self._resolve_measure_usage_states()

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

    def _load_measures(self):
        """
        This converts all measures found inside the report data into Measure objects and binds them to the report.
        The comment part is optional or to be seen as a recommendation to make life easier for yourself.
        Comment to be put in the measure looks like:
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
            entity_name = entity.get("name","Unknown")
            all_measures = entity.get("measures",[])

            for measure in all_measures:
                name = measure["name"]
                expression = measure["expression"]
                new_measure = Measure(name, entity_name, expression, self)

                # If you don't put comments like described above in your measures, you can ignore this.
                comment = re.search("/\*.*? Author:.*?\*/", expression, re.DOTALL)
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

                # This searches for all other measures like Sales[Amount], entity[name]
                references = set(result.strip() for result in re.findall("[a-zA-Z0-9_ '\"]+\[[a-zA-ZΑ-Ωα-ω0-9_ &]*]{1}", expression))

                # Sometimes, a measure expression looks like "DIVIDE([Measure 1], [Measure 2])"
                # In this case, the measure.references keep the real names
                if "references" in measure:
                    if "measures" in measure["references"]:
                        references.update(set(f'{ref["entity"]}[{ref["name"]}]' for ref in measure["references"]["measures"]))

                new_measure.referenced_measures = references
                self.measures[new_measure.full_name] = new_measure

    def _resolve_measure_dependencies(self):
        """
        Iterates through all measures to build the bi-directional dependency graph.

        This method populates the `referenced_by_measures` set for each measure.
        """
        for measure in self.measures.values():
            for referenced_name in measure.referenced_measures:
                # Find the measure object that is being referenced
                referenced_measure = self.measures.get(referenced_name)
                if referenced_measure:
                    # Add this measure to the dependents of the referenced measure
                    referenced_measure.referenced_by_measures.add(measure)

    def _resolve_usage_dependencies(self):
        """
        Links visuals and pages to the measures they use.

        This method iterates through all pages and their visuals, using the
        `used_fields` set to look up measures in the central `self.measures`
        dictionary and build the bi-directional relationships.
        """
        for page in self.pages:
            # Link page-level filters to measures
            for field_name in page.used_fields:
                measure = self.measures.get(field_name)
                if measure:
                    page.used_measures.add(measure)
                    measure.used_in_pages.add(page)
                    measure.usage_state = UsageState.DIRECTLY_USED

    def _resolve_measure_usage_states(self):
        """
        Calculates the final usage state for all measures by traversing the dependency graph.
        """
        # Propagate the 'INDIRECTLY_USED' state up the dependency chain
        for measure in self.measures.values():
            if measure.usage_state == UsageState.DIRECTLY_USED:
                self._propagate_indirect_usage(measure)

        # Identify 'DANGLING' measures
        for measure in self.measures.values():
            if measure.usage_state == UsageState.UNREFERENCED:
                if measure.referenced_by_measures:
                    measure.usage_state = UsageState.DANGLING

    def _propagate_indirect_usage(self, measure: Measure):
        """
        Recursively sets the usage state of parent measures to INDIRECTLY_USED.
        Example: If Measure A is used, Measure B is not used and A's formula is `SUM(Measure B)`,
        this function will mark Measure B as INDIRECTLY_USED.

        Args:
            Measure: The (in-)directly used parent measure.
        """
        for referenced_name in measure.referenced_measures:
            referenced_measure = self.measures.get(referenced_name)
            if referenced_measure:
                if referenced_measure.usage_state == UsageState.UNREFERENCED:
                    referenced_measure.usage_state = UsageState.INDIRECTLY_USED
                    self._propagate_indirect_usage(referenced_measure)

    def __repr__(self) -> str:
        """
        Provides a developer-friendly string representation of the Report object.
        """
        return f"Report(name='{self.name}', pages={len(self.pages)})"

