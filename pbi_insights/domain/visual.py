from __future__ import annotations
import json
from pathlib import Path
from typing import Optional, Dict, Any, Set, List, TYPE_CHECKING

from pbi_insights.parsing.utils import _recursive_find_fields

if TYPE_CHECKING:
    from .page import Page


class Visual:
    """Represents a single visual on a Power BI report page."""

    def __init__(self, container: Dict[str, Any], page: "Page"):
        """
        Initializes a Visual object from its JSON container.

        Args:
            container: The dictionary representing the 'visualContainer' from the report layout.
            page: A reference to the parent Page object.
        """
        # --- Positional and Sizing Attributes ---
        self.x: Optional[int] = container.get("x")
        self.y: Optional[int] = container.get("y")
        self.z: Optional[int] = container.get("z")
        self.width: Optional[float] = container.get("width")
        self.height: Optional[float] = container.get("height")
        self.title: Optional[str] = None

        # --- Parent Reference ---
        self.page: Page = page

        # --- Core Data ---
        # These are JSON strings that need to be parsed.
        config_str: Optional[str] = container.get("config")
        self.config: Dict[str, Any] = json.loads(config_str) if config_str else {}

        filters_str: Optional[str] = container.get("filters")
        self.filters: List[Dict[str, Any]] = json.loads(filters_str) if filters_str else []

        data_transforms_str: Optional[str] = container.get("dataTransforms")
        self.data_transforms: Dict[str, Any] = json.loads(data_transforms_str) if data_transforms_str else {}

        self.singleVisual: Dict[str, Any] = self.config.get("singleVisual", {})

        # --- Parsed Information (to be populated later) ---
        self.id: str = self.config.get("name", "")
        self.type: str = self.singleVisual.get("visualType", "Unknown")
        self.used_fields: Set[str] = set()
        self._find_used_fields()
        self._find_title()

    def _find_used_fields(self):
        """Calls the helper function to recursively find used fields in the visual's data."""
        if self.filters:
            self.used_fields.update(_recursive_find_fields(self.filters))

        if self.data_transforms:
            self.used_fields.update(_recursive_find_fields(self.data_transforms))

        if self.singleVisual:
            self.used_fields.update(_recursive_find_fields(self.singleVisual))

    def _find_title(self):
        """Searches for the visual title."""
        try:
            vcObjects = self.singleVisual.get("vcObjects", {})
            title = vcObjects.get("title", [])
            self.title = title[0].get("properties", {}).get("text", {}).get("expr", {}).get("Literal", {}).get("Value", "")
        except Exception:
            return

    @classmethod
    def from_definition(cls, visual_json: Dict[str, Any], page: "Page") -> "Visual":
        """
        Factory method to create a Visual instance from a parsed visual.json (definition format).

        In the definition format the visual data is already a fully-parsed JSON object,
        unlike the old format where position/config were JSON strings embedded inside a
        visualContainer dict.

        Args:
            visual_json: The parsed contents of a visual.json file.
            page: A reference to the parent Page object.

        Returns:
            A fully initialised Visual instance.
        """
        instance = cls.__new__(cls)
        instance.page = page

        # --- Position (top-level in new format) ---
        position: Dict[str, Any] = visual_json.get("position", {})
        instance.x = position.get("x")
        instance.y = position.get("y")
        instance.z = position.get("z")
        instance.width = position.get("width")
        instance.height = position.get("height")

        # --- Visual core (nested under "visual" key) ---
        visual_node: Dict[str, Any] = visual_json.get("visual", {})
        instance.type = visual_node.get("visualType", "Unknown")

        # In the new format config / filters / dataTransforms are already dicts, not strings
        instance.config = {}           # not a separate block in definition format
        instance.filters = visual_json.get("filterConfig", {}).get("filters", [])
        instance.data_transforms = {}  # no dataTransforms block in definition format
        instance.singleVisual = {}     # no singleVisual wrapper; used_fields sourced differently

        instance.id = visual_json.get("name", "")
        instance.is_hidden = visual_json.get("isHidden", False)

        # --- Used fields: sourced from visual.query.queryState ---
        instance.used_fields: Set[str] = set()
        query_state = visual_node.get("query", {}).get("queryState", {})
        if query_state:
            projections_by_role = {
                role: role_data.get("projections", [])
                for role, role_data in query_state.items()
                if isinstance(role_data, dict)
            }
            instance.used_fields.update(_recursive_find_fields({"projections": projections_by_role}))

        if instance.filters:
            instance.used_fields.update(_recursive_find_fields(instance.filters))

        # Scan the visual's formatting objects for extra field references
        objects_node = visual_node.get("objects", {})
        if objects_node:
            instance.used_fields.update(_recursive_find_fields(objects_node))

        # --- Title (under visualContainerObjects in new format) ---
        instance.title = None
        instance._find_title_from_definition(visual_json)

        return instance

    def _find_title_from_definition(self, visual_json: Dict[str, Any]):
        """Searches for the visual title in the definition format (visualContainerObjects)."""
        try:
            title_list = visual_json.get("visualContainerObjects", {}).get("title", [])
            self.title = title_list[0].get("properties", {}).get("text", {}).get("expr", {}).get("Literal", {}).get("Value", "")
        except (IndexError, AttributeError, TypeError):
            return

    def __repr__(self) -> str:
        return f"Visual(id='{self.id}', type='{self.type}', page='{self.page.name}')"

