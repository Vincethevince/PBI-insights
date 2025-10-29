from __future__ import annotations
import json
from typing import Optional, Dict, Any, Set,List, TYPE_CHECKING
from utils import _recursive_find_fields

if TYPE_CHECKING:
    from .page import Page


class Visual:
    """Represents a single visual on a Power BI report page."""

    def __init__(self, container: Dict[str, Any], page: Page):
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

        # --- Parent Reference ---
        self.page: Page = page

        # --- Core Data ---
        # These are JSON strings that need to be parsed.
        config_str: Optional[str] = container.get("config")
        self.config: Dict[str, Any] = json.loads(config_str) if config_str else {}

        filters_str: Optional[str] = container.get("filters")
        self.filters: List[Dict[str,Any]] = json.loads(filters_str) if filters_str else []

        data_transforms_str: Optional[str] = container.get("dataTransforms")
        self.data_transforms: Dict[str, Any] = json.loads(data_transforms_str) if data_transforms_str else {}

        self.singleVisual: Dict[str, Any] = self.config.get("singleVisual",{})

        # --- Parsed Information (to be populated later) ---
        self.id: str = self.config.get("name", "")
        self.type: str = self.singleVisual.get("visualType", "Unknown")
        #self.used_measures: Set[Measure] = set()
        #self.used_columns: Set['CalculatedColumn'] = set()
        self.used_fields: Set[str] = set()
        self._find_used_fields()

    def _find_used_fields(self):
        """Calls the helper function to recursively find used fields in the visual's data."""
        if self.filters:
            self.used_fields.update(_recursive_find_fields(self.filters))

        if self.data_transforms:
            self.used_fields.update(_recursive_find_fields(self.data_transforms))

        if self.singleVisual:
            self.used_fields.update(_recursive_find_fields(self.singleVisual))

    def __repr__(self) -> str:
        return f"Visual(id='{self.id}', type='{self.type}', page='{self.page.name}')"


