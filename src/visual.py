import json
from typing import Optional, Dict, Any, Set, TYPE_CHECKING
if TYPE_CHECKING:
    from .measure import Measure


class Visual:
    """Represents a single visual on a Power BI report page."""

    def __init__(self, container: Dict[str, Any], page: 'Page'):
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
        self.page: 'Page' = page

        # --- Core Data ---
        # These are JSON strings that need to be parsed.
        config_str: Optional[str] = container.get("config")
        self.config: Dict[str, Any] = json.loads(config_str) if config_str else {}

        filters_str: Optional[str] = container.get("filters")
        self.filters: Dict[str, Any] = json.loads(filters_str) if filters_str else {}

        data_transforms_str: Optional[str] = container.get("dataTransforms")
        self.data_transforms: Dict[str, Any] = json.loads(data_transforms_str) if data_transforms_str else {}

        # --- Parsed Information (to be populated later) ---
        self.id: str = self.config.get("name", "")
        self.type: str = self.config.get("singleVisual", {}).get("visualType", "Unknown")
        self.used_measures: Set['Measure'] = set()
        self.used_columns: Set['CalculatedColumn'] = set()

    def __repr__(self) -> str:
        return f"Visual(id='{self.id}', type='{self.type}', page='{self.page.name}')"