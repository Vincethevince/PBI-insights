from __future__ import annotations
from typing import Optional, Set, TYPE_CHECKING
from enum import Enum
if TYPE_CHECKING:
    from .report import Report
    from .page import Page

class UsageState(Enum):
    """Represents the usage status of a measure."""
    # The measure is used directly in a visual.
    DIRECTLY_USED = "Directly Used"

    # The measure is part of a dependency chain that leads to a visual.
    INDIRECTLY_USED = "Indirectly Used"

    # The measure is not referenced by any visual or other item.
    UNREFERENCED = "Unreferenced"

    # The measure is only referenced by other items that are themselves unused.
    DANGLING = "Dangling"

class Measure:
    """Represents a single, hashable DAX measure."""

    def __init__(self, name: str, entity_name: str, expression: str, report: Report):
        """
        Initializes a new Measure object.

        Args:
            name: The name of the measure (e.g., "Total Sales").
            entity_name: The name of the table the measure belongs to (e.g., "Sales").
            expression: The raw DAX expression for the measure.
            report: A back-reference to the parent Report object that contains this measure.
        """
        # --- Core Attributes ---
        self.name: str = name
        self.entity_name: str = entity_name
        self.expression: str = expression
        self.report: Report = report

        # --- Metadata ---
        self.author: Optional[str] = None
        self.description: Optional[str] = None
        self.last_change: Optional[str] = None

        # --- State Management ---
        self.usage_state: UsageState = UsageState.UNREFERENCED

        # --- Dependencies (What this measure USES) ---
        self.referenced_measures: Set[str] = set()
        #self.referenced_columns: Set['CalculatedColumn'] = set()

        # --- Dependents (Where this measure IS USED) ---
        self.referenced_by_measures: Set[Measure] = set()
        self.used_in_pages: Set[Page] = set()

    @property
    def full_name(self) -> str:
        """Returns the fully qualified name, e.g., 'Sales'[Revenue]."""
        return f"{self.entity_name}[{self.name}]"

    def __eq__(self, other) -> bool:
        """Two measures are equal if they have the same name and belong to the same entity."""
        if not isinstance(other, Measure):
            return NotImplemented
        return self.name == other.name and self.entity_name == other.entity_name

    def __hash__(self) -> int:
        """The hash is based on the unique combination of entity name and measure name."""
        return hash(self.full_name)

    def __repr__(self) -> str:
        return f"Measure(full_name={self.full_name})"

