"""Abstract base class shared by all LLM model backends."""
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import List, Dict, Union, TYPE_CHECKING

if TYPE_CHECKING:
    from pbi_insights.domain.measure import Measure
    from pbi_insights.domain.page import Page


class BaseModel(ABC):
    """Common interface for all LLM backends (Vertex AI / OpenAI / …)."""

    # ------------------------------------------------------------------
    # Measures
    # ------------------------------------------------------------------

    @abstractmethod
    async def process_measures_batch(
        self,
        measure_batch: List[Union["Measure", Dict]],
    ) -> Dict[str, str]:
        """
        Processes a single batch of measures and returns a mapping of
        measure name → natural language description.
        """

    @abstractmethod
    async def process_all_measures(
        self,
        measures: List[Union["Measure", Dict]],
        batch_size: int = 20,
    ) -> Dict[str, str]:
        """
        Splits *measures* into batches and processes them concurrently.
        Returns the merged name → description mapping.
        """

    # ------------------------------------------------------------------
    # Pages
    # ------------------------------------------------------------------

    @abstractmethod
    async def process_one_page(
        self,
        page: Union["Page", Dict],
    ) -> Dict[str, str]:
        """
        Processes a single page and returns a mapping of
        page name → natural language description.
        """

    @abstractmethod
    async def process_all_pages(
        self,
        pages: Union[List["Page"], List[Dict]],
    ) -> Dict[str, str]:
        """
        Processes all pages concurrently and returns the merged
        page name → description mapping.
        """

