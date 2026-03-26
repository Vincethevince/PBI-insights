"""OpenAI LLM backend (GPT-4o / GPT-4o-mini / etc.) and embeddings."""
from __future__ import annotations

import asyncio
import json
import os
from typing import Dict, List, Union

from dotenv import load_dotenv
from langchain_openai import ChatOpenAI

from pbi_insights.domain.measure import Measure
from pbi_insights.domain.page import Page
from pbi_insights.models.base import BaseModel


class OpenAIModel(BaseModel):
    """
    A client for interacting with OpenAI chat models (e.g. GPT-4o) to generate
    documentation for DAX measures and Power BI report pages.

    Required environment variable
    ─────────────────────────────
    OPENAI_API_KEY  – your OpenAI API key (set in a .env file or the shell).

    Optional environment variable
    ──────────────────────────────
    OPENAI_ORG_ID   – organization ID (only needed for org-scoped keys).
    """

    def __init__(
        self,
        model_name: str = "gpt-4o-mini",
        temperature: float = 0.1,
        max_output_tokens: int = 8192,
    ):
        """
        Initialises the OpenAIModel client.

        Args:
            model_name:        The OpenAI chat model to use.
                               Recommended values:  "gpt-4o"  (best quality),
                                                    "gpt-4o-mini" (faster / cheaper).
            temperature:       Sampling temperature (0 = deterministic).
            max_output_tokens: Maximum tokens to generate per response.
        """
        load_dotenv()
        api_key = os.getenv("OPENAI_API_KEY")
        if not api_key:
            raise EnvironmentError(
                "OPENAI_API_KEY is not set. "
                "Add it to your .env file or as an environment variable."
            )

        self.llm = ChatOpenAI(
            model=model_name,
            temperature=temperature,
            max_tokens=max_output_tokens,
            api_key=api_key,
            organization=os.getenv("OPENAI_ORG_ID"),  # None is fine if not set
        )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _clean_json_response(raw: str) -> str:
        """Strips markdown code fences that some models add around JSON."""
        cleaned = raw.strip()
        if cleaned.startswith("```json"):
            cleaned = cleaned[len("```json"):]
        elif cleaned.startswith("```"):
            cleaned = cleaned[len("```"):]
        if cleaned.endswith("```"):
            cleaned = cleaned[: -len("```")]
        return cleaned.strip()

    # ------------------------------------------------------------------
    # Measures
    # ------------------------------------------------------------------

    async def process_measures_batch(
        self, measure_batch: List[Union[Measure, Dict]]
    ) -> Dict[str, str]:
        """
        Sends a batch of DAX measures to the model and returns a
        name → description mapping.
        """
        if isinstance(measure_batch[0], Measure):
            measures_json = json.dumps(
                {m.name: m.expression for m in measure_batch}, indent=2
            )
        else:
            measures_json = json.dumps(
                {m["name"]: m["expression"] for m in measure_batch}, indent=2
            )

        prompt = f"""
For each DAX measure below, return a concise natural language description of the measure.
Output **only valid JSON** where:
- Keys = measure names
- Values = concise natural language descriptions

Examples:
"Total Sales": "Sums all values in the Sales column",
"YoY Growth": "Calculates year-over-year percentage change"

**Input Measures (valid JSON):**
```json
{measures_json}
```"""

        response = await self.llm.ainvoke(prompt)
        try:
            return json.loads(self._clean_json_response(response.content))
        except json.JSONDecodeError:
            print(f"[OpenAIModel] Failed to parse batch response: {response.content}")
            return {}

    async def process_all_measures(
        self,
        measures: List[Union[Measure, Dict]],
        batch_size: int = 20,
    ) -> Dict[str, str]:
        """Splits measures into batches and processes them concurrently."""
        batches = [
            measures[i : i + batch_size] for i in range(0, len(measures), batch_size)
        ]
        results = await asyncio.gather(
            *[self.process_measures_batch(batch) for batch in batches]
        )
        merged: Dict[str, str] = {}
        for result in results:
            merged.update(result)
        return merged

    # ------------------------------------------------------------------
    # Pages
    # ------------------------------------------------------------------

    async def process_one_page(self, page: Union[Page, Dict]) -> Dict[str, str]:
        """
        Sends a single page's metadata to the model and returns a
        page-name → description mapping.
        """
        if isinstance(page, Page):
            page_info = {
                "name": page.name,
                "visual_titles": ", ".join(page.visual_titles),
                "used_fields": ", ".join(page.used_fields),
                "measures": {m.name: m.description for m in page.used_measures},
            }
        else:
            page_info = {
                "name": page["name"],
                "visual_titles": page.get("visual_titles", ""),
                "used_fields": page.get("used_fields", ""),
                "measures": page.get("measures", {}),
            }

        page_info_json = json.dumps(page_info, indent=2)

        prompt = f"""
Based on the provided information about a Power BI report page, generate a concise natural
language description of the page. The output must be **only valid JSON** with the page's
name as the key and the description as the value.

Example:
Input:  {{ "name": "Sales Overview", "visual_titles": ["Sales by Region", "YoY Sales Growth"] }}
Output: {{ "Sales Overview": "Provides a high-level overview of sales performance by region and year-over-year growth." }}

**Input Page Info (valid JSON):**
```json
{page_info_json}
```"""

        response = await self.llm.ainvoke(prompt)
        try:
            return json.loads(self._clean_json_response(response.content))
        except json.JSONDecodeError:
            print(f"[OpenAIModel] Failed to parse page response: {response.content}")
            return {}

    async def process_all_pages(
        self, pages: Union[List[Page], List[Dict]]
    ) -> Dict[str, str]:
        """Processes all pages concurrently and returns the merged mapping."""
        results = await asyncio.gather(
            *[self.process_one_page(page) for page in pages]
        )
        merged: Dict[str, str] = {}
        for result in results:
            merged.update(result)
        return merged


# ---------------------------------------------------------------------------
# Quick manual test
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    model = OpenAIModel()
    measures_example = [
        {"name": "Sales[Total Sales]", "expression": "SUM(Sales.Revenue)"},
        {"name": "Sales[Sales per month]", "expression": "DIVIDE(Sales[Total Sales], 12)"},
    ]
    response = asyncio.run(model.process_all_measures(measures=measures_example))
    for key, val in response.items():
        print(f"{key}: {val}")

