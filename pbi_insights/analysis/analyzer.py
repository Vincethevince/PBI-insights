from __future__ import annotations

import asyncio
from pathlib import Path
from typing import List, TYPE_CHECKING, Dict, Literal

import pandas as pd

from pbi_insights.models.vertex import VertexModel
from pbi_insights.models.openai_model import OpenAIModel
from pbi_insights.models.base import BaseModel

if TYPE_CHECKING:
    from pbi_insights.domain.report import Report

# ---------------------------------------------------------------------------
# Internal helper
# ---------------------------------------------------------------------------

ModelProvider = Literal["gemini", "openai"]


def _get_model(provider: ModelProvider) -> BaseModel:
    """
    Returns the appropriate LLM backend.

    Args:
        provider: ``"gemini"`` (default) uses Google Vertex AI;
                  ``"openai"`` uses the OpenAI Chat API.
    """
    if provider == "openai":
        return OpenAIModel()
    return VertexModel()


# ---------------------------------------------------------------------------
# Public analysis functions
# ---------------------------------------------------------------------------

async def analyze_measures_from_reports(
    reports: "List[Report]",
    batch_size: int = 20,
    provider: ModelProvider = "gemini",
):
    """
    Analyzes measures directly from a list of in-memory Report objects.

    This is the "live" analysis mode. It extracts measures that do not have
    a manually written description and sends them to the AI for documentation.

    Args:
        reports:    A list of parsed Report objects.
        batch_size: The number of measures to process in each concurrent batch.
        provider:   LLM backend to use – ``"gemini"`` (default) or ``"openai"``.
    """
    print(f"\n--- Starting Live AI Analysis [provider={provider}] ---")
    model = _get_model(provider)

    for report in reports:
        measures_for_this_report = [
            {"name": m.full_name, "expression": m.expression}
            for m in report.measures.values() if not m.description
        ]

        if not measures_for_this_report:
            print(f"Report '{report.name}': No measures needing analysis.")
            continue

        print(f"Report '{report.name}': Found {len(measures_for_this_report)} measures to analyze.")
        descriptions = await model.process_all_measures(measures_for_this_report, batch_size)
        print(f"Report '{report.name}': Successfully generated descriptions for {len(descriptions)} measures.")

        for measure_name, description in descriptions.items():
            if measure_name in report.measures:
                report.measures[measure_name].description = description


async def analyze_pages_from_reports(
    reports: "List[Report]",
    provider: ModelProvider = "gemini",
):
    """
    Analyzes pages directly from a list of in-memory Report objects.

    Args:
        reports:  A list of parsed Report objects.
        provider: LLM backend to use – ``"gemini"`` (default) or ``"openai"``.
    """
    print(f"\n--- Starting Live AI Analysis for Pages [provider={provider}] ---")
    model = _get_model(provider)

    for report in reports:
        descriptions = await model.process_all_pages(report.pages)
        print(f"Report '{report.name}': Successfully generated descriptions for {len(descriptions)} pages.")

        for page in report.pages:
            if page.name in descriptions:
                page.description = descriptions[page.name]


async def analyze_measures_from_file(
    file_path: Path,
    provider: ModelProvider = "gemini",
) -> pd.DataFrame:
    """
    Analyzes measures from a previously exported Excel file.

    Args:
        file_path: The path to the measure report Excel file.
        provider:  LLM backend to use – ``"gemini"`` (default) or ``"openai"``.

    Returns:
        A pandas DataFrame with the 'Description' column updated with AI-generated content.
    """
    print(f"\n--- Starting Retrospective AI Analysis on {file_path.name} [provider={provider}] ---")
    if file_path.suffix == ".xlsx":
        df = pd.read_excel(file_path)
    elif file_path.suffix == ".csv":
        df = pd.read_csv(str(file_path))
    else:
        raise ValueError(f"Unsupported file type: {file_path.suffix}")

    df["Description"] = df["Description"].fillna("")
    model = _get_model(provider)

    for report_name, report_df in df.groupby("Report"):
        measures_to_process = report_df[report_df["Description"] == ""]
        if measures_to_process.empty:
            print(f"Report '{report_name}': No measures needing analysis.")
            continue

        measures_for_ai = [
            {"name": f"{row['Table']}[{row['Measure Name']}]", "expression": row["Expression"]}
            for _, row in measures_to_process.iterrows()
        ]

        print(f"Report '{report_name}': Found {len(measures_for_ai)} measures to analyze.")
        descriptions = await model.process_all_measures(measures_for_ai)
        print(f"Report '{report_name}': Successfully generated descriptions for {len(descriptions)} measures.")

        for index, row in measures_to_process.iterrows():
            measure_full_name = f"{row['Table']}[{row['Measure Name']}]"
            if measure_full_name in descriptions:
                df.loc[index, "Description"] = descriptions[measure_full_name]

    return df


async def analyze_pages_from_file(
    file_path: Path,
    provider: ModelProvider = "gemini",
) -> pd.DataFrame:
    """
    Analyzes pages from a previously exported Excel file.

    Args:
        file_path: The path to the page report Excel file.
        provider:  LLM backend to use – ``"gemini"`` (default) or ``"openai"``.

    Returns:
        A pandas DataFrame with the 'Description' column updated with AI-generated content.
    """
    print(f"\n--- Starting Retrospective AI Analysis on Page Report: {file_path.name} [provider={provider}] ---")
    if file_path.suffix == ".xlsx":
        df = pd.read_excel(file_path)
    elif file_path.suffix == ".csv":
        df = pd.read_csv(str(file_path))
    else:
        raise ValueError(f"Unsupported file type: {file_path.suffix}")

    df["Description"] = ""
    model = _get_model(provider)

    for report_name, report_df in df.groupby("Report"):
        pages_for_ai = [
            {
                "name": row["Page Name"],
                "visual_titles": row["All Visual Titles"],
                "used_fields": row["All Used Fields (Raw)"],
                "measures": row["Used Measures"],
            }
            for _, row in report_df.iterrows()
        ]

        print(f"Report '{report_name}': Found {len(pages_for_ai)} pages to analyze.")
        descriptions = await model.process_all_pages(pages_for_ai)
        print(f"Report '{report_name}': Successfully generated descriptions for {len(descriptions)} pages.")

        for index, row in report_df.iterrows():
            page_full_name = f"{report_name}[{row['Page Name']}]"
            if page_full_name in descriptions:
                df.loc[index, "Description"] = descriptions[page_full_name]

    return df

