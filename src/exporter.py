from __future__ import annotations
import pandas as pd
from typing import List, TYPE_CHECKING
from pathlib import Path

if TYPE_CHECKING:
    from .report import Report


def export_measure_report(reports: List[Report], output_path: Path, file_name: str, ai_flag: bool= False):
    """
    Generates a detailed Excel report of all measures across all reports.

    Args:
        reports: A list of fully parsed Report objects.
        output_path: The folder path for the output Excel file.
        file_name: The name of the output file - with xlsx or csv extension.
    """
    all_measures_data = []
    for report in reports:
        for measure in report.measures.values():
            measure_data = {
                "Report": report.name,
                "Table": measure.entity_name,
                "Measure Name": measure.name,
                "Usage State": measure.usage_state.value,
                "Expression": measure.expression,
                "Referenced Measures (Raw)": ", ".join(sorted(measure.referenced_measures)),
                "Referenced By": ", ".join(sorted([m.full_name for m in measure.referenced_by_measures])),
                "Used In Pages": ", ".join(sorted([p.name for p in measure.used_in_pages])),
                "Author": measure.author,
                "Description": measure.description,
                "Last Change": measure.last_change
            }
            all_measures_data.append(measure_data)

    if not all_measures_data:
        print("No measures found to export.")
        return

    df = pd.DataFrame(all_measures_data)
    if file_name.endswith(".xlsx"):
        if ai_flag:
            file_name = file_name.replace(".xlsx", "_enhanced.xlsx")
        df.to_excel(output_path/file_name, index=False, sheet_name="Measures")

    elif file_name.endswith(".csv"):
        if ai_flag:
            file_name = file_name.replace(".csv", "_enhanced.csv")
        df.to_csv(output_path/file_name)
    else:
        print(f"Unsupported file extension: {file_name}")
        return
    print(f"Successfully exported measure report to {output_path}")


def export_page_report(reports: List[Report], output_path: Path, file_name: str):
    """
    Generates a detailed Excel report of all pages across all reports.

    Args:
        reports: A list of fully parsed Report objects.
        output_path: The file path for the output Excel file.
        file_name: The name of the output Excel file.
    """
    all_pages_data = []
    for report in reports:
        for page in report.pages:
            page_data = {
                "Report": report.name,
                "Page Name": page.name,
                "Is Visible": page.is_visible,
                "Number of Visuals": len(page.visuals),
                "Used Measures": ", ".join(sorted([m.full_name for m in page.used_measures])),
                "All Used Fields (Raw)": ", ".join(sorted(page.used_fields))
            }
            all_pages_data.append(page_data)

    if not all_pages_data:
        print("No pages found to export.")
        return

    df = pd.DataFrame(all_pages_data)
    if file_name.endswith(".xlsx"):
        df.to_excel(output_path / file_name, index=False, sheet_name="Pages")
    elif file_name.endswith(".csv"):
        df.to_csv(output_path / file_name)
    else:
        print(f"Unsupported file extension: {file_name}")
        return
    print(f"Successfully exported page report to {output_path}")