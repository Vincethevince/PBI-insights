"""Report parsing: discovers and loads Report objects from the file system."""
from __future__ import annotations

from pathlib import Path
from typing import List, Dict, Optional

import pandas as pd

from pbi_insights.domain.report import Report


def parse_reports(unzipped_path: Path, pbir_path: Optional[Path] = None) -> List[Report]:
    """Parses all reports from the unzipped pbix directory and (optionally) a pbir directory."""
    all_reps = []
    print("--- Starting Report Parsing ---")

    # --- Parse unzipped pbix folders (old format + new format auto-detected) ---
    for report_folder in unzipped_path.iterdir():
        if report_folder.is_dir():
            try:
                report = Report.from_unzipped_report(report_folder)
                all_reps.append(report)
                print(f"Successfully parsed (pbix): {report.name}")
            except (FileNotFoundError, ValueError) as e:
                print(f"Could not parse {report_folder.name}: {e}")

    # --- Parse pbir folders ---
    if pbir_path and pbir_path.is_dir():
        for pbir_folder in pbir_path.iterdir():
            if pbir_folder.is_dir():
                try:
                    report = Report.from_pbir_folder(pbir_folder)
                    all_reps.append(report)
                    print(f"Successfully parsed (pbir): {report.name}")
                except (FileNotFoundError, ValueError) as e:
                    print(f"Could not parse {pbir_folder.name}: {e}")

    return all_reps


def find_latest_file(output_path: Path, prefix: str) -> Optional[Path]:
    """
    Finds the most recently modified file in output_path that starts with the given prefix.

    Args:
        output_path: The directory to search in.
        prefix: The prefix to match (e.g., 'measures_' or 'pages_').

    Returns:
        Path to the latest matching file, or None if no match found.
    """
    matching_files = [
        f for f in output_path.iterdir()
        if f.is_file() and f.name.startswith(prefix)
    ]

    if not matching_files:
        return None

    matching_files.sort(key=lambda f: f.stat().st_mtime, reverse=True)
    return matching_files[0]


def get_measure_data_from_report(report: Report) -> List[Dict]:
    """
    Extracts measure data from a single report as a list of dictionaries.

    Args:
        report: A fully parsed Report object.

    Returns:
        List of dictionaries containing measure data.
    """
    measures_data = []
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
            "Last Change": measure.last_change,
        }
        measures_data.append(measure_data)
    return measures_data


def get_page_data_from_report(report: Report, include_description: bool = False) -> List[Dict]:
    """
    Extracts page data from a single report as a list of dictionaries.

    Args:
        report: A fully parsed Report object.
        include_description: If True, includes the AI-generated Description field.

    Returns:
        List of dictionaries containing page data.
    """
    pages_data = []
    for page in report.pages:
        page_data = {
            "Report": report.name,
            "Page Name": page.name,
            "Section": page.section,
            "Is Visible": page.is_visible,
            "Number of Visuals": len(page.visuals),
            "Used Measures": ", ".join(sorted([m.full_name for m in page.used_measures])),
            "All Used Fields (Raw)": ", ".join(sorted(page.used_fields)),
            "All Visual Titles": ", ".join(sorted(page.visual_titles)),
        }
        if include_description:
            page_data["Description"] = getattr(page, "description", None)
        pages_data.append(page_data)
    return pages_data


def append_report_to_files(report: Report, output_path: Path, enhanced: bool = False) -> bool:
    """
    Appends a single report's data to the latest measures and pages files.

    Args:
        report: A fully parsed Report object.
        output_path: The directory containing the output files.
        enhanced: If True, includes AI-generated Description fields for pages.

    Returns:
        True if successful, False otherwise.
    """
    latest_measures = find_latest_file(output_path, "measures_")
    if latest_measures is None:
        print("Error: No existing measures file found in output folder.")
        return False

    latest_pages = find_latest_file(output_path, "pages_")
    if latest_pages is None:
        print("Error: No existing pages file found in output folder.")
        return False

    print(f"Found latest measures file: {latest_measures.name}")
    print(f"Found latest pages file: {latest_pages.name}")

    try:
        if latest_measures.suffix == ".xlsx":
            existing_measures_df = pd.read_excel(latest_measures)
        else:
            existing_measures_df = pd.read_csv(latest_measures)

        if latest_pages.suffix == ".xlsx":
            existing_pages_df = pd.read_excel(latest_pages)
        else:
            existing_pages_df = pd.read_csv(latest_pages)
    except Exception as e:
        print(f"Error reading existing files: {e}")
        return False

    if report.name in existing_measures_df["Report"].values:
        print(f"Warning: Report '{report.name}' already exists in measures file. Removing old entries...")
        existing_measures_df = existing_measures_df[existing_measures_df["Report"] != report.name]

    if report.name in existing_pages_df["Report"].values:
        print(f"Warning: Report '{report.name}' already exists in pages file. Removing old entries...")
        existing_pages_df = existing_pages_df[existing_pages_df["Report"] != report.name]

    new_measures_data = get_measure_data_from_report(report)
    new_pages_data = get_page_data_from_report(report, include_description=enhanced)

    new_measures_df = pd.DataFrame(new_measures_data)
    new_pages_df = pd.DataFrame(new_pages_data)

    updated_measures_df = pd.concat([existing_measures_df, new_measures_df], ignore_index=True)
    updated_pages_df = pd.concat([existing_pages_df, new_pages_df], ignore_index=True)

    try:
        if latest_measures.suffix == ".xlsx":
            updated_measures_df.to_excel(latest_measures, index=False, sheet_name="Measures")
        else:
            updated_measures_df.to_csv(latest_measures, index=False)

        if latest_pages.suffix == ".xlsx":
            updated_pages_df.to_excel(latest_pages, index=False, sheet_name="Pages")
        else:
            updated_pages_df.to_csv(latest_pages, index=False)

        print(f"\nSuccessfully appended {len(new_measures_data)} measures and {len(new_pages_data)} pages.")
        print(f"Updated: {latest_measures.name}")
        print(f"Updated: {latest_pages.name}")
        return True
    except Exception as e:
        print(f"Error saving files: {e}")
        return False

