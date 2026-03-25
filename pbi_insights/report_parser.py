import argparse
import asyncio
from pathlib import Path
from typing import List, Dict, Optional
import pandas as pd
from .report import Report
from .exporter import export_measure_report, export_page_report
from .analyzer import analyze_measures_from_reports, analyze_measures_from_file, analyze_pages_from_reports, analyze_pages_from_file
from datetime import datetime


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
    
    # Sort by modification time, most recent first
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
            "Last Change": measure.last_change
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
            "All Visual Titles": ", ".join(sorted(page.visual_titles))
        }
        if include_description:
            page_data["Description"] = getattr(page, 'description', None)
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
    # Find latest measures file
    latest_measures = find_latest_file(output_path, "measures_")
    if latest_measures is None:
        print("Error: No existing measures file found in output folder.")
        return False
    
    # Find latest pages file
    latest_pages = find_latest_file(output_path, "pages_")
    if latest_pages is None:
        print("Error: No existing pages file found in output folder.")
        return False
    
    print(f"Found latest measures file: {latest_measures.name}")
    print(f"Found latest pages file: {latest_pages.name}")
    
    # Load existing data
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
    
    # Check if report already exists in the files
    if report.name in existing_measures_df["Report"].values:
        print(f"Warning: Report '{report.name}' already exists in measures file. Removing old entries...")
        existing_measures_df = existing_measures_df[existing_measures_df["Report"] != report.name]
    
    if report.name in existing_pages_df["Report"].values:
        print(f"Warning: Report '{report.name}' already exists in pages file. Removing old entries...")
        existing_pages_df = existing_pages_df[existing_pages_df["Report"] != report.name]
    
    # Get new data from parsed report
    new_measures_data = get_measure_data_from_report(report)
    new_pages_data = get_page_data_from_report(report, include_description=enhanced)
    
    # Create DataFrames and append
    new_measures_df = pd.DataFrame(new_measures_data)
    new_pages_df = pd.DataFrame(new_pages_data)
    
    updated_measures_df = pd.concat([existing_measures_df, new_measures_df], ignore_index=True)
    updated_pages_df = pd.concat([existing_pages_df, new_pages_df], ignore_index=True)
    
    # Save back to the same files
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

async def main():
    """Main function to handle command-line arguments and orchestrate tasks."""
    PROJECT_ROOT = Path(__file__).parent.parent
    DATA_PATH = PROJECT_ROOT / "data"
    UNZIPPED_PATH = DATA_PATH / "unzipped_pbi_folders"
    PBIR_PATH = DATA_PATH / "pbir_folders"
    OUTPUT_PATH = PROJECT_ROOT / "output"
    OUTPUT_PATH.mkdir(exist_ok=True)

    parser = argparse.ArgumentParser(description="PBI Insights: Parse, Export, and Analyze Power BI reports.")
    subparsers = parser.add_subparsers(dest="command", required=True, help="Available commands")

    # 'run' command: Parse, Export, and optionally Analyze
    parser_run = subparsers.add_parser("run", help="Parse reports, export to Excel, and optionally run live AI analysis.")
    parser_run.add_argument(
        "--analyze",
        action="store_true",
        help="Run live AI analysis on measures after parsing and exporting."
    )
    parser_run.add_argument(
        "--file_type",
        type=str,
        choices=["csv", "xlsx"],
        help="Type of file to export. Options are 'csv' and 'xlsx'."
    )

    # 'analyze-file' command: Retrospective analysis
    parser_analyze = subparsers.add_parser("analyze-file", help="Run retrospective AI analysis on an existing measure report Excel file.")
    parser_analyze.add_argument(
        "file_path",
        type=Path,
        help="Path to the measure report Excel file to analyze."
    )
    parser_analyze.add_argument(
        "analysis_type",
        type=str,
        choices=["measures", "pages"],
        help="Type of file to analyze. Options are 'measures' and 'pages'."
    )

    # 'parse-single' command: Parse a single report and append to latest output files
    parser_single = subparsers.add_parser("parse-single", help="Parse a single report and append it to the latest output files.")
    parser_single.add_argument(
        "report_name",
        type=str,
        help="Name of the report folder to parse (must exist in the unzipped_pbi_folders directory)."
    )

    args = parser.parse_args()

    if args.command == "run":
        parsed_reports = parse_reports(UNZIPPED_PATH, pbir_path=PBIR_PATH)
        if not parsed_reports:
            print("No reports were parsed. Exiting.")
            return

        # If --analyze flag is used, run live analysis
        if args.analyze:
            await analyze_measures_from_reports(parsed_reports)
            await analyze_pages_from_reports(parsed_reports)

        # Export the reports
        print("\n--- Starting Report Export ---")
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        if args.file_type == "csv":
            measure_filename = f"measures_{timestamp}.csv"
            page_filename = f"pages_{timestamp}.csv"
        else:
            measure_filename = f"measures_{timestamp}.xlsx"
            page_filename = f"pages_{timestamp}.xlsx"

        export_measure_report(parsed_reports, OUTPUT_PATH, measure_filename, include_description=args.analyze)
        export_page_report(parsed_reports, OUTPUT_PATH, page_filename, include_description=args.analyze)
        print("\nExport complete.")

    elif args.command == "analyze-file":
        if not args.file_path.exists():
            print(f"Error: File not found at {args.file_path}")
            return

        # Run retrospective analysis
        if args.analysis_type == "measures":
            updated_df = await analyze_measures_from_file(args.file_path)
        elif args.analysis_type =="pages":
            updated_df = await analyze_pages_from_file(args.file_path)


        # Save the updated DataFrame to a new file
        new_filename = args.file_path.stem + "_analyzed.xlsx"
        new_filepath = OUTPUT_PATH / new_filename
        updated_df.to_excel(new_filepath, index=False)
        print(f"\nAnalysis complete. Updated report saved to: {new_filepath}")

    elif args.command == "parse-single":
        report_folder = UNZIPPED_PATH / args.report_name

        # Also check pbir_folders (with and without the '.Report' suffix)
        pbir_folder = PBIR_PATH / args.report_name
        pbir_folder_with_suffix = PBIR_PATH / f"{args.report_name}.Report"

        is_pbir = False
        if not report_folder.exists():
            if pbir_folder.exists():
                report_folder = pbir_folder
                is_pbir = True
            elif pbir_folder_with_suffix.exists():
                report_folder = pbir_folder_with_suffix
                is_pbir = True
            else:
                print(f"Error: Report folder not found at {report_folder}")
                print("\nAvailable reports (pbix):")
                for folder in UNZIPPED_PATH.iterdir():
                    if folder.is_dir():
                        print(f"  - {folder.name}")
                if PBIR_PATH.is_dir():
                    print("\nAvailable reports (pbir):")
                    for folder in PBIR_PATH.iterdir():
                        if folder.is_dir():
                            print(f"  - {folder.name}")
                return

        print(f"--- Parsing Single Report: {args.report_name} ---")
        try:
            if is_pbir:
                report = Report.from_pbir_folder(report_folder)
            else:
                report = Report.from_unzipped_report(report_folder)
            print(f"Successfully parsed: {report.name}")
            print(f"  - Measures found: {len(report.measures)}")
            print(f"  - Pages found: {len(report.pages)}")
        except (FileNotFoundError, ValueError) as e:
            print(f"Error parsing report: {e}")
            return
        
        # Check if latest files are "enhanced" versions
        latest_measures = find_latest_file(OUTPUT_PATH, "measures_")
        latest_pages = find_latest_file(OUTPUT_PATH, "pages_")
        
        is_enhanced = False
        if latest_measures and latest_pages:
            is_enhanced = "enhanced" in latest_measures.name or "enhanced" in latest_pages.name
        
        # If files are enhanced, run AI analysis on the single report
        if is_enhanced:
            print("\n--- Detected enhanced files, running AI analysis ---")
            await analyze_measures_from_reports([report])
            await analyze_pages_from_reports([report])
        
        print("\n--- Appending to Latest Output Files ---")
        success = append_report_to_files(report, OUTPUT_PATH, enhanced=is_enhanced)
        if not success:
            print("\nFailed to append report data to output files.")
            return
        
        print("\nParse-single complete.")


if __name__ == '__main__':
    asyncio.run(main())
