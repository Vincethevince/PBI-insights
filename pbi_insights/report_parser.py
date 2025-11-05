import argparse
import asyncio
from pathlib import Path
from typing import List, Dict, Optional
from .report import Report
from .exporter import export_measure_report, export_page_report
from .analyzer import analyze_measures_from_reports, analyze_measures_from_file, analyze_pages_from_reports, analyze_pages_from_file
from datetime import datetime


def parse_reports(unzipped_path: Path) -> List[Report]:
    """Parses all reports from a directory and exports them to Excel."""
    all_reps = []
    print("--- Starting Report Parsing ---")
    for report_folder in unzipped_path.iterdir():
        if report_folder.is_dir():
            try:
                report = Report.from_unzipped_report(report_folder)
                all_reps.append(report)
                print(f"Successfully parsed: {report.name}")
            except (FileNotFoundError, ValueError) as e:
                print(f"Could not parse {report_folder.name}: {e}")

    return all_reps

async def main():
    """Main function to handle command-line arguments and orchestrate tasks."""
    PROJECT_ROOT = Path(__file__).parent.parent
    DATA_PATH = PROJECT_ROOT / "data"
    UNZIPPED_PATH = DATA_PATH / "unzipped_pbi_folders"
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

    args = parser.parse_args()

    if args.command == "run":
        parsed_reports = parse_reports(UNZIPPED_PATH)
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
        measure_filename = f"measures_{timestamp}.xlsx"
        page_filename = f"pages_{timestamp}.xlsx"

        export_measure_report(parsed_reports, OUTPUT_PATH, measure_filename, args.analyze)
        export_page_report(parsed_reports, OUTPUT_PATH, page_filename,args.analyze)
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


if __name__ == '__main__':
    asyncio.run(main())
