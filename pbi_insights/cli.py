"""CLI entry-point: argument parsing and command orchestration."""
from __future__ import annotations

import argparse
import asyncio
from datetime import datetime
from pathlib import Path

from pbi_insights.config import UNZIPPED_PATH, PBIR_PATH, OUTPUT_PATH, VECTOR_DB_PATH
from pbi_insights.parsing.parser import parse_reports, find_latest_file, append_report_to_files
from pbi_insights.output.exporter import export_measure_report, export_page_report
from pbi_insights.analysis.analyzer import (
    analyze_measures_from_reports,
    analyze_measures_from_file,
    analyze_pages_from_reports,
    analyze_pages_from_file,
)
from pbi_insights.vector_store import VectorDBFactory, VectorDBBackend, format_results
from pbi_insights.domain.report import Report


async def main():
    """Main function to handle command-line arguments and orchestrate tasks."""
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

    # 'build-db' command: Build the vector DB from a page report file
    parser_build = subparsers.add_parser("build-db", help="Build (or rebuild) the vector DB from an existing page report file.")
    parser_build.add_argument(
        "file_path",
        type=Path,
        help="Path to the enhanced page report Excel/CSV file (must contain a 'Description' column)."
    )
    parser_build.add_argument(
        "--backend",
        type=str,
        choices=["chroma", "faiss"],
        default="chroma",
        help="Vector DB backend to use. Choices: 'chroma' (default), 'faiss'."
    )
    parser_build.add_argument(
        "--include-hidden",
        action="store_true",
        help="Include hidden pages in the vector DB. By default they are excluded."
    )
    parser_build.add_argument(
        "--collection",
        type=str,
        default="pbi_pages",
        help="Collection / index name inside the vector DB (default: 'pbi_pages')."
    )

    # 'query-db' command: Query the vector DB
    parser_query = subparsers.add_parser("query-db", help="Search the vector DB with a natural-language query.")
    parser_query.add_argument(
        "query",
        type=str,
        help="Natural-language search string, e.g. 'show me pages about delayed items'."
    )
    parser_query.add_argument(
        "--backend",
        type=str,
        choices=["chroma", "faiss"],
        default="chroma",
        help="Vector DB backend to query. Must match the backend used when building. (default: 'chroma')"
    )
    parser_query.add_argument(
        "--top-k",
        type=int,
        default=5,
        help="Number of results to return (default: 5)."
    )
    parser_query.add_argument(
        "--collection",
        type=str,
        default="pbi_pages",
        help="Collection / index name to query (default: 'pbi_pages')."
    )

    args = parser.parse_args()

    if args.command == "run":
        parsed_reports = parse_reports(UNZIPPED_PATH, pbir_path=PBIR_PATH)
        if not parsed_reports:
            print("No reports were parsed. Exiting.")
            return

        if args.analyze:
            await analyze_measures_from_reports(parsed_reports)
            await analyze_pages_from_reports(parsed_reports)

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

        if args.analysis_type == "measures":
            updated_df = await analyze_measures_from_file(args.file_path)
        elif args.analysis_type == "pages":
            updated_df = await analyze_pages_from_file(args.file_path)

        new_filename = args.file_path.stem + "_analyzed.xlsx"
        new_filepath = OUTPUT_PATH / new_filename
        updated_df.to_excel(new_filepath, index=False)
        print(f"\nAnalysis complete. Updated report saved to: {new_filepath}")

    elif args.command == "parse-single":
        report_folder = UNZIPPED_PATH / args.report_name

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

        latest_measures = find_latest_file(OUTPUT_PATH, "measures_")
        latest_pages = find_latest_file(OUTPUT_PATH, "pages_")

        is_enhanced = False
        if latest_measures and latest_pages:
            is_enhanced = "enhanced" in latest_measures.name or "enhanced" in latest_pages.name

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

    elif args.command == "build-db":
        if not args.file_path.exists():
            print(f"Error: File not found at {args.file_path}")
            return

        backend = VectorDBBackend(args.backend)
        db = VectorDBFactory.create(backend=backend, db_path=VECTOR_DB_PATH)
        db.create_pagedb_from_file(
            report_file=args.file_path,
            collection_name=args.collection,
            include_hidden=args.include_hidden,
        )
        print("\nVector DB build complete.")

    elif args.command == "query-db":
        backend = VectorDBBackend(args.backend)
        db = VectorDBFactory.create(backend=backend, db_path=VECTOR_DB_PATH)

        try:
            results = db.query_pages(
                query=args.query,
                collection_name=args.collection,
                top_k=args.top_k,
            )
        except FileNotFoundError as e:
            print(f"Error: {e}")
            return

        if not results:
            print("No results found.")
            return

        print(f"\n{'='*60}")
        for entry in format_results(results):
            print(f"\n[{entry['rank']}] {entry['report']}  /  {entry['page']}")
            print(f"    Visible : {entry['is_visible']}")
            print(f"    {entry['description']}")
        print(f"\n{'='*60}")


if __name__ == "__main__":
    asyncio.run(main())


