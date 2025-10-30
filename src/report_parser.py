from pathlib import Path
from report import Report
from exporter import export_measure_report, export_page_report
from datetime import datetime

if __name__ == '__main__':
    PROJECT_ROOT = Path(__file__).parent.parent
    DATA_PATH = PROJECT_ROOT / "data"
    UNZIPPED_PATH = DATA_PATH / "unzipped_pbi_folders"
    OUTPUT_PATH = PROJECT_ROOT / "output"
    OUTPUT_PATH.mkdir(exist_ok=True)
    all_reps = []

    for report_folder in UNZIPPED_PATH.iterdir():
        if report_folder.is_dir():
            report = Report.from_unzipped_report(report_folder)
            all_reps.append(report)
            print(f"Successfully parsed: {report.name}")

    if all_reps:
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

        export_measure_report(all_reps, OUTPUT_PATH ,f"measures_{timestamp}.xlsx")
        export_page_report(all_reps, OUTPUT_PATH , f"pages_{timestamp}.xlsx")
