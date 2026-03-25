import zipfile
import logging
import shutil
from pathlib import Path
from tqdm import tqdm
from typing import Optional, List

# Set up basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


class Unzipper:
    """Handles the unzipping of .pbix files from a source to a destination directory."""

    def __init__(self, source_dir: Path, destination_dir: Path, single_file: str = ''):
        """
        Initializes the Unzipper.

        Args:
            source_dir: The directory containing .pbix files.
            destination_dir: The directory where unzipped folders will be created.
            single_file: If specified, only this file will be processed.

        Raises:
            NotADirectoryError: If source or destination directories do not exist.
            FileNotFoundError: If a specified single_file does not exist.
        """
        if not source_dir.is_dir():
            raise NotADirectoryError(f"Provided source directory does not exist: {source_dir}")
        self.source_dir = source_dir

        if not destination_dir.is_dir():
            raise NotADirectoryError(f"Provided destination directory does not exist: {destination_dir}")
        self.destination_dir = destination_dir

        self.single_file_path: Optional[Path] = None
        if single_file:
            # Ensure the filename ends with .pbix
            file_name = single_file if single_file.endswith(".pbix") else f"{single_file}.pbix"
            file_path = self.source_dir / file_name
            if not file_path.is_file():
                raise FileNotFoundError(f"Provided file does not exist: {file_path}")
            self.single_file_path = file_path

        self.errors: List[str] = []

    def unzip_one_file(self, file_path: Path):
        """
        Unzips a single .pbix file to a subdirectory in the destination.

        Args:
            file_path: The full path to the .pbix file.
        """
        if file_path.suffix != ".pbix":
            logging.warning(f"Skipping non-pbix file: {file_path.name}")
            return

        target_path = self.destination_dir / file_path.stem

        # If the target directory already exists, remove it to ensure a clean extraction.
        if target_path.exists():
            logging.info(f"Removing existing directory: '{target_path}'")
            try:
                shutil.rmtree(target_path)
            except OSError as e:
                logging.error(f"Error removing directory '{target_path}': {e}")
                self.errors.append(f"Could not remove old directory for '{file_path.name}': {e}")
                return

        logging.info(f"Extracting '{file_path.name}' to '{target_path}'...")

        try:
            with zipfile.ZipFile(str(file_path), "r") as zip_ref:
                zip_ref.extractall(target_path)
        except zipfile.BadZipFile:
            error_msg = f"Could not extract '{file_path.name}'. It may be corrupt or password-protected."
            logging.error(error_msg)
            self.errors.append(error_msg)
        except Exception as e:
            error_msg = f"An unexpected error occurred while extracting '{file_path.name}': {e}"
            logging.error(error_msg)
            self.errors.append(error_msg)

    def unzip_all(self):
        """Iterates through the source directory and unzips all .pbix files."""
        files_to_unzip = list(self.source_dir.glob("*.pbix"))
        if not files_to_unzip:
            logging.warning(f"No .pbix files found in {self.source_dir}")
            return

        for file in tqdm(files_to_unzip, desc="Unzipping .pbix files"):
            self.unzip_one_file(file)

    def run(self):
        """Runs the unzipping process for either a single file or all files."""
        if self.single_file_path:
            self.unzip_one_file(self.single_file_path)
        else:
            self.unzip_all()

        if self.errors:
            logging.warning("\n--- Extraction Summary: Encountered Errors ---")
            for error in self.errors:
                logging.warning(f"- {error}")
            logging.warning("---------------------------------------------")
        else:
            logging.info("Unzipping process completed successfully with no errors.")


if __name__ == '__main__':
    # Define paths relative to the project root for better portability
    # Assumes your script is in a 'pbi-insights' folder, and 'data' is a sibling to 'pbi-insights'
    PROJECT_ROOT = Path(__file__).parent.parent
    DATA_PATH = PROJECT_ROOT / "data"
    RAW_PBI_PATH = DATA_PATH / "pbi_files"
    UNZIPPED_PATH = DATA_PATH / "unzipped_pbi_folders"

    # Ensure directories exist
    RAW_PBI_PATH.mkdir(parents=True, exist_ok=True)
    UNZIPPED_PATH.mkdir(parents=True, exist_ok=True)

    try:
        unzipper = Unzipper(RAW_PBI_PATH, UNZIPPED_PATH)
        unzipper.run()
    except (NotADirectoryError, FileNotFoundError) as e:
        logging.error(f"Initialization failed: {e}")
