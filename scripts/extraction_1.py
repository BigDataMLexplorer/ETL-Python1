import os
import csv
import tempfile
from zipfile import ZipFile

import requests

# Configuration settings
work_dir = "C:/Users/42077/Desktop/github/DataPipeline-Project"

# START - Configuration for new dataset - March 2023

# URL of the dataset
dataset_url = "https://example.com/datasets/2023-03/historical_data.zip"

# Local path to store the downloaded dataset
dataset_download_path = f"{work_dir}/data/fetched_2023-03/historical_data.zip"

# Path to store the extracted dataset file
extracted_file_path = f"{work_dir}/data/fetched_2023-03/historical_data.csv"


# END - Configuration for new dataset - March 2023

def ensure_directory_exists(folder_path):
    os.makedirs(os.path.dirname(folder_path), exist_ok=True)


def fetch_dataset():
    ensure_directory_exists(dataset_download_path)
    with open(dataset_download_path, "wb") as file:
        response = requests.get(dataset_url, verify=False)
        file.write(response.content)


def process_and_store_data():
    """
    Extract and process the dataset for future use
    """
    ensure_directory_exists(extracted_file_path)
    with tempfile.TemporaryDirectory() as temp_dir:
        with ZipFile(dataset_download_path, "r") as zip_ref:
            file_list = zip_ref.namelist()
            csv_file = zip_ref.extract(file_list[0], path=temp_dir)

            with open(csv_file, mode="r", encoding="windows-1252") as input_file:
                reader = csv.DictReader(input_file)

                # Print the first row for verification
                first_row = next(reader)
                print("[Data Processing] First row example:", first_row)

                with open(extracted_file_path, mode="w", encoding="windows-1252") as output_file:
                    # Renaming fields for consistency
                    field_mapping = {
                        "Date of Sale (dd/mm/yyyy)": "sale_date",
                        "Address": "property_address",
                        "Postal Code": "zip_code",
                        "County": "region",
                        "Price (€)": "sale_price",
                        "Description of Property": "property_description",
                    }
                    writer = csv.DictWriter(output_file, fieldnames=field_mapping)
                    writer.writeheader()
                    for data_row in reader:
                        writer.writerow(data_row)


def run_pipeline():
    print("[Data Pipeline] Initialization")
    print("[Data Pipeline] Downloading dataset")
    fetch_dataset()
    print(f"[Data Pipeline] Processing data from '{dataset_download_path}' to '{extracted_file_path}'")
    process_and_store_data()
    print("[Data Pipeline] Process Completed")


if __name__ == '__main__':
    run_pipeline()
