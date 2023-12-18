import os
import csv
from datetime import datetime

from defining_tables import PropertyDataRaw
from base import db_session
from sqlalchemy import text

# Configuration settings
project_dir = os.path.abspath(__file__ + "/../../")

# Paths configuration for the latest dataset

# Path for the raw dataset file
dataset_raw_path = f"{project_dir}/data/raw/fetched_2023-03/historical_data.csv"

def format_to_lowercase(input_str):
    """
    Convert a string to lowercase.
    """
    return input_str.lower()

def reformat_date(date_str):
    """
    Reformat date from DD/MM/YYYY to YYYY-MM-DD format.
    """
    date_obj = datetime.strptime(date_str, "%d/%m/%Y")
    return date_obj.strftime("%Y-%m-%d")

def simplify_description(desc):
    """
    Simplify the property description to basic categories.
    """
    desc = format_to_lowercase(desc)
    if "new" in desc:
        return "new"
    elif "second-hand" in desc:
        return "second-hand"
    return desc

def convert_price(price_str):
    """
    Convert price string to integer, removing currency symbols and commas.
    """
    price_str = price_str.replace("€", "").replace(",", "")
    return int(float(price_str))

def reset_table():
    """
    Clear the 'property_data_raw' table and reset its primary key.
    """
    db_session.execute(text("TRUNCATE TABLE property_data_raw RESTART IDENTITY;"))
    db_session.commit()

def transform_and_load_data():
    """
    Apply transformations to the raw data and load it into the database.
    """
    with open(dataset_raw_path, mode="r", encoding="windows-1252") as file:
        reader = csv.DictReader(file)
        raw_data_objects = []
        for row in reader:
            raw_data_objects.append(
                PropertyDataRaw(
                    sale_date=reformat_date(row["date_of_sale"]),
                    location=format_to_lowercase(row["address"]),
                    zip_code=format_to_lowercase(row["postal_code"]),
                    region=format_to_lowercase(row["county"]),
                    sale_price=convert_price(row["price"]),
                    property_desc=simplify_description(row["description"]),
                )
            )
        db_session.bulk_save_objects(raw_data_objects)
        db_session.commit()

def run_transformation():
    print("[Data Transformation] Starting process")
    reset_table()
    print("[Data Transformation] Transforming and loading new data")
    transform_and_load_data()
    print("[Data Transformation] Process completed")

if __name__ == '__main__':
    run_transformation()
