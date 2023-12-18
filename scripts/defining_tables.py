from sqlalchemy import cast, Column, Integer, String, Date
from sqlalchemy.orm import column_property

from base import EntityBase  # Importing the Base class for ORM

# Defining the ORM model for raw property price data
class PropertyDataRaw(EntityBase):
    __tablename__ = "property_data_raw"  # Name of the table in the database

    # Defining columns of the table
    record_id = Column(Integer, primary_key=True)  # Unique ID for each record
    sale_date = Column(String(55))  # Date of the property sale
    location = Column(String(255))  # Address of the property
    zip_code = Column(String(55))  # Postal code of the property
    region = Column(String(55))  # County where the property is located
    sale_price = Column(String(55))  # Sale price of the property
    property_desc = Column(String(255))  # Description of the property

    # Creating a unique transaction ID using several columns
    unique_transaction_id = column_property(
        sale_date + "_" + location + "_" + region + "_" + sale_price
    )

# Defining the ORM model for cleaned property price data
class PropertyDataClean(EntityBase):
    __tablename__ = "property_data_clean"  # Name of the table for clean data

    # Defining columns of the table
    record_id = Column(Integer, primary_key=True)  # Unique ID for each record
    sale_date = Column(Date)  # Date of the property sale (as Date object)
    location = Column(String(255))  # Address of the property
    zip_code = Column(String(55))  # Postal code of the property
    region = Column(String(55))  # County where the property is located
    sale_price = Column(Integer)  # Sale price of the property (as Integer)
    property_desc = Column(String(255))  # Description of the property

    # Creating a unique transaction ID combining various fields
    unique_transaction_id = column_property(
        cast(sale_date, String) + "_" + location + "_" + region + "_" + cast(sale_price, String)
    )
