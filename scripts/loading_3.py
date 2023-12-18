from base import db_session
from defining_tables import PropertyDataRaw, PropertyDataClean

from sqlalchemy import cast, Integer, Date
from sqlalchemy.dialects.postgresql import insert

def add_new_transactions():
    """
    Function to insert new transactions into the clean table from the raw table.
    """
    # Select transaction IDs from the clean table for comparison
    existing_transaction_ids = db_session.query(PropertyDataClean.unique_transaction_id)

    # Prepare data to insert: cast date_of_sale and sale_price to appropriate data types
    new_transactions = db_session.query(
        cast(PropertyDataRaw.sale_date, Date),
        PropertyDataRaw.location,
        PropertyDataRaw.zip_code,
        PropertyDataRaw.region,
        cast(PropertyDataRaw.sale_price, Integer),
        PropertyDataRaw.property_desc,
    ).filter(~PropertyDataRaw.unique_transaction_id.in_(existing_transaction_ids))

    # Count and print the number of new transactions to be inserted
    print("New transactions to insert:", new_transactions.count())

    # Create an insert statement for new transactions
    insert_statement = insert(PropertyDataClean).from_select(
        ["sale_date", "location", "zip_code", "region", "sale_price", "property_desc"],
        new_transactions,
    )

    # Execute the insert operation
    db_session.execute(insert_statement)
    db_session.commit()

def remove_old_transactions():
    """
    Function to delete transactions from the clean table that are no longer in the raw table.
    """
    # Get transaction IDs from the raw table
    raw_transaction_ids = db_session.query(PropertyDataRaw.unique_transaction_id)

    # Identify transactions to delete from the clean table
    transactions_to_remove = db_session.query(PropertyDataClean).filter(
        ~PropertyDataClean.unique_transaction_id.in_(raw_transaction_ids)
    )

    # Count and print the number of transactions to be deleted
    print("Transactions to delete:", transactions_to_remove.count())

    # Execute the delete operation
    transactions_to_remove.delete(synchronize_session=False)
    db_session.commit()

def run_load_operations():
    print("[Data Loading] Starting process")
    print("[Data Loading] Adding new transactions")
    add_new_transactions()
    print("[Data Loading] Removing outdated transactions")
    remove_old_transactions()
    print("[Data Loading] Process completed")

if __name__ == '__main__':
    run_load_operations()
