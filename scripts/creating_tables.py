from base import EntityBase, engine  # Importing Base class and engine configuration

# Importing classes corresponding to the raw and clean data tables
from defining_tables import PropertyDataRaw, PropertyDataClean

# Main execution block
if __name__ == "__main__":
    # This script will create the database schema based on the defined ORM classes
    EntityBase.metadata.create_all(engine)
    # 'create_all' method generates the necessary SQL commands
    # to create tables and relationships based on the classes derived from 'Base'.
    # The 'engine' object connects to the database and executes these SQL commands.
