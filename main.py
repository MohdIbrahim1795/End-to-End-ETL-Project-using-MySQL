from source import generate_data
from source_data_load import load_csv_to_mysql
from etl_pipeline import main as run_etl
from test_etl_pipeline import TestETL
import unittest

def run_tests():
    """Run unit tests."""
    suite = unittest.TestLoader().loadTestsFromTestCase(TestETL)
    unittest.TextTestRunner().run(suite)

if __name__ == "__main__":
    # Step 1: Generate source data
    generate_data()

    # Step 2: Load CSV data into MySQL
    load_csv_to_mysql()

    # Step 3: Run ETL pipeline
    run_etl()

    # Step 4: Run unit tests
    run_tests()