import unittest
from pyspark.sql import SparkSession
import config

class TestETL(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder \
            .appName("TestETL") \
            .config("spark.jars", "D:\Data Engineering Projects\ETL Project using SQL\mysql-connector-j-8.0.33.jar") \
            .getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_extract_data(self):
        from etl_pipeline import extract_data
        raw_df, _ = extract_data()
        self.assertGreater(raw_df.count(), 0)

    def test_transform_data(self):
        from etl_pipeline import extract_data, transform_data
        raw_df, _ = extract_data()
        transformed_df = transform_data(raw_df)
        self.assertIn("value_category", transformed_df.columns)

if __name__ == "__main__":
    unittest.main()