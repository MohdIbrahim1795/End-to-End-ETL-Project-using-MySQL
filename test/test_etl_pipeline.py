import unittest
from pyspark.sql import SparkSession
from etl_pipeline import extract_data, transform_data

class TestETLPipeline(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder \
            .appName("TestETL") \
            .master("local[*]") \
            .getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_extract_data(self):
        # Mock data
        data = [("2023-01-01", 50, "A", 0.5, True)]
        columns = ["timestamp", "value", "category", "Usages", "Outcome"]
        df = self.spark.createDataFrame(data, columns)
        
        # Test schema validation
        self.assertEqual(len(extract_data().columns), 5)

    def test_transform_data(self):
        # Mock data
        data = [("2023-01-01", 50, "A", 0.5, True)]
        columns = ["timestamp", "value", "category", "Usages", "Outcome"]
        df = self.spark.createDataFrame(data, columns)
        
        # Test transformation
        transformed_df = transform_data(df)
        self.assertIn("value_category", transformed_df.columns)

if __name__ == "__main__":
    unittest.main()