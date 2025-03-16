from pyspark.sql import SparkSession
from pyspark.sql.functions import year, month, col, when
import config

def extract_data():
    """Extract data from MySQL source table."""
    try:
        spark = SparkSession.builder \
            .appName("MySQLETL") \
            .config("spark.jars", "mysql-connector-j-8.0.33.jar")\
            .getOrCreate()

        raw_df = spark.read \
            .format("jdbc") \
            .option("url", f"jdbc:mysql://{config.MYSQL_HOST}:{config.MYSQL_PORT}/{config.MYSQL_DATABASE}") \
            .option("dbtable", config.SOURCE_TABLE) \
            .option("user", config.MYSQL_USER) \
            .option("password", config.MYSQL_PASSWORD) \
            .option("driver", "com.mysql.cj.jdbc.Driver") \
            .load()
        
        return raw_df, spark
    except Exception as e:
        print(f"Error during extraction: {e}")
        raise

def transform_data(raw_df):
    """Perform transformations on the raw data."""
    try:
        transformed_df = raw_df.withColumn("year", year("timestamp")) \
            .withColumn("month", month("timestamp")) \
            .withColumn("value_category", 
                        col("value") * when(col("category") == "A", 1.1).otherwise(1.0))
        
        return transformed_df
    except Exception as e:
        print(f"Error during transformation: {e}")
        raise

def load_data(transformed_df, spark):
    """Load transformed data to MySQL target table."""
    try:
        transformed_df.write \
            .format("jdbc") \
            .option("url", f"jdbc:mysql://{config.MYSQL_HOST}:{config.MYSQL_PORT}/{config.MYSQL_DATABASE}") \
            .option("dbtable", config.TARGET_TABLE) \
            .option("user", config.MYSQL_USER) \
            .option("password", config.MYSQL_PASSWORD) \
            .option("driver", "com.mysql.cj.jdbc.Driver") \
            .mode("overwrite") \
            .save()
        
        print(f"Data loaded to {config.TARGET_TABLE} successfully!")
    except Exception as e:
        print(f"Error during loading: {e}")
        raise
    finally:
        spark.stop()

def main():
    """Main ETL pipeline."""
    try:
        # Extract
        raw_df, spark = extract_data()
        
        # Transform
        transformed_df = transform_data(raw_df)
        
        # Load
        load_data(transformed_df, spark)
        
        print("ETL pipeline completed successfully!")
    except Exception as e:
        print(f"ETL pipeline failed: {e}")
        raise

if __name__ == "__main__":
    main()