from ingest_data import DividendDataProcessor
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

ingest = DividendDataProcessor()

class DataFrameCreator:

    def create_spark_dataframe(self):
        """
        Create a Spark DataFrame from the processed data.

        Args:
            data (list): A list of tuples containing the processed dividend data.

        Returns:
            DataFrame: The Spark DataFrame containing the dividend data.
        """
        
        data = ingest.fetch_random_dividend_data()
        
        spark = SparkSession.builder.getOrCreate()
        schema = StructType([
            StructField('Symbol', StringType(), False),
            StructField('Date', StringType()),
            StructField('Label', StringType()),
            StructField('Adjusted_Dividend', StringType()),
            StructField('Dividend', StringType()),
            StructField('Record_Date', StringType()),
            StructField('Payment_Date', StringType()),
            StructField('Declaration_Date', StringType()),
            StructField('Remark', StringType(), False)
        ])
        
        df = spark.createDataFrame(data, schema=schema)
        
        return df

