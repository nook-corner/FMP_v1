#Test transform
from test_ingest_data import DividendDataProcessor
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

ingest = DividendDataProcessor()

class DataFrameCreator:

    def create_spark_dataframe(self):
        """
        Create a Spark DataFrame from the processed data.

        Returns:
            DataFrame: The Spark DataFrame containing the dividend data.
        """
        data = ingest.fetch_fixed_dividend_data()

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

def test_create_spark_dataframe():
    # Initialize the DataFrameCreator
    data_frame_test = DataFrameCreator()

    # Call the create_spark_dataframe method
    df = data_frame_test.create_spark_dataframe()

    # Perform your assertions to validate the DataFrame
    # Check if DataFrame is not None
    assert df is not None
    # Check if DataFrame has data
    assert df.count() > 0
    # Check if 'Symbol' column is not null
    assert df.filter(df['Symbol'].isNull()).count() == 0

def test_schema():
    # Initialize the DataFrameCreator
    schema_test = DataFrameCreator()

    # Call the create_spark_dataframe method
    df = schema_test.create_spark_dataframe()

    # Define the expected schema
    expected_schema = StructType([
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

    # Get the actual DataFrame schema
    actual_schema = df.schema

    # Perform assertion to check if actual schema matches the expected schema
    assert actual_schema == expected_schema
