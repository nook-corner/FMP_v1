#Test filter
from pyspark.sql.functions import to_date, year, col
from test_transform import DataFrameCreator

transform = DataFrameCreator()

class FilterYear :

    def filter_dataframe_by_date(self):
        """
        Filter data to keep only records with dates after 2020.

        Returns:
            DataFrame: The filtered Spark DataFrame containing records with dates after 2020.
        """

        df_spark = transform.create_spark_dataframe()

         # Convert the "Date" column to a DateType using "yyyy-MM-dd" format
        df_spark = df_spark.withColumn("Date", to_date(col("Date"), "yyyy-MM-dd"))

        # Filter the DataFrame to keep only records with dates after 2020
        df_spark = df_spark.filter(year(col("Date")) > 2020)

        return df_spark

def test_final_dataframe():
    # Initialize the DataFrameCreator
    test_final_dataframe = FilterYear()

    # Call the create_spark_dataframe method
    df_final = test_final_dataframe.filter_dataframe_by_date()

    # Get distinct symbol from dataframe
    distinct_symbols = df_final.select('Symbol').distinct().rdd.flatMap(lambda x: x).collect()

    # Expected symbol after filter
    expected_result = ["AAPL"]

    assert distinct_symbols == expected_result

    return distinct_symbols == expected_result

def main():
    result = test_final_dataframe()
    
    if result:
        print("Test passed. Distinct symbols after filter match the expected result.")
    else:
        print("Test failed. Distinct symbols after filter do not match the expected result.")

if __name__ == "__main__":
    main()


