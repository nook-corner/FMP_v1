from pyspark.sql.functions import to_date, year, col
from transform import DataFrameCreator

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

