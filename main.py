from filter import FilterYear

filter = FilterYear()

df = filter.filter_dataframe_by_date()

df.createOrReplaceTempView("temp_dividend")

# Here, you have the SQL-style code for inserting data to the table.
# Please ensure you are running this code in a Spark cluster or platform like Databricks,

'''
# Delete existing records from "raw_dividend" table for the current month.

delete from  viz_sandbox_db.raw_dividend where mnth_id = '${bsns_dt}'

# Delete existing records from "raw_dividend" table for the current month.

set spark.databricks.delta.retentionDurationCheck.enabled = false;

# Vacuum the "raw_dividend" table to clean up files and optimize performance.

vacuum  viz_sandbox_db.raw_dividend  RETAIN 0 hours;

# Insert filtered dividend data from the temporary view "temp_dividend" into "raw_dividend" table.

insert into viz_sandbox_db.raw_dividend 
select 
Symbol ,
Date    ,
Label     ,
CAST(Adjusted_Dividend AS DECIMAL)     ,
CAST(Dividend AS DECIMAL)     ,
Record_Date    ,
Payment_Date     ,
Declaration_Date    ,
Remark ,
current_timestamp() as dl_load_ts , 
'${bsns_dt}' as mnth_id
 
from temp_dividend

# Disable retention duration check again for post-insert operations.

set spark.databricks.delta.retentionDurationCheck.enabled = false;

# Vacuum the "raw_dividend" table again after insertion.

vacuum  viz_sandbox_db.raw_dividend  RETAIN 0 hours;

# Refresh the "raw_dividend" table to update metadata and cache.

refresh table viz_sandbox_db.raw_dividend ;
'''