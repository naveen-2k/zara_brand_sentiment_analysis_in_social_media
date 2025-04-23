from pyspark.sql.functions import col, lit, concat_ws
from delta.tables import DeltaTable
from pyspark.sql.utils import AnalysisException


def table_exists(catalog, silver_schema, error_table_name):
    try:
        result = spark.sql(f"SHOW TABLES IN {catalog}.{silver_schema}").filter(f"tableName = '{error_table_name}'")
        return result.count() > 0  # Returns True if table exists, False otherwise
    except AnalysisException:
        return False


# ───────────────────────────────────────────────────────────────
# Function to perform null validation
def validate_nulls(df, column_name):
    return df.filter(col(column_name).isNull())

# ───────────────────────────────────────────────────────────────
# Retrieve widget parameters
dbutils.widgets.text("dcv_id", "")
dbutils.widgets.text("column_name", "")
dbutils.widgets.text("column_datatype", "")
dbutils.widgets.text("dataset_id", "")
dbutils.widgets.text("business_date", "")
dbutils.widgets.text("year", "")
dbutils.widgets.text("month", "")
dbutils.widgets.text("day", "")
dbutils.widgets.text("bronze_path", "")
dbutils.widgets.text("format", "")
dbutils.widgets.text("dataset_name", "")
dbutils.widgets.text("error_table_name", "")
dbutils.widgets.text("catalog", "")
dbutils.widgets.text("silver_schema", "")

# Fetch widget values
dataset_column_validation_id = int(dbutils.widgets.get("dcv_id"))
column_name = dbutils.widgets.get("column_name")
column_datatype = dbutils.widgets.get("column_datatype")
dataset_id = int(dbutils.widgets.get("dataset_id"))
business_date = dbutils.widgets.get("business_date")
year = dbutils.widgets.get("year")
month = dbutils.widgets.get("month")
day = dbutils.widgets.get("day")
bronze_path = dbutils.widgets.get("bronze_path")
format = dbutils.widgets.get("format")
dataset_name = dbutils.widgets.get("dataset_name")
error_table_name = dbutils.widgets.get("error_table_name")
catalog = dbutils.widgets.get("catalog")
silver_schema = dbutils.widgets.get("silver_schema")

# ───────────────────────────────────────────────────────────────
# Load source data
if format == 'csv':
    df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(f"{bronze_path}/{year}/{month}/{day}/")
elif format == 'json':
    df = spark.read.format("json").option("multiline", "false").load(f"{bronze_path}/{year}/{month}/{day}/*/")

# ───────────────────────────────────────────────────────────────
# Run null validation on the given column
error_df = validate_nulls(df, column_name)

# Proceed only if there are null rows
if error_df.count() > 0:
    error_df = error_df.withColumn("error_message", lit(f"Column {column_name} is null"))

    # Get Delta table reference
    full_error_table = f"{catalog}.{silver_schema}.{error_table_name}"

    # Check if the table exists
    if not table_exists(catalog, silver_schema, error_table_name):
        print(f"Table {full_error_table} does not exist. Creating it now.")
        error_df.write.format("delta").mode("overwrite").saveAsTable(full_error_table)
    
    # Get the DeltaTable reference
    delta_table = DeltaTable.forName(spark, full_error_table)

    # Perform the upsert (MERGE)
    merge_columns = [c for c in error_df.columns if c != "error_message"]
    merge_condition = " AND ".join([f"target.{c} <=> source.{c}" for c in merge_columns])

    delta_table.alias("target").merge(
        error_df.alias("source"),
        merge_condition
    ).whenMatchedUpdate(
        set={"error_message": concat_ws(" | ", col("target.error_message"), col("source.error_message"))}
    ).whenNotMatchedInsertAll().execute()

    dbutils.notebook.exit("Validation Passed")



else:
    dbutils.notebook.exit("Validation Passed with no errors")
