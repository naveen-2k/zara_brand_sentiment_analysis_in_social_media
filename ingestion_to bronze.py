from pyspark.sql.functions import col, regexp_extract, current_timestamp
import shutil
import re

# Read dataset metadata for 'post_detail'
dataset_metadata = spark.sql("""
    SELECT * FROM hive_metastore.pipeline_metastore.dataset 
    WHERE name = 'post_detail' AND source_name = 'facebook'
""").collect()[0]

# Extract metadata
bronze_path = dataset_metadata['bronze_path']
landing_path = dataset_metadata['source_path']
checkpoint_path = dataset_metadata['schema_information']
unknown_path = dataset_metadata['unknown_file_path']
expected_filename_format = dataset_metadata['file_name_format']
expected_filename = dataset_metadata['file_name']
ingestion_metadata_id = dataset_metadata['id']
file_format = dataset_metadata['format']
current_user = spark.sql("SELECT current_user()").collect()[0][0]

# Regex pattern from config
filename_date_pattern = rf"{expected_filename_format}"

# Audit table
audit_table = "hive_metastore.pipeline_metastore.dataset_audit"

# Audit logging function with business date from filename
def log_audit(dataset_id, business_date, status, error_message=""):
    existing = spark.sql(f"""
        SELECT COUNT(*) FROM {audit_table}
        WHERE dataset_id = {dataset_id} AND buisness_date = DATE('{business_date}')
    """).collect()[0][0]

    if existing == 0:
        spark.sql(f"""
            INSERT INTO {audit_table}
            (dataset_id, buisness_date, status, error_message, created_by, created_timestamp, modified_timestamp)
            VALUES
            ({dataset_id}, DATE('{business_date}'), '{status}', '{error_message}', '{current_user}', current_timestamp(), current_timestamp())
        """)
    else:
        spark.sql(f"""
            UPDATE {audit_table}
            SET status = '{status}',
                error_message = '{error_message}',
                modified_by = '{current_user}',
                modified_timestamp = current_timestamp()
            WHERE dataset_id = {dataset_id} AND buisness_date = DATE('{business_date}')
        """)

# Move invalid files to unknown folder
def move_to_unknown(file_path, filename):
    unknown_file_path = f"{unknown_path}/{filename}"
    shutil.move(file_path, unknown_file_path)
    print(f"Moved {file_path} ➝ {unknown_file_path}")

# Write batch files to Bronze layer
def write_to_bronze(batch_df, batch_id):
    if batch_df.isEmpty():
        return

    for file in batch_df.select("filename", "source_file", "year", "month", "day", "file_date").distinct().collect():
        filename = file['filename']
        file_path = file['source_file']
        business_date = file['file_date']  # Extracted from filename (yyyy-mm-dd)

        if not re.match(filename_date_pattern, filename):
            log_audit(ingestion_metadata_id, business_date, "Failed", f"Filename {filename} format mismatch")
            move_to_unknown(file_path, filename)
            continue

        log_audit(ingestion_metadata_id, business_date, "Started")
        try:
            print(f"📥 Ingesting {filename}...")
            log_audit(ingestion_metadata_id, business_date, "In-Progress")

            batch_df.filter(col("filename") == filename).write \
                .format(file_format) \
                .option("header", "true") \
                .mode("append") \
                .save(f"{bronze_path}/{file['year']}/{file['month']}/{file['day']}/{filename}")

            log_audit(ingestion_metadata_id, business_date, "Success")
            print(f"Ingestion Success for {filename}")

        except Exception as e:
            log_audit(ingestion_metadata_id, business_date, "Failed", str(e))
            print(f"Error processing {filename}: {e}")

# Define streaming DataFrame
df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", file_format)
    .option("header", "true")
    .option("multiLine", "true")
    .option("cloudFiles.inferColumnTypes", "true")
    .option("cloudFiles.schemaLocation", checkpoint_path)
    .option("cloudFiles.schemaEvolutionMode", "rescue")
    .load(landing_path)
    .withColumn("source_file", col("_metadata.file_path"))
    .withColumn("file_date", regexp_extract(col("_metadata.file_path"), filename_date_pattern, 1)) 
    .withColumn("year", regexp_extract(col("file_date"), r"(\d{4})", 1))
    .withColumn("month", regexp_extract(col("file_date"), r"-(\d{2})-", 1))
    .withColumn("day", regexp_extract(col("file_date"), r"-(\d{2})_", 1))
    .withColumn("filename", regexp_extract(col("source_file"), r".*/(.*)", 1))
)

# Start the streaming ingestion
df.writeStream \
    .foreachBatch(write_to_bronze) \
    .option("checkpointLocation", checkpoint_path) \
    .start()

print(" Ingestion started for post_detail...")
