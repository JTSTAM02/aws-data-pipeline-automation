import boto3
import pandas as pd
from datetime import datetime
from io import StringIO
import os

def transform_and_upload():
    s3 = boto3.client("s3")
    athena = boto3.client("athena")
    bucket = "tyler-data-pipeline-2025"
    database = "taxi_demo"
    table = "trips_processed"

    # 1. List all objects under raw/
    response = s3.list_objects_v2(Bucket=bucket, Prefix="raw/")
    objects = response.get("Contents", [])
    csv_files = [obj for obj in objects if obj["Key"].endswith(".csv")]
    if not csv_files:
        raise FileNotFoundError("❌ No CSV files found in raw/ folder on S3")

    # 2. Pick the latest file by LastModified timestamp
    latest_obj = max(csv_files, key=lambda x: x["LastModified"])
    latest_key = latest_obj["Key"]
    print(f"📂 Using latest raw file: {latest_key}")

    # 3. Load raw data from S3
    obj = s3.get_object(Bucket=bucket, Key=latest_key)
    df = pd.read_csv(StringIO(obj["Body"].read().decode("utf-8")))

    # 4. Transform
    df["fare_per_mile"] = df["total_amount"] / df["trip_distance"]

    # 5. Extract raw filename
    raw_filename = os.path.basename(latest_key)

    # 6. Partition path (year/month/day)
    today = datetime.today()
    year, month, day = today.strftime("%Y"), today.strftime("%m"), today.strftime("%d")
    output_csv = raw_filename
    df.to_csv(output_csv, index=False)
    processed_key = f"processed/year={year}/month={month}/day={day}/{raw_filename}"

    # 7. Upload processed CSV to S3
    s3.upload_file(output_csv, bucket, processed_key)
    print(f"✅ Uploaded processed data to s3://{bucket}/{processed_key}")

    # 8. Update run history log
    log_key = "processed/logs/run_history.csv"
    run_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    try:
        log_obj = s3.get_object(Bucket=bucket, Key=log_key)
        log_df = pd.read_csv(log_obj["Body"])
    except s3.exceptions.NoSuchKey:
        log_df = pd.DataFrame(columns=["raw_key", "processed_key", "run_time"])
    new_row = {"raw_key": latest_key, "processed_key": processed_key, "run_time": run_time}
    log_df = pd.concat([log_df, pd.DataFrame([new_row])], ignore_index=True)
    log_df.to_csv("run_history.csv", index=False)
    s3.upload_file("run_history.csv", bucket, log_key)
    print(f"📝 Run history updated at s3://{bucket}/{log_key}")

    # 9. Add Athena partition
    partition_query = f"""
    ALTER TABLE {database}.{table}
    ADD IF NOT EXISTS PARTITION (year='{year}', month='{month}', day='{day}')
    LOCATION 's3://{bucket}/processed/year={year}/month={month}/day={day}/'
    """
    response = athena.start_query_execution(
        QueryString=partition_query,
        QueryExecutionContext={"Database": database},
        ResultConfiguration={"OutputLocation": f"s3://{bucket}/athena-results/"}
    )
    print(f"⚡ Athena partition added for {year}-{month}-{day}")

if __name__ == "__main__":
    transform_and_upload()
