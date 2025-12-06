import boto3
import csv
from datetime import datetime
import os
from io import StringIO

def transform_and_upload():
    s3 = boto3.client("s3")
    athena = boto3.client("athena")
    bucket = "tyler-data-pipeline-2025"
    database = "taxi_demo"
    table = "trips_processed"

    response = s3.list_objects_v2(Bucket=bucket, Prefix="raw/")
    objects = response.get("Contents", [])
    csv_files = [obj for obj in objects if obj["Key"].endswith(".csv")]
    if not csv_files:
        raise FileNotFoundError("No CSV files found")

    latest_obj = max(csv_files, key=lambda x: x["LastModified"])
    latest_key = latest_obj["Key"]

    obj = s3.get_object(Bucket=bucket, Key=latest_key)
    csv_content = obj["Body"].read().decode("utf-8")
    reader = csv.DictReader(StringIO(csv_content))
    rows = list(reader)

    # Transform: add fare_per_mile
    for r in rows:
        trip_distance = float(r.get("trip_distance", 0) or 1)
        total_amount = float(r.get("total_amount", 0))
        r["fare_per_mile"] = total_amount / trip_distance

    raw_filename = os.path.basename(latest_key)
    today = datetime.today()
    year, month, day = today.strftime("%Y"), today.strftime("%m"), today.strftime("%d")
    processed_key = f"processed/year={year}/month={month}/day={day}/{raw_filename}"

    # Write CSV to S3
    output = StringIO()
    writer = csv.DictWriter(output, fieldnames=rows[0].keys())
    writer.writeheader()
    writer.writerows(rows)
    s3.put_object(Bucket=bucket, Key=processed_key, Body=output.getvalue())

    # Athena partition
    partition_query = f"""
    ALTER TABLE {database}.{table}
    ADD IF NOT EXISTS PARTITION (year='{year}', month='{month}', day='{day}')
    LOCATION 's3://{bucket}/processed/year={year}/month={month}/day={day}/'
    """
    athena.start_query_execution(
        QueryString=partition_query,
        QueryExecutionContext={"Database": database},
        ResultConfiguration={"OutputLocation": f"s3://{bucket}/athena-results/"}
    )

    return {"status": "success", "processed_key": processed_key}

def lambda_handler(event, context):
    return transform_and_upload()
