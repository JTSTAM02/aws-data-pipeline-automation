# aws-data-pipeline-automation
End-to-end data pipeline using AWS S3, lambda, Glue, and Redshift to automate ingestion, transformation, and loading of CSV datasets into a cloud data warehouse

# Taxi ETL Pipeline (AWS Lambda + S3 + Athena)

## Current Pipeline
1. **Source:** CSV files uploaded to `s3://tyler-data-pipeline-2025/raw/`
2. **Transform:** Lambda function reads latest CSV, adds `fare_per_mile` column
3. **Load:** Processed CSV uploaded to `s3://tyler-data-pipeline-2025/processed/year=YYYY/month=MM/day=DD/` and Athena partition updated
4. **Automation:** S3 trigger automatically runs Lambda on new CSV uploads
5. **Monitoring:** CloudWatch logs track each Lambda run

## Usage
1. Upload raw CSV to `s3://tyler-data-pipeline-2025/raw/`
2. Lambda automatically triggers and processes the file
3. Check S3 and Athena for processed data

## Dependencies
- pandas
- boto3

# Future Plans

# AWS Data Pipeline Automation

### Overview
This project demonstrates an automated ETL data pipeline built using AWS cloud services. It ingests raw CSV files from Amazon S3, processes them with an AWS Lambda function using Python and Pandas, and stores cleaned, structured data into Amazon Redshift for analytics and BI reporting.

### Architecture
S3 → Lambda → Glue → Redshift  
- **S3:** Stores raw and processed data  
- **Lambda:** Handles ETL transformation logic  
- **Glue:** Manages data catalog and schema normalization  
- **Redshift:** Serves as the final data warehouse for reporting  
- **CloudWatch:** Monitors pipeline runs and logs  

### Features
- Automated daily ingestion of raw data files  
- Schema normalization and validation  
- Versioned storage of processed data  
- CI/CD pipeline with GitHub Actions  
- Scalable design for additional datasets or sources  

### Technologies
- AWS S3, Lambda, Glue, Redshift, CloudWatch  
- Python (Pandas, Boto3)  
- SQL  
- GitHub Actions (CI/CD)

### Future Enhancements
- Integrate Step Functions for orchestration  
- Add unit testing and error notifications via SNS  

---

### 🚀 Run Locally
Clone the repo:
```bash
git clone https://github.com/JTSTAM02/aws-data-pipeline-automation.git
cd aws-data-pipeline-automation
pip install -r requirements.txt
