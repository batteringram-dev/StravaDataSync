## Strava Data Pipeline - Architecture

![Strava Project's Architecture](https://github.com/user-attachments/assets/21327dcd-e0a3-42a1-8110-10244aaffa56)


### Strava's API
Strava has a great API that lets you connect and extract your data for your own use which is simply brilliant! The goal is to extract all of my personal data, and build an ETL pipeline to load the data into Amazon Redshift.

### Transformation - main.py
- Standardized column names and dtypes
- Implemented data quality checks using Pandera to ensure high integrity

### S3 - s3_to_redshift.py
Processed data is loaded here and ready to get pushed into Redshift for storage and analysis. Additionally, you could use Athena here as well to analyse the data.

### Redshift - queries_on_redshift.sql
Data is loaded from S3 to Redshift in Python for doing analysis on my own data to derive insights!

![Redshift Screenshot](https://github.com/user-attachments/assets/106dc4dd-1e48-4483-8745-de12a935d3f5)
