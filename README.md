# ETL-S3-Spark-Snowflake

An ETL pipeline of S3 to Spark to Snowflake.

## Overview
This project implements an ETL pipeline that extracts JSON data from an S3 bucket, transforms it using Apache Spark, and loads it into a Snowflake database. 

The json files uploaded to S3 can be viewed here (/jsonFilesInS3). They are a trimmed version of https://randomuser.me/api/ output.

<b>Extract</b>: Extract JSON data files from AWS S3.

<b>Transform</b>: Transform the extracted data with Apache Spark.

<b>Load</b>: Load the transformed data into Snowflake.

To run this code please set up your AWS S3 and Snowflake accounts and Snowflake database.

The terminal output of the ETL pipeline is found in terminalOutput.txt.
The loaded Snowflake output images can be found in /snowflakeOutput

## Technologies Used

<li> Python</li>
<li> PySpark </li>
<li> Snowflake </li>
<li> AWS S3 </li>

## Install and Run

1. Clone the repository:

    ```git clone https://github.com/Arun152k/ETL-S3-Spark-Snowflake.git```
2. Setup environment variables:
Create a .env file in the root directory with the following content:

    ```aws_access_key_id=your_aws_access_key_id
    aws_secret_access_key=your_aws_secret_access_key
    region_name=your_aws_s3_region
    snowflake_url=https://your_snowflake_url
    snowflake_account=your_snowflake_account
    snowflake_user=your_snowflake_user
    snowflake_password=your_snowflake_password```

3. Install required Python packages:

    ```pip install -r requirements.txt```

4. Run the Python script:

    ```python ETL.py```

## Future
I have already created (yet to upload to GitHub) an ETL pipeline that:
1. Uses Airflow to periodically extract data.
2. Pushes them into a Kafka Queue
3. Use PySpark to Stream and Transform
4. Load into Cassandra

I plan to integrate a Kafka-S3 connector to automatically upload the JSON data into S3, replacing the current manual upload process.
