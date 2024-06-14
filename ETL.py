import findspark

findspark.init()
findspark.find()

import logging
import boto3
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
import json
from pyspark.sql.types import StringType
import re
import snowflake.connector
from dotenv import load_dotenv
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv()


def getBucketName():
    bucket_name = "s3sparkpoc"
    return bucket_name


def getSnowflakeAccountDetails():
    snowflake_url = os.getenv("SNOWFLAKE_URL")
    snowflake_account = os.getenv("SNOWFLAKE_ACCOUNT")
    snowflake_user = os.getenv("SNOWFLAKE_USER")
    snowflake_password = os.getenv("SNOWFLAKE_PASSWORD")
    return snowflake_url, snowflake_account, snowflake_user, snowflake_password


def getDatabaseDetails():
    database_name = "S3SPARKTRIAL"
    schema_name = "s3Sink"
    table_name = "users"
    return database_name, schema_name, table_name


def createSparkConnection():
    spark = None
    try:
        spark = (
            SparkSession.builder.appName("S3toSparktoSnowflakePOC")
            .config(
                "spark.jars.packages",
                "net.snowflake:snowflake-jdbc:3.12.0,net.snowflake:spark-snowflake_2.12:2.9.0-spark_3.1",
            )
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel("ERROR")
        logger.info(f"Spark session created successfully.")
    except Exception as e:
        logger.error(f"Error creating Spark session: {e}")
    return spark


def awsClient():
    aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
    aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
    region_name = os.getenv("REGION_NAME")

    s3_client = None
    try:
        s3_client = boto3.client(
            "s3",
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=region_name,
        )
        logger.info(f"AWS client created successfully")
    except Exception as e:
        logger.error(f"AWS client creation unsuccesful: {e}")

    return s3_client


def printS3FilesList(s3_client):
    fileCount = 0
    bucket_name = getBucketName()
    try:
        response = s3_client.list_objects_v2(Bucket=bucket_name)
        if "Contents" in response:
            print("Files in bucket:")
            for obj in response["Contents"]:
                print(f"  {fileCount+1}. {obj['Key']}")
                fileCount += 1
            logger.info(f"{fileCount} files present in the S3 bucket")
        else:
            logger.warn(f"No files found in bucket {bucket_name}")
    except Exception as e:
        logger.error(f"Error in reading the S3 bucket: {e}")


def extract(s3_client, spark):
    bucket_name = getBucketName()
    response = s3_client.list_objects_v2(Bucket=bucket_name)
    json_list = []
    for obj in response.get("Contents", []):
        key = obj["Key"]
        if key.endswith(".json"):
            file_obj = s3_client.get_object(Bucket=bucket_name, Key=key)
            file_content = file_obj["Body"].read().decode("utf-8")
            json_data = json.loads(file_content)
            json_list.append(json_data)

    flattened_json_list = []
    for sublist in json_list:
        for item in sublist["results"]:
            flattened_json_list.append(item)
    df = spark.createDataFrame(flattened_json_list)
    return df


def printRawDataFrame(df):
    print("Full DataFrame")
    df.show()
    print()
    logger.info(f"name and location columns have nested values")
    print(f"Printing the first row of the longer columns (name, location)")
    print(f"name column", df.select("name").collect()[0])
    print(f"location column", df.select("location").collect()[0])


def transform(df):
    def firstName(name):
        return name.get("first", "")

    def lastName(name):
        return name.get("last", "")

    def location(location):
        return str(
            location.get("street", "")
            + ", "
            + location.get("city")
            + ", "
            + location.get("state")
            + ", "
            + location.get("postcode")
            + ", "
            + location.get("country")
        )

    def phone(phone_number):
        return re.sub(r"\D", "", phone_number)

    locationUDF = udf(location, StringType())
    firstUDF = udf(firstName, StringType())
    lastUDF = udf(lastName, StringType())
    phoneUDF = udf(phone, StringType())

    df_transformed = (
        df.withColumn("firstName", firstUDF(df.name))
        .withColumn("lastName", lastUDF(df.name))
        .withColumn("location", locationUDF(df.location))
        .withColumn("phone", phoneUDF(df.phone))
        .drop("name")
    )

    return df_transformed


def printTransformedDataframe(df_transformed):
    print("Full DataFrame")
    df_transformed.show()
    print()
    print(f"Printing the first row of the firstName, lastName, location, phone")
    print(
        df_transformed.select("firstName", "lastName", "location", "phone").collect()[
            0
        ],
    )


def snowflakeSetup():
    snowflake_url, snowflake_account, snowflake_user, snowflake_password = (
        getSnowflakeAccountDetails()
    )

    database_name, schema_name, table_name = getDatabaseDetails()

    schema_creation_sql = f"CREATE SCHEMA IF NOT EXISTS {schema_name}"

    table_creation_sql = f"""
    CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} (
        dob DATE,
        email VARCHAR PRIMARY KEY,
        gender VARCHAR,
        location VARCHAR,
        phone VARCHAR,
        registered DATE,
        first_name VARCHAR,
        last_name VARCHAR
    )
    """

    def create_schema(cursor):
        try:
            cursor.execute(schema_creation_sql)
            logger.info(f"Schema '{schema_name}' created successfully")
        except Exception as e:
            logger.error(f"Failed to create schema: {e}")

    def create_table(cursor):
        try:
            cursor.execute(table_creation_sql)
            print(
                f"Table '{table_name}' created successfully in schema '{schema_name}'"
            )
        except Exception as e:
            logger.error(
                f"Failed to create table '{table_name}' in schema '{schema_name}': {e}"
            )

    try:
        conn = snowflake.connector.connect(
            account=snowflake_account,
            user=snowflake_user,
            password=snowflake_password,
            database=database_name,
        )
        logger.info(f"Connection successful!")

        cursor = conn.cursor()
        create_schema(cursor)
        create_table(cursor)

    except Exception as e:
        logger.error(f"Failed to connect to Snowflake: {e}")

    finally:
        if conn:
            conn.close()
            logger.info(f"Connection closed")


def load(df_transformed):
    snowflake_url, snowflake_account, snowflake_user, snowflake_password = (
        getSnowflakeAccountDetails()
    )
    database_name, schema_name, table_name = getDatabaseDetails()

    snowflake_options = {
        "sfURL": snowflake_url,
        "sfUser": snowflake_user,
        "sfPassword": snowflake_password,
        "sfDatabase": database_name,
        "sfSchema": schema_name,
    }
    try:
        df_transformed.write.format("net.snowflake.spark.snowflake").options(
            **snowflake_options
        ).option("dbtable", table_name).mode("overwrite").save()
        logger.info(f"DataFrame succesfully inserted to Snowflake")

    except Exception as e:
        logger.error(f"Error writing DataFrame to Snowflake: {e}")


if __name__ == "__main__":
    spark = createSparkConnection()
    s3_client = awsClient()
    printS3FilesList(s3_client)
    df = extract(s3_client, spark)
    printRawDataFrame(df)
    df_transformed = transform(df)
    printTransformedDataframe(df_transformed)
    snowflakeSetup()
    load(df_transformed)
