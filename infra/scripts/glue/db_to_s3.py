import sys
import boto3
import json
import time
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

# Get job parameters
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'output_bucket',
    'database_secret_arn'
])

# Convert parameter names for easier access
output_bucket = args['output_bucket']
database_secret_arn = args['database_secret_arn']

# Initialize Glue context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

print("Starting MySQL to S3 ETL job...")
print(f"Job name: {args['JOB_NAME']}")
print(f"Output bucket: {output_bucket}")

try:
    # Get database credentials from Secrets Manager
    print("Retrieving database credentials from Secrets Manager...")
    secrets_client = boto3.client('secretsmanager')
    secret_response = secrets_client.get_secret_value(SecretId=database_secret_arn)
    credentials = json.loads(secret_response['SecretString'])
    print("Database credentials retrieved successfully")

    # JDBC connection properties
    jdbc_url = "jdbc:mysql://REPLACE_WITH_EC2_IP:3306/sampledb"
    connection_properties = {
        "user": credentials['username'],
        "password": credentials['password'],
        "driver": "com.mysql.cj.jdbc.Driver"
    }

    print(f"Connecting to database at: {jdbc_url}")
    print(f"Using username: {credentials['username']}")

    # Test connection and read data from MySQL using JDBC
    print("Reading data from MySQL employees table...")
    employees_df = spark.read \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", "employees") \
        .option("user", connection_properties["user"]) \
        .option("password", connection_properties["password"]) \
        .option("driver", connection_properties["driver"]) \
        .option("fetchsize", "1000") \
        .load()
    
    record_count = employees_df.count()
    print(f"Successfully read {record_count} records from MySQL")
    
    if record_count == 0:
        print("⚠️ Warning: No records found in employees table")
        job.commit()
        sys.exit(0)
    
    
    # Show sample data
    print("Sample data from MySQL:")
    employees_df.show(5, truncate=False)
    
    # Print schema
    print("Data schema:")
    employees_df.printSchema()
    
    # Convert to Glue DynamicFrame for better schema handling
    employees_dynamic_frame = DynamicFrame.fromDF(employees_df, glueContext, "employees")
    
    # Write data to S3 in Parquet format
    output_path = f"s3://{output_bucket}/mysql-data/employees/"
    
    print(f"Writing data to S3: {output_path}")
    
    glueContext.write_dynamic_frame.from_options(
        frame=employees_dynamic_frame,
        connection_type="s3",
        connection_options={
            "path": output_path,
            "partitionKeys": ["department"]  # Partition by department
        },
        format="parquet",
        transformation_ctx="write_to_s3"
    )
    
    print("Employee data successfully written to S3")
    
    # Create a summary report
    print("Creating summary report...")
    summary_df = employees_df.groupBy("department").agg(
        {"salary": "avg", "id": "count"}
    ).withColumnRenamed("avg(salary)", "avg_salary") \
     .withColumnRenamed("count(id)", "employee_count")
    
    print("Summary data:")
    summary_df.show()
    
    summary_dynamic_frame = DynamicFrame.fromDF(summary_df, glueContext, "summary")
    
    summary_path = f"s3://{output_bucket}/mysql-data/summary/"
    
    glueContext.write_dynamic_frame.from_options(
        frame=summary_dynamic_frame,
        connection_type="s3",
        connection_options={"path": summary_path},
        format="parquet",
        transformation_ctx="write_summary_to_s3"
    )
    
    print("Summary report written to S3")
    
    # Write job completion status
    completion_info = {
        "job_name": args['JOB_NAME'],
        "completion_time": time.strftime("%Y-%m-%d %H:%M:%S"),
        "records_processed": record_count,
        "output_location": output_path,
        "summary_location": summary_path
    }
    
    status_path = f"s3://{output_bucket}/mysql-data/_job_status/"
    status_df = spark.createDataFrame([completion_info])
    status_dynamic_frame = DynamicFrame.fromDF(status_df, glueContext, "status")
    
    glueContext.write_dynamic_frame.from_options(
        frame=status_dynamic_frame,
        connection_type="s3",
        connection_options={"path": status_path},
        format="json",
        transformation_ctx="write_status_to_s3"
    )
    
    print("Job completed successfully!")
    print(f"Total records processed: {record_count}")
    print(f"Data location: {output_path}")
    print(f"Summary location: {summary_path}")

except Exception as e:
    print(f"❌ Error occurred: {str(e)}")
    print(f"Error type: {type(e).__name__}")
    import traceback
    print("Full traceback:")
    traceback.print_exc()
    raise e

finally:
    print("Committing job...")
    job.commit()
    