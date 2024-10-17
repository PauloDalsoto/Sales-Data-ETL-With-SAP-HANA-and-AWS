import sys
import json
import pandas as pd
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from pyspark.sql import SparkSession
from awsglue.job import Job
from hdbcli import dbapi

# Input parameters
output_s3_path = "s3://sap-hana-analytics-on-quicksight/raw-data/"

# Create SparkSession and GlueContext
spark = SparkSession.builder.appName('GlueJob').getOrCreate()
glueContext = GlueContext(spark)

# Create the Job object
job = Job(glueContext)
job.init('sap-hana-data-extraction')

# Load SAP HANA configuration from S3
# Define connection variables
db_user = 'DBADMIN'  
db_pwd = 'XXXXX'  
db_url = 'XXXXX.hana.trial-us10.hanacloud.ondemand.com'
db_port = 999

try:
    # Connect to SAP HANA using hdbcli
    connection = dbapi.connect(
        address=db_url,
        port=db_port,
        user=db_user,
        password=db_pwd
    )

    cursor = connection.cursor()
    
    # List of tables to iterate over
    tables = ["Categories", "Suppliers", "Shippers", "Employees", "Customers", "Products", "Orders", "OrderDetails"]

    # Loop through the list of tables
    for table in tables:
        try:
            # Execute the SELECT query on the current table
            query = f'SELECT * FROM "{table}"'
            cursor.execute(query)
            rows = cursor.fetchall()

            # Convert the data into a Pandas DataFrame
            columns = [desc[0] for desc in cursor.description]
            pandas_df = pd.DataFrame(rows, columns=columns)

            # Save the DataFrame as a CSV file in S3
            output_file_path = output_s3_path + f"{table.lower()}.csv" 
            pandas_df.to_csv(output_file_path, index=False)

            print(f'Data successfully saved to {output_file_path}')

        except Exception as e:
            print(f'Error querying table {table}: {e}')

except Exception as e:
    print(f'Error connecting or querying: {e}')

finally:
    # Close the connection
    if 'connection' in locals():
        connection.close()

job.commit()
