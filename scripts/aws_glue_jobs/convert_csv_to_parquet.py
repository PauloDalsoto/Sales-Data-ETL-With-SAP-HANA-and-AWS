import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col

# Initialize Glue context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init('convert_csv_to_parquet')

# List of CSV files and their paths in the S3 bucket
tables = {
    "categories": "s3://sap-hana-analytics-on-quicksight/raw-data/categories.csv",
    "customers": "s3://sap-hana-analytics-on-quicksight/raw-data/customers.csv",
    "employees": "s3://sap-hana-analytics-on-quicksight/raw-data/employees.csv",
    "orderdetails": "s3://sap-hana-analytics-on-quicksight/raw-data/orderdetails.csv",
    "orders": "s3://sap-hana-analytics-on-quicksight/raw-data/orders.csv",
    "products": "s3://sap-hana-analytics-on-quicksight/raw-data/products.csv",
    "shippers": "s3://sap-hana-analytics-on-quicksight/raw-data/shippers.csv",
    "suppliers": "s3://sap-hana-analytics-on-quicksight/raw-data/suppliers.csv"
}

# Loop to process each table
for table_name, s3_path in tables.items():
    # Read each CSV file individually
    datasource = glueContext.create_dynamic_frame.from_options(
        format_options={"withHeader": True},
        connection_type="s3",
        format="csv",
        connection_options={"paths": [s3_path]},
        transformation_ctx=f"datasource_{table_name}"
    )

    # Convert to a Spark DataFrame
    df = datasource.toDF()

    # Data type conversion as necessary
    if table_name == "categories":
        df = df.withColumn("CategoryID", col("CategoryID").cast("smallint"))
        df = df.withColumn("CategoryName", col("CategoryName").cast("string"))
        df = df.withColumn("Description", col("Description").cast("string"))
    elif table_name == "customers":
        df = df.withColumn("CustomerID", col("CustomerID").cast("string"))
        df = df.withColumn("CompanyName", col("CompanyName").cast("string"))
        df = df.withColumn("ContactName", col("ContactName").cast("string"))
        df = df.withColumn("ContactTitle", col("ContactTitle").cast("string"))
        df = df.withColumn("Address", col("Address").cast("string"))
        df = df.withColumn("City", col("City").cast("string"))
        df = df.withColumn("Region", col("Region").cast("string"))
        df = df.withColumn("PostalCode", col("PostalCode").cast("string"))
        df = df.withColumn("Country", col("Country").cast("string"))
        df = df.withColumn("Phone", col("Phone").cast("string"))
    elif table_name == "employees":
        df = df.withColumn("EmployeeID", col("EmployeeID").cast("smallint"))
        df = df.withColumn("LastName", col("LastName").cast("string"))
        df = df.withColumn("FirstName", col("FirstName").cast("string"))
        df = df.withColumn("BirthDate", col("BirthDate").cast("date"))
        df = df.withColumn("HireDate", col("HireDate").cast("date"))
    elif table_name == "orderdetails":
        df = df.withColumn("OrderID", col("OrderID").cast("smallint"))
        df = df.withColumn("ProductID", col("ProductID").cast("smallint"))
        df = df.withColumn("Quantity", col("Quantity").cast("smallint"))
        df = df.withColumn("UnitPrice", col("UnitPrice").cast("float"))
        df = df.withColumn("Discount", col("Discount").cast("float"))
    elif table_name == "orders":
        df = df.withColumn("OrderID", col("OrderID").cast("smallint"))
        df = df.withColumn("CustomerID", col("CustomerID").cast("string"))
        df = df.withColumn("OrderDate", col("OrderDate").cast("date"))
        df = df.withColumn("RequiredDate", col("RequiredDate").cast("date"))
        df = df.withColumn("ShippedDate", col("ShippedDate").cast("date"))
    elif table_name == "products":
        df = df.withColumn("ProductID", col("ProductID").cast("smallint"))
        df = df.withColumn("ProductName", col("ProductName").cast("string"))
        df = df.withColumn("SupplierID", col("SupplierID").cast("smallint"))
        df = df.withColumn("CategoryID", col("CategoryID").cast("smallint"))
        df = df.withColumn("QuantityPerUnit", col("QuantityPerUnit").cast("string"))
        df = df.withColumn("UnitPrice", col("UnitPrice").cast("float"))
        df = df.withColumn("UnitsInStock", col("UnitsInStock").cast("smallint"))
        df = df.withColumn("ReorderLevel", col("ReorderLevel").cast("smallint"))
        df = df.withColumn("Discontinued", col("Discontinued").cast("integer"))
    elif table_name == "shippers":
        df = df.withColumn("ShipperID", col("ShipperID").cast("smallint"))
        df = df.withColumn("CompanyName", col("CompanyName").cast("string"))
        df = df.withColumn("Phone", col("Phone").cast("string"))
    elif table_name == "suppliers":
        df = df.withColumn("SupplierID", col("SupplierID").cast("smallint"))
        df = df.withColumn("CompanyName", col("CompanyName").cast("string"))
        df = df.withColumn("ContactName", col("ContactName").cast("string"))
        df = df.withColumn("ContactTitle", col("ContactTitle").cast("string"))
        df = df.withColumn("Address", col("Address").cast("string"))
        df = df.withColumn("City", col("City").cast("string"))
        df = df.withColumn("Region", col("Region").cast("string"))
        df = df.withColumn("PostalCode", col("PostalCode").cast("string"))
        df = df.withColumn("Country", col("Country").cast("string"))
        df = df.withColumn("Phone", col("Phone").cast("string"))

    # Write to Parquet format in a separate folder for each table
    df.write.mode("overwrite").parquet(f"s3://sap-hana-analytics-on-quicksight/output-parquet/{table_name}/")

# Commit the job
job.commit()
