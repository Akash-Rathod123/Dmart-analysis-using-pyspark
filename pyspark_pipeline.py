import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Task 1: Establish PySpark Connection
spark = SparkSession.builder \
    .appName("Dmart Sales Data Analysis") \
    .config("spark.hadoop.fs.defaultFS", "file:///") \
    .getOrCreate()

# Verify if file exists
file_path = "/Users/akashrathod/Downloads/product.csv"
sales_file_path = "/Users/akashrathod/Downloads/sales.csv"
customers_file_path = "/Users/akashrathod/Downloads/customer.csv"

# Checking if the required files exist
if not os.path.exists(file_path) or not os.path.exists(sales_file_path) or not os.path.exists(customers_file_path):
    raise FileNotFoundError("One or more files are missing. Please check the file paths.")

# Task 2: Load Data into PySpark DataFrames
products_df = spark.read.csv(file_path, header=True, inferSchema=True)
sales_df = spark.read.csv(sales_file_path, header=True, inferSchema=True)
customers_df = spark.read.csv(customers_file_path, header=True, inferSchema=True)

# Task 3: Data Transformation and Cleaning
# Renaming columns for consistency
products_df = products_df.withColumnRenamed("Product ID", "product_id")
sales_df = sales_df.withColumnRenamed("Product ID", "product_id").withColumnRenamed("Customer ID", "customer_id")
customers_df = customers_df.withColumnRenamed("Customer ID", "customer_id")

# Handle missing data by filtering rows with NULL values in key columns
products_df = products_df.filter(products_df["product_id"].isNotNull())
customers_df = customers_df.filter(customers_df["customer_id"].isNotNull())
sales_df = sales_df.filter(sales_df["product_id"].isNotNull() & sales_df["customer_id"].isNotNull())

# Handle missing data (fillna for other columns)
sales_df = sales_df.fillna({"Quantity": 0, "Sales": 0.0})
customers_df = customers_df.fillna({"Customer Name": "Unknown"})

# Casting columns to the appropriate data types
products_df = products_df.withColumn("product_id", col("product_id").cast("string"))
sales_df = sales_df.withColumn("Sales", col("Sales").cast("double"))
customers_df = customers_df.withColumn("customer_id", col("customer_id").cast("string"))

# Task 4: Join the DataFrames
# Performing inner join between sales, products, and customers DataFrames
sales_products_df = sales_df.join(products_df, "product_id", "inner")
final_df = sales_products_df.join(customers_df, "customer_id", "inner")

# Show the final transformed data
final_df.show()

# Optionally, you can display the count of rows to check if the data looks good
print(f"Final DataFrame count: {final_df.count()}")

# Saving the final DataFrame as a CSV file
final_df.write \
    .option("header", "true") \
    .csv("/Users/akashrathod/Documents/Sixth_project/integrated_dataset.csv")

