import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, avg, countDistinct

def create_spark_session():
    """
    Create and return a SparkSession.
    """
    return SparkSession.builder \
        .appName("Dmart Sales Data Analysis") \
        .config("spark.hadoop.fs.defaultFS", "file:///") \
        .getOrCreate()

def load_data(spark, file_path):
    """
    Load CSV data into a PySpark DataFrame.
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File not found: {file_path}")
    return spark.read.csv(file_path, header=True, inferSchema=True)

def clean_data(products_df, sales_df, customers_df):
    """
    Perform data cleaning and transformation.
    """
    products_df = products_df.withColumnRenamed("Product ID", "product_id")
    sales_df = sales_df.withColumnRenamed("Product ID", "product_id").withColumnRenamed("Customer ID", "customer_id")
    customers_df = customers_df.withColumnRenamed("Customer ID", "customer_id")

    # Handle missing data
    products_df = products_df.filter(products_df["product_id"].isNotNull())
    customers_df = customers_df.filter(customers_df["customer_id"].isNotNull())
    sales_df = sales_df.filter(sales_df["product_id"].isNotNull() & sales_df["customer_id"].isNotNull())

    sales_df = sales_df.fillna({"Quantity": 0, "Sales": 0.0})
    customers_df = customers_df.fillna({"Customer Name": "Unknown"})

    # Data type casting
    products_df = products_df.withColumn("product_id", col("product_id").cast("string"))
    sales_df = sales_df.withColumn("Sales", col("Sales").cast("double"))
    customers_df = customers_df.withColumn("customer_id", col("customer_id").cast("string"))

    return products_df, sales_df, customers_df

def join_data(products_df, sales_df, customers_df):
    """
    Join products, sales, and customers DataFrames.
    """
    sales_products_df = sales_df.join(products_df, "product_id", "inner")
    final_df = sales_products_df.join(customers_df, "customer_id", "inner")
    return final_df

def perform_analysis(df):
    """
    Perform various data analysis tasks.
    """
    df.groupBy("Category").agg(sum("Sales").alias("Total Sales")).show()
    df.groupBy("Customer Name").agg(countDistinct("Order ID").alias("Number of Purchases")).orderBy("Number of Purchases", ascending=False).limit(1).show()
    df.agg(avg("Discount").alias("Average Discount")).show()
    df.groupBy("Region").agg(countDistinct("Product Name").alias("Unique Products Sold")).show()
    df.groupBy("State").agg(sum("Profit").alias("Total Profit")).show()
    df.groupBy("Sub-Category").agg(sum("Sales").alias("Total Sales")).orderBy("Total Sales", ascending=False).limit(1).show()
    df.groupBy("Segment").agg(avg("Age").alias("Average Age")).show()
    df.groupBy("Ship Mode").agg(countDistinct("Order ID").alias("Number of Orders")).show()
    df.groupBy("City").agg(sum("Quantity").alias("Total Quantity Sold")).show()

def save_data(df, output_path):
    df.write.option("header", "true").mode("overwrite").csv(output_path)


def main():
    spark = create_spark_session()
    
    product_path = "/Users/akashrathod/Downloads/product.csv"
    sales_path = "/Users/akashrathod/Downloads/sales.csv"
    customer_path = "/Users/akashrathod/Downloads/customer.csv"
    output_path = "/Users/akashrathod/Documents/Sixth_project/integrated_dataset.csv"
    
    products_df = load_data(spark, product_path)
    sales_df = load_data(spark, sales_path)
    customers_df = load_data(spark, customer_path)
    
    products_df, sales_df, customers_df = clean_data(products_df, sales_df, customers_df)
    final_df = join_data(products_df, sales_df, customers_df)
    
    final_df.show()
    print(f"Final DataFrame count: {final_df.count()}")
    save_data(final_df, output_path)
    
    perform_analysis(final_df)
    
if __name__ == "__main__":
    main()
