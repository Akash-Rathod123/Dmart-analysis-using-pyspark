from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, avg, countDistinct

# Initialize SparkSession with local mode
spark = SparkSession.builder.master("local[*]").appName("DmartSalesAnalysis").getOrCreate()

# Load the integrated dataset (ensure you provide the correct local path)
df = spark.read.csv("file:///Users/akashrathod/Documents/Sixth_project/integrated_dataset.csv", header=True, inferSchema=True)

# 1. Total sales for each product category
category_sales = df.groupBy("Category").agg(sum("Sales").alias("Total Sales"))
category_sales.show()

# 2. Customer with the highest number of purchases
customer_purchase_count = df.groupBy("Customer Name").agg(countDistinct("Order ID").alias("Number of Purchases"))
max_purchase_customer = customer_purchase_count.orderBy("Number of Purchases", ascending=False).limit(1)
max_purchase_customer.show()

# 3. Average discount across all products
avg_discount = df.agg(avg("Discount").alias("Average Discount"))
avg_discount.show()

# 4. Unique products in each region
unique_products_by_region = df.groupBy("Region").agg(countDistinct("Product Name").alias("Unique Products Sold"))
unique_products_by_region.show()

# 5. Total profit generated in each state
profit_by_state = df.groupBy("State").agg(sum("Profit").alias("Total Profit"))
profit_by_state.show()

# 6. Product sub-category with the highest sales
highest_sales_subcategory = df.groupBy("Sub-Category").agg(sum("Sales").alias("Total Sales"))
highest_sales_subcategory = highest_sales_subcategory.orderBy("Total Sales", ascending=False).limit(1)
highest_sales_subcategory.show()

# 7. Average age of customers in each segment
avg_age_by_segment = df.groupBy("Segment").agg(avg("Age").alias("Average Age"))
avg_age_by_segment.show()

# 8. Number of orders shipped by each shipping mode
orders_by_shipping_mode = df.groupBy("Ship Mode").agg(countDistinct("Order ID").alias("Number of Orders"))
orders_by_shipping_mode.show()

# 9. Total quantity of products sold in each city
quantity_by_city = df.groupBy("City").agg(sum("Quantity").alias("Total Quantity Sold"))
quantity_by_city.show()

# 10. Customer segment with the highest profit margin
profit_margin_by_segment = df.groupBy("Segment").agg(sum("Profit").alias("Total Profit"), sum("Sales").alias("Total Sales"))
profit_margin_by_segment = profit_margin_by_segment.withColumn("Profit Margin", profit_margin_by_segment["Total Profit"] / profit_margin_by_segment["Total Sales"])
highest_profit_margin_segment = profit_margin_by_segment.orderBy("Profit Margin", ascending=False).limit(1)
highest_profit_margin_segment.show()
