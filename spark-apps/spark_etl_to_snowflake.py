import pyspark.sql
from pyspark.sql.functions import col, year, month, dayofmonth, quarter, dayofweek

spark = pyspark.sql.SparkSession.builder \
    .appName("PostgreSQL to Snowflake ETL") \
    .getOrCreate()

jdbc_url = "jdbc:postgresql://postgres:5432/sales_db"
connection_properties = {
    "user": "postgres",
    "password": "postgres",
    "driver": "org.postgresql.Driver"
}

df = spark.read.jdbc(url=jdbc_url, table="mock_data", properties=connection_properties)

dim_customer = df.select(
    col("sale_customer_id").alias("customer_id"),
    col("customer_first_name"),
    col("customer_last_name"),
    col("customer_age"),
    col("customer_email"),
    col("customer_country"),
    col("customer_postal_code"),
    col("customer_pet_type"),
    col("customer_pet_name"),
    col("customer_pet_breed")
).distinct()

dim_seller = df.select(
    col("sale_seller_id").alias("seller_id"),
    col("seller_first_name"),
    col("seller_last_name"),
    col("seller_email"),
    col("seller_country"),
    col("seller_postal_code")
).distinct()

dim_product = df.select(
    col("sale_product_id").alias("product_id"),
    col("product_name"),
    col("product_category"),
    col("product_price"),
    col("product_quantity"),
    col("pet_category"),
    col("product_weight"),
    col("product_color"),
    col("product_size"),
    col("product_brand"),
    col("product_material"),
    col("product_description"),
    col("product_rating"),
    col("product_reviews"),
    col("product_release_date"),
    col("product_expiry_date")
).distinct()

dim_store = df.select(
    col("store_name").alias("store_id"),
    col("store_location"),
    col("store_city"),
    col("store_state"),
    col("store_country"),
    col("store_phone"),
    col("store_email")
).distinct()

dim_supplier = df.select(
    col("supplier_name").alias("supplier_id"),
    col("supplier_contact"),
    col("supplier_email"),
    col("supplier_phone"),
    col("supplier_address"),
    col("supplier_city"),
    col("supplier_country")
).distinct()

dim_date = df.select(
    col("sale_date").alias("date"),
    col("sale_date").cast("string").alias("date_string"),
    year(col("sale_date")).alias("year"),
    month(col("sale_date")).alias("month"),
    dayofmonth(col("sale_date")).alias("day"),
    quarter(col("sale_date")).alias("quarter"),
    dayofweek(col("sale_date")).alias("day_of_week")
).distinct()

fact_sales = df.select(
    col("id").alias("sale_id"),
    col("sale_date"),
    col("sale_customer_id"),
    col("sale_seller_id"),
    col("sale_product_id"),
    col("store_name"),
    col("supplier_name"),
    col("sale_quantity"),
    col("sale_total_price"),
    col("product_rating"),
    col("product_reviews")
)

dim_customer.write.jdbc(
    url=jdbc_url,
    table="dim_customer",
    mode="overwrite",
    properties=connection_properties
)

dim_seller.write.jdbc(
    url=jdbc_url,
    table="dim_seller",
    mode="overwrite",
    properties=connection_properties
)

dim_product.write.jdbc(
    url=jdbc_url,
    table="dim_product",
    mode="overwrite",
    properties=connection_properties
)

dim_store.write.jdbc(
    url=jdbc_url,
    table="dim_store",
    mode="overwrite",
    properties=connection_properties
)

dim_supplier.write.jdbc(
    url=jdbc_url,
    table="dim_supplier",
    mode="overwrite",
    properties=connection_properties
)

dim_date.write.jdbc(
    url=jdbc_url,
    table="dim_date",
    mode="overwrite",
    properties=connection_properties
)

fact_sales.write.jdbc(
    url=jdbc_url,
    table="fact_sales",
    mode="overwrite",
    properties=connection_properties
)

spark.stop()
