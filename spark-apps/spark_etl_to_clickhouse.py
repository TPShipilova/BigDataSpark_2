import pyspark.sql
from pyspark.sql.functions import sum, count, avg, desc, col, month, year


def init_spark():
    return pyspark.sql.SparkSession.builder \
        .appName("PostgreSQL to ClickHouse ETL") \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.5.0,com.clickhouse:clickhouse-jdbc:0.4.6") \
        .getOrCreate()


def read_postgres_tables(spark):
    jdbc_url = "jdbc:postgresql://postgres:5432/sales_db"
    connection_properties = {
        "user": "postgres",
        "password": "postgres",
        "driver": "org.postgresql.Driver"
    }

    return {
        "dim_customer": spark.read.jdbc(url=jdbc_url, table="dim_customer", properties=connection_properties),
        "dim_product": spark.read.jdbc(url=jdbc_url, table="dim_product", properties=connection_properties),
        "dim_store": spark.read.jdbc(url=jdbc_url, table="dim_store", properties=connection_properties),
        "dim_supplier": spark.read.jdbc(url=jdbc_url, table="dim_supplier", properties=connection_properties),
        "dim_date": spark.read.jdbc(url=jdbc_url, table="dim_date", properties=connection_properties),
        "fact_sales": spark.read.jdbc(url=jdbc_url, table="fact_sales", properties=connection_properties)
    }


def prepare_dashboards(tables):
    fact = tables["fact_sales"].alias("fact")
    products = tables["dim_product"].alias("prod")
    customers = tables["dim_customer"].alias("cust")
    stores = tables["dim_store"].alias("store")
    suppliers = tables["dim_supplier"].alias("supp")
    dates = tables["dim_date"].alias("date")

    product_sales = fact.join(products, fact.sale_product_id == products.product_id) \
        .groupBy(products["product_id"], products["product_name"], products["product_category"]) \
        .agg(
        sum(fact["sale_total_price"]).alias("total_revenue"),
        sum(fact["sale_quantity"]).alias("total_sales"),
        avg(products["product_rating"]).alias("avg_rating"),
        sum(products["product_reviews"]).alias("total_reviews")
    )

    dashboards = {
        "product_sales": product_sales,
        "top_products": product_sales.orderBy(desc("total_sales")).limit(10),

        "customer_sales": fact.join(customers, fact.sale_customer_id == customers.customer_id) \
            .groupBy(customers["customer_id"], customers["customer_first_name"],
                     customers["customer_last_name"], customers["customer_country"]) \
            .agg(
            sum(fact["sale_total_price"]).alias("total_spent"),
            count(fact["sale_id"]).alias("purchase_count"),
            avg(fact["sale_total_price"]).alias("avg_order_value")
        ),

        "top_customers": fact.join(customers, fact.sale_customer_id == customers.customer_id) \
            .groupBy(customers["customer_id"], customers["customer_first_name"],
                     customers["customer_last_name"], customers["customer_country"]) \
            .agg(sum(fact["sale_total_price"]).alias("total_spent")) \
            .orderBy(desc("total_spent")).limit(10),

        "time_sales": fact.join(dates, fact.sale_date == dates.date) \
            .groupBy(dates["year"], dates["month"]) \
            .agg(
            sum(fact["sale_total_price"]).alias("monthly_revenue"),
            avg(fact["sale_total_price"]).alias("avg_order_size"),
            count(fact["sale_id"]).alias("order_count")
        ),

        "store_sales": fact.join(stores, fact.store_name == stores.store_id) \
            .groupBy(stores["store_id"], stores["store_city"], stores["store_country"]) \
            .agg(
            sum(fact["sale_total_price"]).alias("total_revenue"),
            count(fact["sale_id"]).alias("sales_count"),
            avg(fact["sale_total_price"]).alias("avg_order_value")
        ),

        "top_stores": fact.join(stores, fact.store_name == stores.store_id) \
            .groupBy(stores["store_id"], stores["store_city"], stores["store_country"]) \
            .agg(sum(fact["sale_total_price"]).alias("total_revenue")) \
            .orderBy(desc("total_revenue")).limit(5),

        "supplier_sales": fact.join(suppliers, fact.supplier_name == suppliers.supplier_id) \
            .join(products, fact.sale_product_id == products.product_id) \
            .groupBy(suppliers["supplier_id"], suppliers["supplier_country"]) \
            .agg(
            sum(fact["sale_total_price"]).alias("total_revenue"),
            avg(products["product_price"]).alias("avg_product_price"),
            count(fact["sale_id"]).alias("sales_count")
        ),

        "top_suppliers": fact.join(suppliers, fact.supplier_name == suppliers.supplier_id) \
            .groupBy(suppliers["supplier_id"], suppliers["supplier_country"]) \
            .agg(sum(fact["sale_total_price"]).alias("total_revenue")) \
            .orderBy(desc("total_revenue")).limit(5),

        "product_quality": products.join(fact, products.product_id == fact.sale_product_id, "left") \
            .groupBy(products["product_id"], products["product_name"], products["product_category"]) \
            .agg(
            avg(products["product_rating"]).alias("avg_rating"),
            sum(products["product_reviews"]).alias("total_reviews"),
            sum(fact["sale_quantity"]).alias("total_sales")
        )
    }

    return dashboards


def write_to_clickhouse(dashboards):
    clickhouse_url = "jdbc:ch://clickhouse:8123/big_data_2" 
    clickhouse_properties = {
        "user": "admin",
        "password": "admin",
        "driver": "com.clickhouse.jdbc.ClickHouseDriver",
        "socket_timeout": "300000",
        "batchsize": "100000",
        "numPartitions": "2"
    }

    mappings = {
        "product_sales": "product_sales_dashboard",
        "top_products": "top_products",
        "customer_sales": "customer_sales_dashboard",
        "top_customers": "top_customers",
        "time_sales": "time_sales_dashboard",
        "store_sales": "store_sales_dashboard",
        "top_stores": "top_stores",
        "supplier_sales": "supplier_sales_dashboard",
        "top_suppliers": "top_suppliers",
        "product_quality": "product_quality_dashboard"
    }

    for dashboard_name, table_name in mappings.items():
        try:
            dashboards[dashboard_name].write.jdbc(
                url=clickhouse_url,
                table=table_name,
                mode="append",
                properties=clickhouse_properties
            )
            print(f"Successfully wrote {dashboard_name} to {table_name}")
        except Exception as e:
            print(f"Error writing {dashboard_name} to {table_name}: {str(e)}")


def main():
    spark = init_spark()
    try:
        tables = read_postgres_tables(spark)
        dashboards = prepare_dashboards(tables)
        write_to_clickhouse(dashboards)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
