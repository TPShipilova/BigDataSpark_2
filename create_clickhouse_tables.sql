-- 1. Витрина продаж по продуктам
CREATE TABLE IF NOT EXISTS product_sales_dashboard (
    product_id Int32,
    product_name String,
    product_category String,
    total_revenue Float64,
    total_sales Int32,
    avg_rating Float64,
    total_reviews Int32,
    etl_time DateTime DEFAULT now()
) ENGINE = Log();

-- 2. Топ продуктов
CREATE TABLE IF NOT EXISTS top_products (
    product_id Int32,
    product_name String,
    product_category String,
    total_revenue Float64,
    total_sales Int32,
    avg_rating Float64,
    total_reviews Int32,
    rank UInt32
) ENGINE = Log();

-- 3. Витрина продаж по клиентам
CREATE TABLE IF NOT EXISTS customer_sales_dashboard (
    customer_id Int32,
    customer_first_name String,
    customer_last_name String,
    customer_country String,
    total_spent Float64,
    purchase_count Int32,
    avg_order_value Float64
) ENGINE = Log();

-- 4. Топ клиентов
CREATE TABLE IF NOT EXISTS top_customers (
    customer_id Int32,
    customer_first_name String,
    customer_last_name String,
    customer_country String,
    total_spent Float64,
    purchase_count Int32,
    avg_order_value Float64,
    rank UInt32
) ENGINE = Log();

-- 5. Витрина продаж по времени
CREATE TABLE IF NOT EXISTS time_sales_dashboard (
    year Int32,
    month Int32,
    monthly_revenue Float64,
    avg_order_size Float64,
    order_count Int32
) ENGINE = Log();

-- 6. Витрина продаж по магазинам
CREATE TABLE IF NOT EXISTS store_sales_dashboard (
    store_id Int32,
    store_city String,
    store_country String,
    total_revenue Float64,
    sales_count Int32,
    avg_order_value Float64
) ENGINE = Log();

-- 7. Топ магазинов
CREATE TABLE IF NOT EXISTS top_stores (
    store_id Int32,
    store_city String,
    store_country String,
    total_revenue Float64,
    sales_count Int32,
    avg_order_value Float64,
    rank UInt32
) ENGINE = Log();

-- 8. Витрина продаж по поставщикам
CREATE TABLE IF NOT EXISTS supplier_sales_dashboard (
    supplier_id Int32,
    supplier_country String,
    total_revenue Float64,
    avg_product_price Float64,
    sales_count Int32
) ENGINE = Log();

-- 9. Топ поставщиков
CREATE TABLE IF NOT EXISTS top_suppliers (
    supplier_id Int32,
    supplier_country String,
    total_revenue Float64,
    avg_product_price Float64,
    sales_count Int32,
    rank UInt32
) ENGINE = Log();

-- 10. Витрина качества продукции
CREATE TABLE IF NOT EXISTS product_quality_dashboard (
    product_id Int32,
    product_name String,
    product_category String,
    avg_rating Float64,
    total_reviews Int32,
    total_sales Int32
) ENGINE = Log();