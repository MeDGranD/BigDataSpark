from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, count, avg, round, first, expr, year, month, date_format

spark = (
    SparkSession
    .builder
    .appName("ReportsToClickHouse")
    .config(
        "spark.jars",
        "/opt/spark/jars/postgresql-42.6.0.jar,"
        "/opt/spark/jars/clickhouse-jdbc-0.4.6.jar"
    )
    .getOrCreate()
)

pg_jdbc_url = "jdbc:postgresql://postgres:5432/spark_db"
pg_db_properties = {
    "user": "spark_user",
    "password": "spark_password",
    "driver": "org.postgresql.Driver"
}

ch_jdbc_url = "jdbc:clickhouse://clickhouse:8123/default" 
ch_db_properties = {
    "driver": "com.clickhouse.jdbc.ClickHouseDriver"
}

def read_from_pg(table_name):
    return spark.read.jdbc(url=pg_jdbc_url, table=table_name, properties=pg_db_properties)

def write_to_ch(df, table_name):
    df.write \
      .format("jdbc") \
      .option("url", ch_jdbc_url) \
      .option("dbtable", table_name) \
      .option("driver", ch_db_properties["driver"]) \
      .option("createTableOptions", "ENGINE = Log") \
      .mode("overwrite") \
      .save()

#---------------------------------------------------------------------------

fact_sales_df = read_from_pg("fact_sales")
dim_product_df = read_from_pg("dim_product").alias("dp")
dim_customer_df = read_from_pg("dim_customer").alias("dc")
dim_date_df = read_from_pg("dim_date").alias("dd")
dim_store_df = read_from_pg("dim_store").alias("ds")
dim_supplier_df = read_from_pg("dim_supplier").alias("dsu")
dim_product_category_df = read_from_pg("dim_product_category").alias("dpc")
dim_brand_df = read_from_pg("dim_brand").alias("db")
dim_country_df = read_from_pg("dim_country").alias("dco")
dim_city_df = read_from_pg("dim_city").alias("dci")

# --- 1. Витрина продаж по продуктам ---
product_sales_joined_df = fact_sales_df \
    .join(dim_product_df, fact_sales_df.product_id == col("dp.product_id")) \
    .join(dim_product_category_df, col("dp.category_id") == col("dpc.category_id"), "left") \
    .join(dim_brand_df, col("dp.brand_id") == col("db.brand_id"), "left")

product_sales_agg_df = product_sales_joined_df.groupBy(
    col("dp.product_id"),
    col("dp.product_name"),
    col("dpc.category_name"),
    col("db.brand_name")
).agg(
    sum("sale_total_price").alias("total_revenue"),
    sum("sale_quantity").alias("total_quantity_sold"),
    count("*").alias("number_of_sales"),
    first(col("dp.product_avg_rating")).alias("avg_rating"),
    first(col("dp.product_reviews_count")).alias("reviews_count")
).withColumn("report_date", expr("current_date()"))

write_to_ch(product_sales_agg_df, "product_sales_view")

# --- 2. Витрина продаж по клиентам ---
customer_sales_joined_df = fact_sales_df \
    .join(dim_customer_df, fact_sales_df.customer_id == col("dc.customer_id")) \
    .join(dim_country_df, col("dc.country_id") == col("dco.country_id"), "left")

customer_sales_agg_df = customer_sales_joined_df.groupBy(
    col("dc.customer_id"),
    col("dc.customer_first_name"),
    col("dc.customer_last_name"),
    col("dc.customer_email"),
    col("dco.country_name")
).agg(
    sum("sale_total_price").alias("total_purchase_amount"),
    count("*").alias("number_of_orders")
).withColumn("avg_order_value", col("total_purchase_amount") / col("number_of_orders")) \
 .withColumn("report_date", expr("current_date()"))

write_to_ch(customer_sales_agg_df, "customer_sales_view")

# --- 3. Витрина продаж по времени ---
time_sales_joined_df = fact_sales_df.join(dim_date_df, fact_sales_df.date_id == col("dd.date_id"))

time_sales_agg_df = time_sales_joined_df.groupBy(
    col("dd.year").alias("sale_year"),
    col("dd.month").alias("sale_month"),
    col("dd.month_name").alias("sale_month_name")
).agg(
    sum("sale_total_price").alias("total_revenue"),
    sum("sale_quantity").alias("total_quantity_sold"),
    count("*").alias("number_of_orders")
).withColumn("avg_order_value", col("total_revenue") / col("number_of_orders")) \
 .withColumn("report_date", expr("current_date()"))

write_to_ch(time_sales_agg_df, "time_sales_view")

# --- 4. Витрина продаж по магазинам ---
store_sales_joined_df = fact_sales_df \
    .join(dim_store_df, fact_sales_df.store_id == col("ds.store_id")) \
    .join(dim_city_df, col("ds.city_id") == col("dci.city_id"), "left") \
    .join(dim_country_df, col("ds.country_id") == col("dco.country_id"), "left")

store_sales_agg_df = store_sales_joined_df.groupBy(
    col("ds.store_id"),
    col("ds.store_name"),
    col("dci.city_name"),
    col("dco.country_name")
).agg(
    sum("sale_total_price").alias("total_revenue"),
    count("*").alias("number_of_orders")
).withColumn("avg_order_value", col("total_revenue") / col("number_of_orders")) \
 .withColumn("report_date", expr("current_date()"))

write_to_ch(store_sales_agg_df, "store_sales_view")

# --- 5. Витрина продаж по поставщикам ---
supplier_sales_joined_df = fact_sales_df \
    .join(dim_product_df, fact_sales_df.product_id == col("dp.product_id")) \
    .join(dim_supplier_df, col("dp.supplier_id") == col("dsu.supplier_id")) \
    .join(dim_country_df, col("dsu.country_id") == col("dco.country_id"), "left")

supplier_sales_agg_df = supplier_sales_joined_df.groupBy(
    col("dsu.supplier_id"),
    col("dsu.supplier_name"),
    col("dco.country_name")
).agg(
    sum("sale_total_price").alias("total_revenue_from_supplier_products"),
    avg("dp.product_current_price").alias("avg_product_price_from_supplier"),
    count("*").alias("number_of_sales_from_supplier_products")
).withColumn("report_date", expr("current_date()"))

write_to_ch(supplier_sales_agg_df, "supplier_sales_view")

# --- 6. Витрина качества продукции ---
product_quality_joined_df = fact_sales_df \
    .join(dim_product_df, fact_sales_df.product_id == col("dp.product_id")) \
    .join(dim_product_category_df, col("dp.category_id") == col("dpc.category_id"), "left") \
    .join(dim_brand_df, col("dp.brand_id") == col("db.brand_id"), "left")

product_quality_agg_df = product_quality_joined_df.groupBy(
    col("dp.product_id"),
    col("dp.product_name"),
    col("dpc.category_name"),
    col("db.brand_name")
).agg(
    first(col("dp.product_avg_rating")).alias("avg_rating"),
    first(col("dp.product_reviews_count")).alias("reviews_count"),
    sum("sale_quantity").alias("total_quantity_sold")
).withColumn("report_date", expr("current_date()"))

write_to_ch(product_quality_agg_df, "product_quality_view")


spark.stop()