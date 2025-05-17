from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, lit, year, month, dayofmonth, quarter, expr,
    dayofweek, weekofyear, date_format, when, coalesce,
    to_date, upper
)
from pyspark.sql.window import Window

spark = (
    SparkSession.builder.appName("SQLETLtoPySpark")
    .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0")
    .getOrCreate()
)

jdbc_url = "jdbc:postgresql://postgres:5432/spark_db"
db_properties = {
    "user": "spark_user",
    "password": "spark_password",
    "driver": "org.postgresql.Driver"
}

print("Loading raw_data from 'mock_data' table in PostgreSQL...")
try:
    raw_df = spark.read.jdbc(
        url=jdbc_url,
        table="mock_data",
        properties=db_properties
    )
    raw_df.persist()
    print(f"Successfully loaded {raw_df.count()} rows from mock_data.")
except Exception as e:
    print(f"Error loading raw_data from mock_data table: {e}")
    spark.stop()
    exit(1)

def write_to_jdbc_table(df, table_name):
    df.write.jdbc(
        url=jdbc_url,
        table=table_name,
        mode="append",
        properties=db_properties
    )
    print(f"Finished writing to {table_name}.")
    return spark.read.jdbc(url=jdbc_url, table=table_name, properties=db_properties)


# dim_product_category
print("Processing dim_product_category...")
dim_product_category_df = raw_df.select(col("product_category").alias("category_name")) \
    .filter(col("category_name").isNotNull() & (col("category_name") != "")) \
    .distinct()
dim_product_category_df = write_to_jdbc_table(dim_product_category_df, "dim_product_category")
dim_product_category_df.cache() # Cache for potential reuse

# dim_pet_category
print("Processing dim_pet_category...")
dim_pet_category_df = raw_df.select(col("pet_category").alias("pet_category_name")) \
    .filter(col("pet_category_name").isNotNull() & (col("pet_category_name") != "")) \
    .distinct()
dim_pet_category_df = write_to_jdbc_table(dim_pet_category_df, "dim_pet_category")
dim_pet_category_df.cache()

# dim_brand
print("Processing dim_brand...")
dim_brand_df = raw_df.select(col("product_brand").alias("brand_name")) \
    .filter(col("brand_name").isNotNull() & (col("brand_name") != "")) \
    .distinct()
dim_brand_df = write_to_jdbc_table(dim_brand_df, "dim_brand")
dim_brand_df.cache()

# dim_material
print("Processing dim_material...")
dim_material_df = raw_df.select(col("product_material").alias("material_name")) \
    .filter(col("material_name").isNotNull() & (col("material_name") != "")) \
    .distinct()
dim_material_df = write_to_jdbc_table(dim_material_df, "dim_material")
dim_material_df.cache()

# dim_color
print("Processing dim_color...")
dim_color_df = raw_df.select(col("product_color").alias("color_name")) \
    .filter(col("color_name").isNotNull() & (col("color_name") != "")) \
    .distinct()
dim_color_df = write_to_jdbc_table(dim_color_df, "dim_color")
dim_color_df.cache()

# dim_size
print("Processing dim_size...")
dim_size_df = raw_df.select(col("product_size").alias("size_name")) \
    .filter(col("size_name").isNotNull() & (col("size_name") != "")) \
    .distinct()
dim_size_df = write_to_jdbc_table(dim_size_df, "dim_size")
dim_size_df.cache()

# dim_pet_breed
print("Processing dim_pet_breed...")
dim_pet_breed_df = raw_df.select(col("customer_pet_breed").alias("breed_name")) \
    .filter(col("breed_name").isNotNull() & (col("breed_name") != "")) \
    .distinct()
dim_pet_breed_df = write_to_jdbc_table(dim_pet_breed_df, "dim_pet_breed")
dim_pet_breed_df.cache()

# dim_pet_type
print("Processing dim_pet_type...")
dim_pet_type_df = raw_df.select(col("customer_pet_type").alias("pet_type_name")) \
    .filter(col("pet_type_name").isNotNull() & (col("pet_type_name") != "")) \
    .distinct()
dim_pet_type_df = write_to_jdbc_table(dim_pet_type_df, "dim_pet_type")
dim_pet_type_df.cache()

dim_state_df_to_write = raw_df.select(col("store_state").alias("state_name")) \
        .filter(col("state_name").isNotNull() & (col("state_name") != "")) \
        .distinct()
dim_state_df = write_to_jdbc_table(dim_state_df_to_write, "dim_state")
dim_state_df.cache()
print(f"dim_state populated with {dim_state_df.count()} states.")

# dim_country
print("Processing dim_country...")
countries1 = raw_df.select(col("customer_country").alias("country_name"))
countries2 = raw_df.select(col("seller_country").alias("country_name"))
countries3 = raw_df.select(col("store_country").alias("country_name"))
countries4 = raw_df.select(col("supplier_country").alias("country_name"))

dim_country_df = countries1.union(countries2).union(countries3).union(countries4) \
    .filter(col("country_name").isNotNull() & (col("country_name") != "")) \
    .distinct()
dim_country_df = write_to_jdbc_table(dim_country_df, "dim_country")
dim_country_df.cache()

# dim_city
print("Processing dim_city...")
cities1 = raw_df.select(col("supplier_city").alias("city_name"))
cities2 = raw_df.select(col("store_city").alias("city_name"))

dim_city_df = cities1.union(cities2) \
    .filter(col("city_name").isNotNull() & (col("city_name") != "")) \
    .distinct()
dim_city_df = write_to_jdbc_table(dim_city_df, "dim_city")
dim_city_df.cache()

dates_df = raw_df.select(to_date(col("sale_date")).alias("datum")) \
    .filter(col("datum").isNotNull()) \
    .distinct()

dim_date_transformed_df = dates_df.select(
    col("datum").alias("full_date"),
    year(col("datum")).alias("year"),
    quarter(col("datum")).alias("quarter"),
    month(col("datum")).alias("month"),
    dayofmonth(col("datum")).alias("day"),
    dayofweek(col("datum")).alias("weekday"),
    weekofyear(col("datum")).alias("week_of_year"),
    date_format(col("datum"), "EEEE").alias("day_name"),
    date_format(col("datum"), "MMMM").alias("month_name"),
    when(dayofweek(col("datum")).isin([1, 7]), True).otherwise(False).alias("is_weekend")
)
dim_date_df = write_to_jdbc_table(dim_date_transformed_df, "dim_date")
dim_date_df.cache()

supplier_intermediate_df = raw_df.select(
    "supplier_name", "supplier_contact", "supplier_email", "supplier_phone",
    "supplier_address", "supplier_city", "supplier_country"
).filter(col("supplier_name").isNotNull() | col("supplier_email").isNotNull()).distinct()

dim_supplier_transformed_df = supplier_intermediate_df \
    .join(dim_country_df, supplier_intermediate_df.supplier_country == dim_country_df.country_name, "left") \
    .join(dim_city_df, supplier_intermediate_df.supplier_city == dim_city_df.city_name, "left") \
    .select(
        col("supplier_name"),
        col("supplier_contact").alias("supplier_contact_person"),
        col("supplier_email"),
        col("supplier_phone"),
        col("supplier_address"),
        dim_city_df.city_id,
        dim_country_df.country_id 
    )
dim_supplier_df = write_to_jdbc_table(dim_supplier_transformed_df, "dim_supplier")
dim_supplier_df.cache()

store_intermediate_df = raw_df.select(
    "store_name", "store_location", "store_city", "store_state", "store_country",
    "store_phone", "store_email"
).filter(col("store_name").isNotNull()).distinct()

dim_store_transformed_df = store_intermediate_df \
    .join(dim_country_df, store_intermediate_df.store_country == dim_country_df.country_name, "left") \
    .join(dim_city_df, store_intermediate_df.store_city == dim_city_df.city_name, "left") \
    .select(
        col("store_name"),
        col("store_location"),
        dim_city_df.city_id,
        lit(None).cast("integer").alias("state_id"),
        dim_country_df.country_id,
        col("store_phone"),
        col("store_email")
    )
dim_store_df = write_to_jdbc_table(dim_store_transformed_df, "dim_store")
dim_store_df.cache()

# dim_seller
print("Processing dim_seller...")
seller_intermediate_df = raw_df.select(
    "seller_first_name", "seller_last_name", "seller_email", "seller_country", "seller_postal_code"
).filter(col("seller_email").isNotNull()).distinct()

dim_seller_transformed_df = seller_intermediate_df \
    .join(dim_country_df, seller_intermediate_df.seller_country == dim_country_df.country_name, "left") \
    .select(
        col("seller_first_name"),
        col("seller_last_name"),
        col("seller_email"),
        dim_country_df.country_id,
        col("seller_postal_code")
    )
dim_seller_df = write_to_jdbc_table(dim_seller_transformed_df, "dim_seller")
dim_seller_df.cache()

# dim_customer
print("Processing dim_customer...")
customer_intermediate_df = raw_df.select(
    "customer_first_name", "customer_last_name", "customer_age", "customer_email",
    "customer_country", "customer_postal_code"
).filter(col("customer_email").isNotNull()).distinct()

dim_customer_transformed_df = customer_intermediate_df \
    .join(dim_country_df, customer_intermediate_df.customer_country == dim_country_df.country_name, "left") \
    .select(
        col("customer_first_name"),
        col("customer_last_name"),
        col("customer_age").cast("integer"),
        col("customer_email"),
        dim_country_df.country_id,
        col("customer_postal_code")
    )
dim_customer_df = write_to_jdbc_table(dim_customer_transformed_df, "dim_customer")
dim_customer_df.cache()

product_intermediate_df = raw_df \
    .filter(col("product_name").isNotNull() & col("product_brand").isNotNull()) \
    .select(
        "product_name", "product_category", "product_brand", "product_material",
        "product_color", "product_size", "product_price", "product_weight",
        "product_description", "product_rating", "product_reviews",
        "product_release_date", "product_expiry_date",
        "supplier_name", "supplier_email",
        "product_name", "product_brand", "product_weight", "product_color", "product_size", "product_material"
    )

dim_product_transformed_df = product_intermediate_df \
    .join(dim_product_category_df, product_intermediate_df.product_category == dim_product_category_df.category_name, "left") \
    .join(dim_brand_df, product_intermediate_df.product_brand == dim_brand_df.brand_name, "left") \
    .join(dim_material_df, product_intermediate_df.product_material == dim_material_df.material_name, "left") \
    .join(dim_color_df, product_intermediate_df.product_color == dim_color_df.color_name, "left") \
    .join(dim_size_df, product_intermediate_df.product_size == dim_size_df.size_name, "left") \
    .join(dim_supplier_df,
          (product_intermediate_df.supplier_name == dim_supplier_df.supplier_name) & \
          (product_intermediate_df.supplier_email == dim_supplier_df.supplier_email),
          "left") \
    .select(
        product_intermediate_df.product_name,
        dim_product_category_df.category_id,
        dim_brand_df.brand_id,
        dim_material_df.material_id,
        dim_color_df.color_id,
        dim_size_df.size_id,
        col("product_price").alias("product_current_price"),
        col("product_weight").alias("product_weight_gr"),
        col("product_description"),
        col("product_rating").alias("product_avg_rating"),
        col("product_reviews").alias("product_reviews_count"),
        to_date(col("product_release_date")).alias("product_release_date"),
        to_date(col("product_expiry_date")).alias("product_expiry_date"),
        dim_supplier_df.supplier_id,
        product_intermediate_df.product_brand,
        product_intermediate_df.product_weight,
        product_intermediate_df.product_color,
        product_intermediate_df.product_size,
        product_intermediate_df.product_material
    ).dropDuplicates(["product_name", "product_brand", "product_weight", "product_color", "product_size", "product_material"]) \
    .select(
        "product_name", "category_id", "brand_id", "material_id", "color_id", "size_id",
        "product_current_price", "product_weight_gr", "product_description", "product_avg_rating",
        "product_reviews_count", "product_release_date", "product_expiry_date", "supplier_id"
    )

dim_product_df = write_to_jdbc_table(dim_product_transformed_df, "dim_product")
dim_product_df.cache()

pet_intermediate_df = raw_df.select(
    "customer_pet_name", "customer_pet_type", "customer_pet_breed"
).filter(col("customer_pet_name").isNotNull()).distinct()

dim_pet_transformed_df = pet_intermediate_df \
    .join(dim_pet_type_df, pet_intermediate_df.customer_pet_type == dim_pet_type_df.pet_type_name, "left") \
    .join(dim_pet_breed_df, pet_intermediate_df.customer_pet_breed == dim_pet_breed_df.breed_name, "left") \
    .select(
        col("customer_pet_name"),
        dim_pet_type_df.pet_type_id,
        dim_pet_breed_df.breed_id.alias("pet_breed_id")
    ).distinct()
dim_pet_df = write_to_jdbc_table(dim_pet_transformed_df, "dim_pet")
dim_pet_df.cache()

raw_aliased_df = raw_df.alias("raw")

temp_store_country_df = dim_country_df.alias("store_country_dim")
temp_store_city_df = dim_city_df.alias("store_city_dim")
temp_store_state_df = dim_state_df.alias("store_state_dim")

fact_sales_df = raw_aliased_df \
    .join(dim_date_df.alias("dd"), to_date(raw_aliased_df.sale_date) == col("dd.full_date"), "left") \
    .join(dim_customer_df.alias("dcust"), raw_aliased_df.customer_email == col("dcust.customer_email"), "left") \
    .join(dim_seller_df.alias("ds"), raw_aliased_df.seller_email == col("ds.seller_email"), "left") \
    .join(temp_store_country_df, raw_aliased_df.store_country == col("store_country_dim.country_name"), "left") \
    .join(temp_store_city_df, raw_aliased_df.store_city == col("store_city_dim.city_name"), "left") \
    .join(temp_store_state_df, raw_aliased_df.store_state == col("store_state_dim.state_name"), "left") \
    .join(dim_store_df.alias("dst_join"),
          (raw_aliased_df.store_name == col("dst_join.store_name")) & \
          (coalesce(col("store_city_dim.city_id"), lit(-1)) == coalesce(col("dst_join.city_id"), lit(-1))) & \
          (coalesce(col("store_state_dim.state_id"), lit(-1)) == coalesce(col("dst_join.state_id"), lit(-1))) &
          (coalesce(col("store_country_dim.country_id"), lit(-1)) == coalesce(col("dst_join.country_id"), lit(-1))),
          "left") \
    .join(dim_product_category_df.alias("dpc_join"), raw_aliased_df.product_category == col("dpc_join.category_name"), "left") \
    .join(dim_brand_df.alias("db_join"), raw_aliased_df.product_brand == col("db_join.brand_name"), "left") \
    .join(dim_material_df.alias("dm_join"), raw_aliased_df.product_material == col("dm_join.material_name"), "left") \
    .join(dim_color_df.alias("dco_join"), raw_aliased_df.product_color == col("dco_join.color_name"), "left") \
    .join(dim_size_df.alias("dsz_join"), raw_aliased_df.product_size == col("dsz_join.size_name"), "left") \
    .join(dim_product_df.alias("dp_join"),
          (raw_aliased_df.product_name == col("dp_join.product_name")) & \
          (coalesce(col("dpc_join.category_id"), lit(-1)) == coalesce(col("dp_join.category_id"), lit(-1))) & \
          (coalesce(col("db_join.brand_id"), lit(-1)) == coalesce(col("dp_join.brand_id"), lit(-1))) & \
          (coalesce(col("dm_join.material_id"), lit(-1)) == coalesce(col("dp_join.material_id"), lit(-1))) & \
          (coalesce(col("dco_join.color_id"), lit(-1)) == coalesce(col("dp_join.color_id"), lit(-1))) & \
          (coalesce(col("dsz_join.size_id"), lit(-1)) == coalesce(col("dp_join.size_id"), lit(-1))) & \
          (raw_aliased_df.product_weight == col("dp_join.product_weight_gr")), # Careful with float comparison
          "left") \
    .join(dim_pet_type_df.alias("dpt_join_f"), raw_aliased_df.customer_pet_type == col("dpt_join_f.pet_type_name"), "left") \
    .join(dim_pet_breed_df.alias("dpb_join_f"), raw_aliased_df.customer_pet_breed == col("dpb_join_f.breed_name"), "left") \
    .join(dim_pet_df.alias("dpet_join"),
          (raw_aliased_df.customer_pet_name == col("dpet_join.customer_pet_name")) & \
          (coalesce(col("dpt_join_f.pet_type_id"), lit(-1)) == coalesce(col("dpet_join.pet_type_id"), lit(-1))) & \
          (coalesce(col("dpb_join_f.breed_id"), lit(-1)) == coalesce(col("dpet_join.pet_breed_id"), lit(-1))),
          "left") \
    .select(
        col("dd.date_id"),
        col("dcust.customer_id"),
        col("ds.seller_id"),
        col("dp_join.product_id"),
        col("dst_join.store_id"),
        col("dp_join.supplier_id").alias("supplier_id"),
        raw_aliased_df.sale_quantity,
        raw_aliased_df.sale_total_price,
        raw_aliased_df.id.alias("original_raw_id"),
        col("dpet_join.pet_id")
    ).filter(
        col("dd.date_id").isNotNull() & \
        col("dcust.customer_id").isNotNull() & \
        col("ds.seller_id").isNotNull() & \
        col("dp_join.product_id").isNotNull() & \
        col("dst_join.store_id").isNotNull() & \
        col("dp_join.supplier_id").isNotNull()
    )

write_to_jdbc_table(fact_sales_df, "fact_sales")

tables_to_count = [
    "dim_product_category", "dim_pet_category", "dim_brand", "dim_material",
    "dim_color", "dim_size", "dim_country", "dim_city", "dim_pet_breed", "dim_pet_type",
    "dim_date", "dim_supplier", "dim_store", "dim_seller", "dim_customer",
    "dim_product", "dim_pet", "fact_sales"
]

for table_name in tables_to_count:
    try:
        count_df = spark.read.jdbc(url=jdbc_url, table=table_name, properties=db_properties)
        record_count = count_df.count()
        print(f"Table: {table_name}, Count: {record_count}")
    except Exception as e:
        print(f"Could not count table {table_name}: {e}")

spark.catalog.clearCache()

spark.stop()