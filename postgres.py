from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, monotonically_increasing_id
from time import sleep

def build_star_schema(session, df, jdbc_url, opts):
    # Customers dimension
    customers_df = (df.select(
            col("customer_first_name").alias("first_name"),
            col("customer_last_name").alias("last_name"),
            col("customer_age").alias("age"),
            col("customer_email").alias("email"),
            col("customer_country").alias("country"),
            col("customer_postal_code").alias("postal_code"),
            col("customer_pet_type").alias("pet_type"),
            col("customer_pet_name").alias("pet_name"),
            col("customer_pet_breed").alias("pet_breed")
        ).distinct()
        .withColumn("customer_id", monotonically_increasing_id() + 100))
    customers_df.write.jdbc(url=jdbc_url, table="d_customers", mode="overwrite", properties=opts)

    # Sellers dimension
    sellers_df = (df.select(
            col("seller_first_name").alias("first_name"),
            col("seller_last_name").alias("last_name"),
            col("seller_email").alias("email"),
            col("seller_country").alias("country"),
            col("seller_postal_code").alias("postal_code")
        ).distinct()
        .withColumn("seller_id", monotonically_increasing_id() + 100))
    sellers_df.write.jdbc(url=jdbc_url, table="d_sellers", mode="overwrite", properties=opts)

    # Products dimension
    products_df = (df.select(
            col("product_name").alias("name"),
            col("product_category").alias("category"),
            col("product_price").alias("price"),
            col("pet_category").alias("pet_category")
            col("product_weight").alias("weight"),
            col("product_color").alias("color"),
            col("product_size").alias("size"),
            col("product_brand").alias("brand"),
            col("product_material").alias("material"),
            col("product_description").alias("description"),
            col("product_rating").alias("rating"),
            col("product_reviews").alias("reviews"),
            to_date(col("product_release_date"), "M/d/y").alias("release_date"),
            to_date(col("product_expiry_date"), "M/d/y").alias("expiry_date")
        ).distinct()
        .withColumn("product_id", monotonically_increasing_id() + 100))
    products_df.write.jdbc(url=jdbc_url, table="d_products", mode="overwrite", properties=opts)

    # Stores dimension
    stores_df = (df.select(
            col("store_name").alias("name"),
            col("store_location").alias("location"),
            col("store_city").alias("city"),
            col("store_state").alias("state"),
            col("store_country").alias("country"),
            col("store_phone").alias("phone"),
            col("store_email").alias("email")
        ).distinct()
        .withColumn("store_id", monotonically_increasing_id() + 100))
    stores_df.write.jdbc(url=jdbc_url, table="d_stores", mode="overwrite", properties=opts)

    # Suppliers dimension
    suppliers_df = (df.select(
            col("supplier_name").alias("name"),
            col("supplier_contact").alias("contact"),
            col("supplier_email").alias("email"),
            col("supplier_phone").alias("phone"),
            col("supplier_address").alias("address"),
            col("supplier_city").alias("city"),
            col("supplier_country").alias("country")
        ).distinct()
        .withColumn("supplier_id", monotonically_increasing_id() + 100))
    suppliers_df.write.jdbc(url=jdbc_url, table="d_suppliers", mode="overwrite", properties=opts)

    # Read back dimensions to get surrogate keys
    customers_df = session.read.jdbc(url=jdbc_url, table="d_customers", properties=opts)
    sellers_df = session.read.jdbc(url=jdbc_url, table="d_sellers", properties=opts)
    products_df = session.read.jdbc(url=jdbc_url, table="d_products", properties=opts)
    stores_df = session.read.jdbc(url=jdbc_url, table="d_stores", properties=opts)
    suppliers_df = session.read.jdbc(url=jdbc_url, table="d_suppliers", properties=opts)

    # Fact table
    fact_df = (df
        .join(customers_df, df.customer_email == customers_df.email, "left")
        .join(sellers_df, df.seller_email == sellers_df.email, "left")
        .join(products_df, (df.product_name == products_df.name) & (df.product_price == products_df.price), "left")
        .join(stores_df, df.store_email == stores_df.email, "left")
        .join(suppliers_df, df.supplier_email == suppliers_df.email, "left")
        .select(
            col("customer_id"),
            col("seller_id"),
            col("product_id"),
            col("store_id"),
            col("supplier_id"),
            to_date(col("sale_date"), "M/d/y").alias("sale_date"),
            col("product_quantity"),
            col("sale_quantity"),
            col("sale_total_price")
        ).withColumn("sale_id", monotonically_increasing_id() + 100))
    fact_df.write.jdbc(url=jdbc_url, table="f_sales", mode="overwrite", properties=opts)

def run():
    spark = SparkSession.builder \
        .appName("Star Schema Spark Postgres") \
        .config("spark.jars", "postgresql-42.6.0.jar") \
        .getOrCreate()

    url = "jdbc:postgresql://localhost:5432/spark_db"
    props = {"user": "spark_user", "password": "spark_password", "driver": "org.postgresql.Driver"}
    df = spark.read.jdbc(url=url, table="mock_data", properties=props)

    build_star_schema(spark, df, url, props)

    print("sleeping...")
    sleep(2)
    print("woke up!")
    spark.stop()

if __name__ == "__main__":
    run()
