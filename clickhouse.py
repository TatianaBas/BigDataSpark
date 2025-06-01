from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, count as _count, avg as _avg, asc, desc, year, month, quarter, lag, countDistinct, corr
from pyspark.sql.window import Window
from pyspark.ml.stat import Correlation
from pyspark.ml.feature import VectorAssembler
from time import sleep

spark = SparkSession.builder \
        .appName("BD Spark ClickHouse") \
        .config("spark.jars", "postgresql-42.6.0.jar,clickhouse-jdbc-0.4.6.jar") \
        .getOrCreate()

pg_url = "jdbc:postgresql://localhost:5432/spark_db"
pg_opts = {"user": "spark_user", "password": "spark_password", "driver": "org.postgresql.Driver"}

ch_url = "jdbc:clickhouse://localhost:8123/default"
ch_opts = {"driver": "com.clickhouse.jdbc.ClickHouseDriver", "user": "custom_user", "password": "custom_password"}

# Загрузка таблиц звёзды
df_customers = spark.read.jdbc(url=pg_url, table="d_customers", properties=pg_opts)
df_sellers = spark.read.jdbc(url=pg_url, table="d_sellers", properties=pg_opts)
df_products = spark.read.jdbc(url=pg_url, table="d_products", properties=pg_opts)
df_stores = spark.read.jdbc(url=pg_url, table="d_stores", properties=pg_opts)
df_suppliers = spark.read.jdbc(url=pg_url, table="d_suppliers", properties=pg_opts)
df_sales = spark.read.jdbc(url=pg_url, table="f_sales", properties=pg_opts)

def mart_products():
    top = df_sales.groupBy("product_id").agg(_sum("quantity").alias("quantity_sold")) \
        .orderBy(desc("quantity_sold")).limit(10) \
        .join(df_products, df_sales.product_id == df_products.id, "left") \
        .select(df_sales.product_id.alias("id"), df_products.name, col("quantity_sold"))
    top.write.jdbc(url=ch_url, table="mart_products_top", mode="append", properties=ch_opts)

def mart_customers():
    top = df_sales.groupBy("customer_id").agg(_sum("total_price").alias("spent_total")) \
        .orderBy(desc("spent_total")).limit(10) \
        .join(df_customers, df_sales.customer_id == df_customers.id, "left") \
        .select(df_sales.customer_id.alias("id"), df_customers.first_name, df_customers.last_name, col("spent_total"))
    top.write.jdbc(url=ch_url, table="mart_customers_top", mode="append", properties=ch_opts)

def mart_stores():
    top = df_sales.groupBy("store_id").agg(_sum("total_price").alias("revenue")) \
        .orderBy(desc("revenue")).limit(5) \
        .join(df_stores, df_sales.store_id == df_stores.id, "left") \
        .select(df_sales.store_id.alias("id"), df_stores.name.alias("store"), col("revenue"))
    top.write.jdbc(url=ch_url, table="mart_stores_top", mode="append", properties=ch_opts)

def mart_suppliers():
    top = df_sales.groupBy("supplier_id").agg(_sum("total_price").alias("revenue")) \
        .orderBy(desc("revenue")).limit(5) \
        .join(df_suppliers, df_sales.supplier_id == df_suppliers.id, "left") \
        .select(df_sales.supplier_id.alias("id"), df_suppliers.name.alias("supplier"), col("revenue"))
    top.write.jdbc(url=ch_url, table="mart_suppliers_top", mode="append", properties=ch_opts)

def mart_quality():
    best = df_products.orderBy(desc("rating")).limit(100).select(col("id"), col("name"), col("rating"))
    best.write.jdbc(url=ch_url, table="mart_products_best", mode="append", properties=ch_opts)
    worst = df_products.orderBy(asc("rating")).limit(100).select(col("id"), col("name"), col("rating"))
    worst.write.jdbc(url=ch_url, table="mart_products_worst", mode="append", properties=ch_opts)

    sales_by_prod = df_sales.groupBy("product_id").agg(_sum("quantity").alias("quantity_sold"), _sum("total_price").alias("revenue"))
    stats = sales_by_prod.join(df_products, sales_by_prod.product_id == df_products.id, "inner").filter(col("rating").isNotNull())
    assembler = VectorAssembler(inputCols=["rating", "quantity_sold", "revenue"], outputCol="features")
    features = assembler.transform(stats)
    corr_mtx = Correlation.corr(features, "features").collect()[0][0]
    corr_rating_qty = corr_mtx.toArray()[0][1]
    corr_rating_rev = corr_mtx.toArray()[0][2]
    sql_corr = stats.select(corr("rating", "quantity_sold").alias("corr_rating_qty"), corr("rating", "revenue").alias("corr_rating_rev")).collect()[0]
    report = spark.createDataFrame([
        ("Rating vs Quantity", float(corr_rating_qty), float(sql_corr["corr_rating_qty"])),
        ("Rating vs Revenue", float(corr_rating_rev), float(sql_corr["corr_rating_rev"]))
    ], ["metric", "corr_matrix", "corr_sql"])
    report.write.jdbc(url=ch_url, table="mart_quality_correlation", mode="append", properties=ch_opts)

mart_products()
mart_customers()
mart_stores()
mart_suppliers()
mart_quality()

print("sleeping...")
sleep(2)
print("woke up!")

spark.stop()
