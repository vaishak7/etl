from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, trim, when, to_date, regexp_extract, lower,
    split, explode, count, desc
)
from pyspark.sql.types import StructType, StructField, StringType

# ---------- CONFIG ----------
CSV_PATH = "data/netflix_titles.csv"

PG_URL = "jdbc:postgresql://localhost:5432/postgres"
PG_USER = "postgres"
PG_PASSWORD = "Vaishakm@2002"
PG_DRIVER = "org.postgresql.Driver"

TBL_RAW = "netflix_movies"
TBL_METRICS = "netflix_genre_metrics"

# ---------- SPARK (turn off ANSI strict mode to avoid hard failures) ----------
spark = (
    SparkSession.builder
    .appName("NetflixETL")
    .config("spark.sql.ansi.enabled", "false")
    .getOrCreate()
)

# ---------- 1) EXTRACT (ALL STRING schema: safest) ----------
cols = [
    "show_id", "type", "title", "director", "cast", "country",
    "date_added", "release_year", "rating", "duration", "listed_in", "description"
]
schema = StructType([StructField(c, StringType(), True) for c in cols])

df = (
    spark.read
    .option("mode", "PERMISSIVE")
    .csv(CSV_PATH, header=True, schema=schema)
)

# ---------- 2) CLEAN ----------
df = df.withColumnRenamed("cast", "cast_members")

# trim + blank->null (safe because everything is STRING)
for c in df.columns:
    df = df.withColumn(c, trim(col(c)))
    df = df.withColumn(c, when(col(c) == "", None).otherwise(col(c)))

# parse date_added safely -> DATE (bad values become NULL)
df = df.withColumn("date_added", to_date(col("date_added"), "MMMM d, yyyy"))

# safe release_year: extract digits then cast -> INT
# (if release_year is empty/dirty, becomes NULL)
df = df.withColumn("release_year", regexp_extract(col("release_year"), r"(\d{4})", 1).cast("int"))

# dedup
df = df.dropDuplicates(["show_id"])

# ---------- 3) FILTER (example) ----------
df_recent = df.filter(col("release_year").isNotNull() & (col("release_year") >= 2015))

# ---------- 4) TRANSFORM (example business fields) ----------
df_recent = df_recent.withColumn("content_type", lower(col("type")))
df_recent = df_recent.withColumn("rating_clean", lower(col("rating")))

# seasons for TV shows
df_recent = df_recent.withColumn(
    "season_count",
    when(col("type") == "TV Show", regexp_extract(col("duration"), r"(\d+)", 1).cast("int")).otherwise(None)
)

# minutes for movies + long flag
df_recent = df_recent.withColumn(
    "minutes",
    when(col("type") == "Movie", regexp_extract(col("duration"), r"(\d+)", 1).cast("int")).otherwise(None)
)
df_recent = df_recent.withColumn(
    "is_long_movie",
    when((col("type") == "Movie") & (col("minutes") >= 120), True).otherwise(False)
)

df_recent = df_recent.withColumn(
    "dq_missing_core",
    when(col("title").isNull() | col("description").isNull(), True).otherwise(False)
)

# ---------- 5) LOAD main cleaned dataset ----------
df_to_pg = df_recent.select(
    "show_id", "type", "title", "director", "cast_members", "country",
    "date_added", "release_year", "rating", "duration", "listed_in", "description"
)

props = {"user": PG_USER, "password": PG_PASSWORD, "driver": PG_DRIVER}

df_to_pg.write \
    .format("jdbc") \
    .option("url", PG_URL) \
    .option("dbtable", TBL_RAW) \
    .option("user", PG_USER) \
    .option("password", PG_PASSWORD) \
    .option("driver", PG_DRIVER) \
    .option("batchsize", "1000") \
    .mode("overwrite") \
    .save()

# ---------- 6) LOAD metrics table ----------
genres = df_recent.select(
    explode(split(col("listed_in"), ",")).alias("genre")
).withColumn("genre", trim(col("genre"))).filter(col("genre").isNotNull())

genre_metrics = genres.groupBy("genre").agg(count("*").alias("titles_count")) \
                     .orderBy(desc("titles_count"))

genre_metrics.write \
    .format("jdbc") \
    .option("url", PG_URL) \
    .option("dbtable", TBL_METRICS) \
    .option("user", PG_USER) \
    .option("password", PG_PASSWORD) \
    .option("driver", PG_DRIVER) \
    .option("batchsize", "1000") \
    .mode("overwrite") \
    .save()

spark.stop()
print("[OK] ETL complete: loaded netflix_movies and netflix_genre_metrics")
