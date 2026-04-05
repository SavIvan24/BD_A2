from pathvalidate import sanitize_filename
from pyspark.sql import SparkSession
import os


spark = SparkSession.builder \
    .appName('data preparation') \
    .master("local") \
    .config("spark.sql.parquet.enableVectorizedReader", "true") \
    .getOrCreate()


df = spark.read.parquet("/a.parquet")
n = 100
df = df.select(['id', 'title', 'text'])
cnt = df.count()
if cnt == 0:
    raise RuntimeError("Parquet has no rows")
# Spark sample() requires fraction in (0, 1]; avoid invalid fractions when cnt is small
frac = min(1.0, max(float(n) / float(cnt), 1e-6))
df = df.sample(withReplacement=False, fraction=frac, seed=0).limit(n)

os.makedirs("data", exist_ok=True)


def create_doc(row):
    filename = "data/" + sanitize_filename(str(row['id']) + "_" + row['title']).replace(" ", "_") + ".txt"
    with open(filename, "w") as f:
        f.write(row['text'])


df.foreach(create_doc)

spark.stop()
