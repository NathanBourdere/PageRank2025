#!/usr/bin/env python3
from pyspark.sql import SparkSession
from pyspark.sql.functions import split, col, size, explode, lit, sum as _sum, collect_list
from pyspark.sql import functions as F
import sys
import time

def parse_pairs_df(spark, input_path):
    # read as text, split tokens, keep src and dst (very simplified)
    raw = spark.read.text(input_path)
    pairs = raw.select(split(col("value"), "\\s+").alias("parts")) \
               .filter(F.size("parts") >= 3) \
               .select(col("parts").getItem(0).alias("src"), col("parts").getItem(2).alias("dst"))
    pairs = pairs.distinct()
    return pairs

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("usage: pagerank_df.py gs://bucket/path/* [iterations]")
        sys.exit(1)

    input_path = sys.argv[1]
    iterations = int(sys.argv[2]) if len(sys.argv) > 2 else 10

    spark = SparkSession.builder.appName("PageRank-DataFrame").getOrCreate()
    spark.conf.set("spark.sql.adaptive.enabled", "true")  # AEQ helps reduce shuffle impact

    t0 = time.time()
    pairs = parse_pairs_df(spark, input_path)
    # build adjacency list once
    adj = pairs.groupBy("src").agg(collect_list("dst").alias("neighbors")).cache()
    # start ranks
    ranks = adj.select(col("src").alias("id")).withColumn("rank", lit(1.0))

    for i in range(iterations):
        # explode neighbors, compute contribution
        exploded = adj.join(ranks, adj.src == ranks.id).select(F.explode("neighbors").alias("dst"), col("rank"), F.size("neighbors").alias("deg"))
        contribs = exploded.groupBy("dst").agg(_sum(col("rank") / col("deg")).alias("sumr"))
        ranks = contribs.select(col("dst").alias("id"), (lit(0.15) + lit(0.85) * col("sumr")).alias("rank"))

    t_end = time.time()
    # show top
    ranks.orderBy(col("rank").desc()).show(20, False)
    print("build+iter_time_s:", t_end - t0)

    # save
    ranks.write.mode("overwrite").csv(input_path.replace("/data/","/results/") + "/ranks_df")

    spark.stop()
