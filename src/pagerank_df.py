#!/usr/bin/env python3
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_extract, explode, size, lit, collect_list

def clean_uri(uri):
    return uri[uri.rfind("/") + 1 : ]

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()

    spark = SparkSession.builder.appName("PageRankDF").getOrCreate()

    df = spark.read.text(args.input)

    # Extract 1st, 2nd, 3rd <...> blocks
    parsed = df.select(
        regexp_extract("value", r"<([^>]*)>", 1).alias("src_full"),
        regexp_extract("value", r"<([^>]*)>.*<([^>]*)>", 2).alias("pred_full"),
        regexp_extract("value", r"<([^>]*)>.*<([^>]*)>.*<([^>]*)>", 3).alias("dst_full")
    )

    # Clean URIs to keep last part only
    links = links.selectExpr(
        "regexp_extract(src_full, '.*/([^/]*)$', 1) as src",
        "regexp_extract(dst_full, '.*/([^/]*)$', 1) as dst"
    )

    links = links.distinct()

    # Build adjacency list
    adj = links.groupBy("src").agg(collect_list("dst").alias("outlinks")).cache()

    ranks = adj.select(col("src").alias("node"), lit(1.0).alias("rank"))

    for i in range(10):
        contribs = adj.join(ranks, adj.src == ranks.node) \
            .select(
                explode("outlinks").alias("dst"),
                (col("rank") / size("outlinks")).alias("contrib")
            )
        ranks = contribs.groupBy("dst").sum("contrib") \
            .select(
                col("dst").alias("node"),
                (lit(0.15) + lit(0.85) * col("sum(contrib)")).alias("rank")
            )

    ranks.write.mode("overwrite").csv(args.output)
    spark.stop()


if __name__ == "__main__":
    main()
