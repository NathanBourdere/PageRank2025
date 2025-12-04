#!/usr/bin/env python3
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_extract, explode, size, lit, collect_list, coalesce, when, array

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()

    spark = SparkSession.builder.appName("PageRankDF").getOrCreate()

    df = spark.read.text(args.input)

    # Extraire src/pred/dst
    parsed = df.select(
        regexp_extract("value", r"<([^>]*)>", 1).alias("src_full"),
        regexp_extract("value", r"<([^>]*)>.*<([^>]*)>", 2).alias("pred_full"),
        regexp_extract("value", r"<([^>]*)>.*<([^>]*)>.*<([^>]*)>", 3).alias("dst_full")
    )

    # Filtrer uniquement wikiPageWikiLink
    links = parsed.filter(
        col("pred_full") == "http://dbpedia.org/ontology/wikiPageWikiLink"
    ).selectExpr(
        "regexp_extract(src_full, '.*/([^/]*)$', 1) as src",
        "regexp_extract(dst_full, '.*/([^/]*)$', 1) as dst"
    ).distinct()

    # Tous les nœuds uniques
    all_nodes = links.select("src").union(links.select("dst")).distinct()
    ranks = all_nodes.select(col("src").alias("node")).withColumn("rank", lit(1.0))

    # Adjacency list
    adj = links.groupBy("src").agg(collect_list("dst").alias("outlinks"))

    # PageRank iterations
    for i in range(10):
        contribs = adj.join(ranks, adj.src == ranks.node, how="right") \
            .select(
                explode(when(col("outlinks").isNotNull(), col("outlinks")).otherwise(array())).alias("dst"),
                (col("rank") / size(coalesce("outlinks", lit([])))).alias("contrib")
            )

        ranks = contribs.groupBy("dst").sum("contrib") \
            .select(
                col("dst").alias("node"),
                (lit(0.15) + lit(0.85) * col("sum(contrib)")).alias("rank")
            )

    # Écriture finale
    ranks.coalesce(1).write.mode("overwrite").option("header", "true").csv(args.output)

    spark.stop()

if __name__ == "__main__":
    main()
