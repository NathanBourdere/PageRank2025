#!/usr/bin/env python3
import argparse, time, os, tempfile, subprocess
from pyspark.sql import SparkSession
from pyspark.sql.functions import split, col, size, explode, lit, sum as _sum, collect_list
from pyspark.sql import functions as F

def parse_pairs_df(spark, input_path):
    raw = spark.read.text(input_path)
    pairs = raw.select(split(col("value"), "\\s+").alias("parts")) \
               .filter(F.size("parts") >= 3) \
               .select(col("parts").getItem(0).alias("src"), col("parts").getItem(2).alias("dst"))
    pairs = pairs.dropDuplicates()
    return pairs

def write_topk_df(df_topk, out_gcs):
    # collect small topk to driver and write with gsutil
    rows = df_topk.collect()
    tf = tempfile.NamedTemporaryFile(delete=False, mode="w")
    for r in rows:
        tf.write(f"{r['id']}\t{r['rank']}\n")
    tf.close()
    dest = os.path.join(out_gcs, "pagerank_topk.tsv")
    subprocess.check_call(["gsutil", "cp", tf.name, dest])
    os.unlink(tf.name)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--iterations", type=int, default=10)
    parser.add_argument("--partitions", type=int, default=200)
    parser.add_argument("--topk", type=int, default=50)
    args = parser.parse_args()

    spark = SparkSession.builder.appName("PageRank-DataFrame").getOrCreate()
    spark.conf.set("spark.sql.adaptive.enabled", "true")

    t0 = time.time()
    pairs = parse_pairs_df(spark, args.input)
    # repartition by src to align adjacency and ranks (avoid shuffle on join)
    adj = pairs.repartition(args.partitions, "src").groupBy("src").agg(collect_list("dst").alias("neighbors")).cache()
    # start ranks
    ranks = adj.select(col("src").alias("id")).withColumn("rank", lit(1.0)).cache()

    for i in range(args.iterations):
        # join on src==id; since adj and ranks are partitioned by 'src' they will colocate partitions
        exploded = adj.join(ranks, adj.src == ranks.id)\
                      .select(F.explode("neighbors").alias("dst"), col("rank"), F.size("neighbors").alias("deg"))
        contribs = exploded.groupBy("dst").agg(_sum(col("rank") / col("deg")).alias("sumr"))
        ranks = contribs.select(col("dst").alias("id"), (lit(0.15) + lit(0.85) * col("sumr")).alias("rank")).cache()

    t_end = time.time()
    topk_df = ranks.orderBy(col("rank").desc()).limit(args.topk)
    topk_df.show(20, False)

    # write ranks (careful, global ranks can be large; here we write full ranks if needed)
    ranks.write.mode("overwrite").csv(os.path.join(args.output, "ranks_df"))

    # write small topk using gsutil
    write_topk_df(topk_df, args.output)

    print("build+iter_time_s:", t_end - t0)
    spark.stop()
