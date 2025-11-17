#!/usr/bin/env python3
from pyspark import SparkConf, SparkContext
from src.utils import parse_ttl_line
import sys
import time

def build_links(rdd):
    # parse, filter, keep (src, dst) pairs
    pairs = rdd.map(lambda l: parse_ttl_line(l)).filter(lambda x: x is not None)
    # remove duplicates
    pairs = pairs.distinct()
    # Create adjacency lists once; partitionBy to align partitions and minimize future shuffle
    links = pairs.groupByKey().mapValues(list)
    # Persist adjacency (we will reuse for iterations)
    links = links.partitionBy(200).cache()  # ajuste 200 selon taille cluster
    # optionally count nodes
    return links

def pagerank(links, iterations=10, damping=0.85):
    ranks = links.mapValues(lambda _: 1.0)
    for i in range(iterations):
        # join links with ranks: join may shuffle but links and ranks are partitioned similarly
        contribs = links.join(ranks).flatMap(
            lambda kv: [(dst, kv[1][1] / len(kv[1][0])) for dst in kv[1][0]]
        )
        # reduceByKey to pre-aggregate before shuffle
        ranks = contribs.reduceByKey(lambda x, y: x + y).mapValues(lambda v: (1.0 - damping) + damping * v)
        ranks = ranks.cache()
    return ranks

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("usage: pagerank_rdd.py gs://bucket/path/* [iterations]")
        sys.exit(1)

    input_path = sys.argv[1]
    iterations = int(sys.argv[2]) if len(sys.argv) > 2 else 10

    conf = SparkConf().setAppName("PageRank-RDD")
    sc = SparkContext(conf=conf)

    t0 = time.time()
    lines = sc.textFile(input_path)
    links = build_links(lines)
    t_links = time.time()
    ranks = pagerank(links, iterations=iterations)
    top = ranks.takeOrdered(20, key=lambda x: -x[1])
    t_end = time.time()

    print("=== TIMINGS ===")
    print("build_links_s: ", t_links - t0)
    print("total_s: ", t_end - t0)
    print("Top 20:")
    for node, score in top:
        print(node, score)

    # save ranks to GCS
    ranks.coalesce(1).saveAsTextFile(input_path.replace("/data/","/results/") + "/ranks_rdd")

    sc.stop()
