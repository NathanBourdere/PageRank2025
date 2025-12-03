#!/usr/bin/env python3
import argparse, time, sys, os
from pyspark import SparkConf, SparkContext
from src.utils import parse_ttl_line
from pyspark.rdd import portable_hash

def build_links(rdd, npartitions):
    # rdd : lines -> produce (src, dst) pairs
    pairs = rdd.map(lambda l: parse_ttl_line(l)).filter(lambda x: x is not None)
    pairs = pairs.distinct()
    # repartition pairs by src to align neighbors and future ranks
    pairs = pairs.partitionBy(npartitions, lambda k: portable_hash(k))
    # groupByKey will not reshuffle across partitions if partitioning already matches
    links = pairs.groupByKey().mapValues(list).cache()
    return links

def pagerank(links, iterations=10, damping=0.85):
    # links : PairRDD (src, [dsts]) partitioned
    ranks = links.mapValues(lambda _: 1.0).cache()
    for i in range(iterations):
        # join uses partitioner of links and ranks; if both share same partitioner, shuffle reduced
        contribs = links.join(ranks) \
            .flatMap(lambda kv: [(dst, kv[1][1] / len(kv[1][0])) for dst in kv[1][0]])
        ranks = contribs.reduceByKey(lambda x, y: x + y).mapValues(lambda v: (1.0 - damping) + damping * v).cache()
    return ranks

def write_topk(ranks_rdd, out_gcs, topk=50):
    # takeOrdered on driver (topk small)
    top = ranks_rdd.takeOrdered(topk, key=lambda x: -x[1])
    # write pagerank_topk.tsv to GCS (use sc.parallelize -> saveAsTextFile to out_gcs/pagerank_topk)
    lines = ["{}\t{}".format(k, v) for k, v in top]
    # write as a single small file
    import tempfile
    tf = tempfile.NamedTemporaryFile(delete=False, mode="w")
    for l in lines:
        tf.write(l + "\n")
    tf.close()
    # upload via gsutil if available, else try Spark write
    dest = os.path.join(out_gcs, "pagerank_topk.tsv")
    # Attempt simple python copy via subprocess gsutil (Dataproc master has gsutil)
    import subprocess
    subprocess.check_call(["gsutil", "cp", tf.name, dest])
    os.unlink(tf.name)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True, help="gs://.../sample10pct.ttl (or pattern)")
    parser.add_argument("--output", required=True, help="gs://.../outputs/this_run")
    parser.add_argument("--iterations", type=int, default=10)
    parser.add_argument("--partitions", type=int, default=200)
    parser.add_argument("--topk", type=int, default=50)
    args = parser.parse_args()

    conf = SparkConf().setAppName("PageRank-RDD")
    sc = SparkContext(conf=conf)

    t0 = time.time()
    lines = sc.textFile(args.input)
    links = build_links(lines, args.partitions)
    t_links = time.time()
    ranks = pagerank(links, iterations=args.iterations)
    t_iter = time.time()

    # save full ranks as small sample (coalesce to 1) - optional, careful with size
    ranks_coal = ranks.coalesce(1)
    ranks_coal.saveAsTextFile(os.path.join(args.output, "ranks_rdd"))

    # write topk small file
    write_topk(ranks, args.output, topk=args.topk)

    t_end = time.time()
    print("TIMINGS: build_links_s= {:.3f}".format(t_links - t0))
    print("TIMINGS: total_s= {:.3f}".format(t_end - t0))

    sc.stop()
