#!/usr/bin/env python3
import argparse
from pyspark.sql import SparkSession

def parse_ttl(line):
    # Example:
    # <http://dbpedia.org/resource/A> <http://dbpedia.org/ontology/wikiPageWikiLink> <http://dbpedia.org/resource/B> .
    parts = line.strip().split()

    if len(parts) < 3:
        return None

    subj = parts[0]
    pred = parts[1]
    obj  = parts[2]

    # Remove angle brackets, keep last part of URL
    def clean(uri):
        return uri[uri.rfind("/") + 1 : ].replace(">", "").replace("<", "")

    return clean(subj), clean(obj)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()

    spark = SparkSession.builder.appName("PageRankRDD").getOrCreate()
    sc = spark.sparkContext

    print(f"[RDD] Reading: {args.input}")

    lines = sc.textFile(args.input)

    edges = (
        lines.map(parse_ttl)
             .filter(lambda x: x is not None)
             .distinct()
    )

    links = edges.groupByKey().cache()

    # init ranks
    ranks = links.mapValues(lambda _: 1.0)

    for i in range(10):
        print(f"Iteration {i+1}/10")
        contribs = links.join(ranks).flatMap(
            lambda kv: [(dst, kv[1][1] / len(kv[1][0])) for dst in kv[1][0]]
        )
        ranks = contribs.reduceByKey(lambda a, b: a + b).mapValues(
            lambda r: 0.15 + 0.85 * r
        )

    ranks.saveAsTextFile(args.output)
    spark.stop()


if __name__ == "__main__":
    main()
