#!/usr/bin/env python3
import argparse
from pyspark.sql import SparkSession

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()

    spark = SparkSession.builder.appName("PageRankRDD").getOrCreate()
    sc = spark.sparkContext

    lines = sc.textFile(args.input)

    # Extraire src et dst
    def parse_line(line):
        parts = line.split('>')
        if len(parts) >= 3:
            src = parts[0].strip()[1:].split('/')[-1]
            pred = parts[1].strip()[1:].split('/')[-1]
            dst = parts[2].strip()[1:].split('/')[-1]
            if pred == "wikiPageWikiLink":
                return (src, dst)
        return None

    edges = lines.map(parse_line).filter(lambda x: x is not None).distinct()

    # Tous les nœuds
    nodes = edges.flatMap(lambda x: [x[0], x[1]]).distinct()
    ranks = nodes.map(lambda n: (n, 1.0))

    # Construire adjacency list
    links = edges.groupByKey().mapValues(list)

    # PageRank iterations
    for i in range(10):
        contribs = links.join(ranks).flatMap(
            lambda x: [(dst, x[1][1] / len(x[1][0])) for dst in x[1][0]] if x[1][0] else []
        )

        # Ajouter nœuds sans outlinks
        contribs = contribs.union(
            nodes.subtract(contribs.keys()).map(lambda n: (n, 0.0))
        )

        ranks = contribs.reduceByKey(lambda x, y: x + y).mapValues(lambda s: 0.15 + 0.85 * s)

    # Écriture finale
    ranks.map(lambda x: f"{x[0]},{x[1]}").coalesce(1).saveAsTextFile(args.output)

    spark.stop()

if __name__ == "__main__":
    main()
