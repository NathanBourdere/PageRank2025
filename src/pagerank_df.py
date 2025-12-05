#!/usr/bin/env python3
import argparse
import time
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_extract, explode, size, lit, collect_list, coalesce, split, trim

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--iterations", type=int, default=10)
    args = parser.parse_args()
    
    start_time = time.time()
    
    spark = SparkSession.builder.appName("PageRankDF").getOrCreate()
    
    print(f"[INFO] Lecture du fichier : {args.input}")
    df = spark.read.text(args.input)
    
    total_lines = df.count()
    print(f"[INFO] Nombre total de lignes lues : {total_lines}")
    
    # Parse TTL format: <src> <pred> <dst> .
    # Strategy: Split by space, extract URIs from <...>
    parsed = df.select(
        col("value"),
        # Extract all three URIs accounting for the final dot
        regexp_extract("value", r"<([^>]+)>\s+<([^>]+)>\s+<([^>]+)>", 1).alias("src_full"),
        regexp_extract("value", r"<([^>]+)>\s+<([^>]+)>\s+<([^>]+)>", 2).alias("pred_full"),
        regexp_extract("value", r"<([^>]+)>\s+<([^>]+)>\s+<([^>]+)>", 3).alias("dst_full")
    )
    
    print("[DEBUG] Aperçu du parsing (10 premières lignes) :")
    parsed.select("src_full", "pred_full", "dst_full").show(10, truncate=False)
    
    # Count non-empty parsed lines
    non_empty_parsed = parsed.filter(
        (col("src_full") != "") & (col("pred_full") != "") & (col("dst_full") != "")
    ).count()
    print(f"[INFO] Lignes parsées avec succès : {non_empty_parsed}/{total_lines}")
    
    if non_empty_parsed == 0:
        print("[ERROR] Échec du parsing - aucune ligne extraite !")
        print("[DEBUG] Exemple de ligne brute :")
        df.show(5, truncate=False)
        spark.stop()
        return
    
    # Keep only wikiPageWikiLink predicate
    links = parsed.filter(
        col("pred_full") == "http://dbpedia.org/ontology/wikiPageWikiLink"
    )
    
    wikilink_count = links.count()
    print(f"[INFO] Lignes avec wikiPageWikiLink : {wikilink_count}")
    
    if wikilink_count == 0:
        print("[ERROR] Aucun lien wikiPageWikiLink trouvé !")
        print("[DEBUG] Prédicats uniques trouvés (top 20) :")
        parsed.select("pred_full").filter(col("pred_full") != "").distinct().show(20, truncate=False)
        spark.stop()
        return
    
    # Clean URIs to keep last part only (after last /)
    links = links.selectExpr(
        "regexp_extract(src_full, '[^/]+$', 0) as src",
        "regexp_extract(dst_full, '[^/]+$', 0) as dst"
    )
    
    print("[DEBUG] Aperçu des liens nettoyés (10 premiers) :")
    links.show(10, truncate=False)
    
    # Remove empty strings and duplicates
    links = links.filter((col("src") != "") & (col("dst") != "")).distinct()
    
    num_links = links.count()
    print(f"[INFO] Nombre de liens uniques après nettoyage : {num_links}")
    
    if num_links == 0:
        print("[ERROR] Aucun lien valide après nettoyage !")
        spark.stop()
        return
    
    # Build adjacency list
    print("[INFO] Construction de la liste d'adjacence...")
    adj = links.groupBy("src").agg(collect_list("dst").alias("outlinks")).cache()
    
    # Get all unique nodes (sources + destinations)
    print("[INFO] Identification de tous les nœuds...")
    all_nodes = links.select(col("src").alias("node")) \
        .union(links.select(col("dst").alias("node"))) \
        .distinct()
    
    num_nodes = all_nodes.count()
    print(f"[INFO] Nombre de nœuds uniques : {num_nodes}")
    
    # Initialize ranks for ALL nodes
    ranks = all_nodes.select(col("node"), lit(1.0).alias("rank"))
    
    # PageRank iterations
    print(f"[INFO] Démarrage de {args.iterations} itérations PageRank...")
    iteration_times = []
    
    for i in range(args.iterations):
        iter_start = time.time()
        
        # Compute contributions from nodes with outlinks
        contribs = adj.join(ranks, adj.src == ranks.node, "inner") \
            .select(
                explode("outlinks").alias("dst"),
                (col("rank") / size("outlinks")).alias("contrib")
            )
        
        # Aggregate contributions
        new_ranks = contribs.groupBy("dst").sum("contrib") \
            .select(
                col("dst").alias("node"),
                (lit(0.15) + lit(0.85) * col("sum(contrib)")).alias("rank")
            )
        
        # Merge with all nodes to avoid losing nodes without incoming links
        ranks = all_nodes.join(new_ranks, "node", "left") \
            .select(
                col("node"),
                coalesce(col("rank"), lit(0.15)).alias("rank")
            )
        
        iter_time = time.time() - iter_start
        iteration_times.append(iter_time)
        print(f"[INFO] Itération {i+1}/{args.iterations} terminée en {iter_time:.2f}s")
    
    total_time = time.time() - start_time
    
    # Save PageRank results with header
    print(f"[INFO] Sauvegarde des résultats dans : {args.output}/ranks")
    ranks.orderBy(col("rank").desc()) \
        .write.mode("overwrite").option("header", "true").csv(f"{args.output}/ranks")
    
    # Save top 100 for quick reference
    print(f"[INFO] Sauvegarde du top 100 dans : {args.output}/top100")
    top_100 = ranks.orderBy(col("rank").desc()).limit(100)
    top_100.write.mode("overwrite").option("header", "true").csv(f"{args.output}/top100")
    
    # Show top 10 in logs
    print("[INFO] Top 10 pages par PageRank :")
    top_100.show(10, truncate=False)
    
    # Save metadata
    metadata = {
        "input_path": args.input,
        "output_path": args.output,
        "num_iterations": args.iterations,
        "num_nodes": num_nodes,
        "num_links": num_links,
        "total_lines": total_lines,
        "parsed_lines": non_empty_parsed,
        "wikilink_lines": wikilink_count,
        "total_time_seconds": round(total_time, 2),
        "avg_iteration_time_seconds": round(sum(iteration_times) / len(iteration_times), 2),
        "iteration_times": [round(t, 2) for t in iteration_times]
    }
    
    print(f"[INFO] Métadonnées : {json.dumps(metadata, indent=2)}")
    
    # Save metadata as JSON in GCS
    metadata_path = f"{args.output}/metadata.json"
    metadata_rdd = spark.sparkContext.parallelize([json.dumps(metadata, indent=2)])
    metadata_rdd.saveAsTextFile(metadata_path)
    
    print(f"[INFO] Métadonnées sauvegardées dans : {metadata_path}")
    print(f"[SUCCESS] PageRank terminé en {total_time:.2f}s avec {num_nodes} nœuds et {num_links} liens")
    
    spark.stop()

if __name__ == "__main__":
    main()