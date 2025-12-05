#!/usr/bin/env python3
import argparse
import time
import json
import re
from pyspark.sql import SparkSession

def parse_ttl_line(line):
    """
    Parse une ligne TTL format: <src> <pred> <dst> .
    Retourne (src, dst) si pred == wikiPageWikiLink, sinon None
    """
    # Regex pour extraire les 3 URIs entre < >
    match = re.match(r'<([^>]+)>\s+<([^>]+)>\s+<([^>]+)>', line)
    if not match:
        return None
    
    src_full, pred_full, dst_full = match.groups()
    
    # Filtrer uniquement wikiPageWikiLink
    if pred_full != "http://dbpedia.org/ontology/wikiPageWikiLink":
        return None
    
    # Extraire la dernière partie de l'URI (après le dernier /)
    src = src_full.split('/')[-1]
    dst = dst_full.split('/')[-1]
    
    if src and dst:  # S'assurer que les noms ne sont pas vides
        return (src, dst)
    return None

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--iterations", type=int, default=10)
    args = parser.parse_args()

    start_time = time.time()
    
    spark = SparkSession.builder.appName("PageRankRDD").getOrCreate()
    sc = spark.sparkContext

    print(f"[INFO] Lecture du fichier : {args.input}")
    lines = sc.textFile(args.input)
    
    total_lines = lines.count()
    print(f"[INFO] Nombre total de lignes lues : {total_lines}")
    
    # Debug: afficher quelques lignes
    sample_lines = lines.take(5)
    print(f"[DEBUG] Échantillon de lignes :")
    for i, line in enumerate(sample_lines, 1):
        print(f"  {i}. {line[:100]}...")

    # Parser les lignes TTL
    edges = lines.map(parse_ttl_line).filter(lambda x: x is not None).distinct().cache()
    
    num_links = edges.count()
    print(f"[INFO] Nombre de liens wikiPageWikiLink uniques : {num_links}")
    
    if num_links == 0:
        print("[ERROR] Aucun lien wikiPageWikiLink trouvé après parsing !")
        spark.stop()
        return
    
    # Debug: afficher quelques liens parsés
    sample_edges = edges.take(5)
    print(f"[DEBUG] Échantillon de liens parsés :")
    for i, (src, dst) in enumerate(sample_edges, 1):
        print(f"  {i}. {src} -> {dst}")

    # Tous les nœuds (sources + destinations)
    print("[INFO] Identification de tous les nœuds...")
    nodes = edges.flatMap(lambda x: [x[0], x[1]]).distinct().cache()
    num_nodes = nodes.count()
    print(f"[INFO] Nombre de nœuds uniques : {num_nodes}")

    # Initialiser les ranks pour tous les nœuds
    ranks = nodes.map(lambda n: (n, 1.0))

    # Construire adjacency list : (src, [dst1, dst2, ...])
    print("[INFO] Construction de la liste d'adjacence...")
    links = edges.groupByKey().mapValues(list).cache()
    num_nodes_with_outlinks = links.count()
    print(f"[INFO] Nœuds avec liens sortants : {num_nodes_with_outlinks}")

    # PageRank iterations
    print(f"[INFO] Démarrage de {args.iterations} itérations PageRank...")
    iteration_times = []
    
    for i in range(args.iterations):
        iter_start = time.time()
        
        # Calculer les contributions des nœuds avec outlinks
        contribs = links.join(ranks).flatMap(
            lambda x: [(dst, x[1][1] / len(x[1][0])) for dst in x[1][0]]
        )
        
        # Agréger les contributions par nœud destination
        new_ranks = contribs.reduceByKey(lambda x, y: x + y)
        
        # CRITICAL FIX: Fusionner avec tous les nœuds pour éviter de perdre
        # les nœuds sans liens entrants (dangling nodes)
        # Stratégie: leftOuterJoin avec tous les nœuds
        ranks = nodes.map(lambda n: (n, None)) \
            .leftOuterJoin(new_ranks) \
            .mapValues(lambda x: 0.15 + 0.85 * x[1] if x[1] is not None else 0.15)
        
        iter_time = time.time() - iter_start
        iteration_times.append(iter_time)
        print(f"[INFO] Itération {i+1}/{args.iterations} terminée en {iter_time:.2f}s")
    
    total_time = time.time() - start_time
    
    # Trier par rank décroissant
    print("[INFO] Tri des résultats par PageRank...")
    sorted_ranks = ranks.sortBy(lambda x: x[1], ascending=False).cache()
    
    # Afficher le top 10 dans les logs
    top_10 = sorted_ranks.take(10)
    print("[INFO] Top 10 pages par PageRank :")
    for i, (node, rank) in enumerate(top_10, 1):
        print(f"  {i}. {node}: {rank:.6f}")
    
    # Sauvegarder tous les résultats avec header
    print(f"[INFO] Sauvegarde des résultats dans : {args.output}/ranks")
    sorted_ranks.map(lambda x: f"{x[0]},{x[1]}") \
        .coalesce(1) \
        .saveAsTextFile(f"{args.output}/ranks")
    
    # Sauvegarder le header séparément (workaround pour CSV avec RDD)
    sc.parallelize(["node,rank"]).coalesce(1).saveAsTextFile(f"{args.output}/header")
    
    # Sauvegarder le top 100
    print(f"[INFO] Sauvegarde du top 100 dans : {args.output}/top100")
    sc.parallelize(sorted_ranks.take(100)) \
        .map(lambda x: f"{x[0]},{x[1]}") \
        .coalesce(1) \
        .saveAsTextFile(f"{args.output}/top100")
    
    # Sauvegarder les métadonnées
    metadata = {
        "input_path": args.input,
        "output_path": args.output,
        "num_iterations": args.iterations,
        "num_nodes": num_nodes,
        "num_links": num_links,
        "nodes_with_outlinks": num_nodes_with_outlinks,
        "total_lines": total_lines,
        "total_time_seconds": round(total_time, 2),
        "avg_iteration_time_seconds": round(sum(iteration_times) / len(iteration_times), 2),
        "iteration_times": [round(t, 2) for t in iteration_times],
        "top_10": [{"node": node, "rank": round(rank, 6)} for node, rank in top_10]
    }
    
    print(f"[INFO] Métadonnées : {json.dumps(metadata, indent=2)}")
    
    metadata_json = json.dumps(metadata, indent=2)
    sc.parallelize([metadata_json]).coalesce(1).saveAsTextFile(f"{args.output}/metadata.json")
    
    print(f"[INFO] Métadonnées sauvegardées dans : {args.output}/metadata.json")
    print(f"[SUCCESS] PageRank terminé en {total_time:.2f}s avec {num_nodes} nœuds et {num_links} liens")
    
    spark.stop()

if __name__ == "__main__":
    main()