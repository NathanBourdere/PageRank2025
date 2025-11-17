# PageRank 2025-2026 — Nathan B.

## Objectif
Comparer performances PageRank RDD vs DataFrame sur Dataproc (2,4,6 nœuds). Données : DBpedia wikilinks (2022.12.01). Voir NSDI (RDD partitioning/persist) et docs Spark.   
- Aligne partitions des RDDs qui vont être joints: `partitionBy(N)` sur les deux datasets pour éviter un shuffle global. (implémentation RDD permit partition control). :contentReference[oaicite:8]{index=8}  
- Pré-agrège côté map : `map` → `reduceByKey` (RDD) ou `groupBy().agg()` mais en minimisant les `groupBy` multiples (DF). :contentReference[oaicite:9]{index=9}  
- Broadcast-join si un des inputs est petit (DataFrame). :contentReference[oaicite:10]{index=10}

---

# 5) Mesures à collecter & comment les obtenir
- **Wall-clock** : `time` autour du job (nous imprimons timings dans les scripts).  
- **Shuffle bytes / stages** : via Spark UI (History Server) ou logs `spark.eventLog` (enable `spark.eventLog.enabled=true` si besoin).  
- **Coût estimé** : calculer vCPU * hours * prix Dataproc + VM cost; capture des heures d’exécution (start/stop) et multiplier par prix région.  
- **Top entity (centre Wikipedia)** : résultat `takeOrdered(1)` ou `orderBy(desc("rank")).limit(1)` — inscrit dans README.

---

# 6) Plan d’expérimentation (ordre pour économiser crédit)
1. Local / petit sample (10%) : itérer et déboguer les scripts.  
2. Small cluster 2-nœuds (master+1 worker) sur sample : valider timings, mesurer shuffle bytes.  
3. Runs 4-nodes et 6-nodes sur sample (identiques hardware).  
4. Une fois tout réglé, exécuter **au plus une** passe sur 100% des données (si budget le permet). Sinon, scaler sample à 25/50% pour confirmation.  
5. Supprimer cluster immédiatement après chaque run.

---

# 7) Notes pratiques & budget
- **Utiliser preemptible workers** pour baisser coût (workers préemptés = tâches ré-exécutées) — utile pour runs longs et non-interactifs.  
- Toujours **supprimer** le cluster après chaque série de runs pour éviter facturation continue.  
- Stockage GCS (2GB) est négligeable ; l’essentiel du coût vient des VM & Dataproc fee. (voir pricing Dataproc pour ta région).  

# Références
- Zaharia et al., *Resilient Distributed Datasets: A Fault-Tolerant Abstraction for In-Memory Cluster Computing*, NSDI 2012. :contentReference[oaicite:11]{index=11}  
- Spark RDD Programming Guide. :contentReference[oaicite:12]{index=12}  
- PySpark Quickstart DataFrame. :contentReference[oaicite:13]{index=13}