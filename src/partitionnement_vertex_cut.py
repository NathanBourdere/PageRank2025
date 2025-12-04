import bz2
import hashlib
from collections import defaultdict
import sys

def hash_uri(uri, num_partitions=10):
    """Hash cohérent pour assigner une URI à une partition"""
    return int(hashlib.md5(uri.encode('utf-8')).hexdigest(), 16) % num_partitions

def partition_ttl_stream(input_file, output_prefix, sample_rate=0.1, num_partitions=10):
    """
    Partitionne un fichier TTL en gardant les arêtes sortantes groupées.
    
    Args:
        input_file: Chemin vers le fichier .ttl.bz2
        output_prefix: Préfixe pour les fichiers de sortie
        sample_rate: Taux d'échantillonnage (0.1 = 10%)
        num_partitions: Nombre de partitions
    """
    
    # Buffers pour chaque partition
    partition_buffers = defaultdict(list)
    partition_files = {}
    
    # Statistiques
    total_triples = 0
    sampled_subjects = set()
    
    print(f"Traitement de {input_file}...")
    print(f"Échantillonnage: {sample_rate*100}%, Partitions: {num_partitions}")
    
    try:
        # Ouvrir les fichiers de sortie
        for i in range(num_partitions):
            partition_files[i] = open(f"{output_prefix}_partition_{i}.ttl", 'w', encoding='utf-8')
        
        with bz2.open(input_file, 'rt', encoding='utf-8') as f:
            current_subject = None
            subject_partition = None
            subject_lines = []
            
            for line in f:
                total_triples += 1
                
                # Ignorer les commentaires et lignes vides
                if line.startswith('#') or line.strip() == '':
                    continue
                
                # Détecter un nouveau sujet (ligne commence par <http...)
                if line.startswith('<'):
                    # Traiter le sujet précédent si existe
                    if current_subject and subject_lines:
                        if current_subject in sampled_subjects:
                            partition_files[subject_partition].writelines(subject_lines)
                    
                    # Extraire le nouveau sujet
                    parts = line.split(None, 1)
                    if parts:
                        current_subject = parts[0].strip('<>')
                        subject_partition = hash_uri(current_subject, num_partitions)
                        
                        # Décider si on échantillonne ce sujet (basé sur son hash)
                        subject_hash = hash_uri(current_subject, 100)
                        if subject_hash < sample_rate * 100:
                            sampled_subjects.add(current_subject)
                            subject_lines = [line]
                        else:
                            subject_lines = []
                else:
                    # Continuation du sujet actuel
                    if current_subject in sampled_subjects:
                        subject_lines.append(line)
                
                # Affichage progression
                if total_triples % 100000 == 0:
                    print(f"Traité {total_triples:,} triplets, "
                          f"{len(sampled_subjects):,} sujets échantillonnés")
            
            # Traiter le dernier sujet
            if current_subject and subject_lines and current_subject in sampled_subjects:
                partition_files[subject_partition].writelines(subject_lines)
    
    finally:
        # Fermer tous les fichiers
        for f in partition_files.values():
            f.close()
    
    print(f"\n✓ Terminé!")
    print(f"Total triplets traités: {total_triples:,}")
    print(f"Sujets échantillonnés: {len(sampled_subjects):,}")
    print(f"Taux réel: {len(sampled_subjects)/total_triples*100:.2f}%")
    
    # Afficher la taille de chaque partition
    print("\nDistribution des partitions:")
    for i in range(num_partitions):
        filename = f"{output_prefix}_partition_{i}.ttl"
        try:
            size = sum(1 for _ in open(filename))
            print(f"  Partition {i}: {size:,} lignes")
        except:
            print(f"  Partition {i}: erreur lecture")

# Version optimisée pour GCP avec streaming
def partition_to_gcs(input_file, gcs_bucket, gcs_prefix, sample_rate=0.1, num_partitions=10):
    """
    Version pour Google Cloud Storage (nécessite google-cloud-storage)
    """
    from google.cloud import storage
    
    client = storage.Client()
    bucket = client.bucket(gcs_bucket)
    
    # Même logique mais écriture vers GCS
    # ... (adaptez la fonction précédente)
    pass

if __name__ == "__main__":
    # Exemple d'utilisation
    input_file = "wikilinks_lang=en.ttl.bz2"
    output_prefix = "wikilinks_sampled"
    
    partition_ttl_stream(
        input_file=input_file,
        output_prefix=output_prefix,
        sample_rate=0.1,  # 10%
        num_partitions=10
    )
    
    print("\n📊 Les fichiers peuvent maintenant être traités en parallèle")
    print("   sans shuffle pour PageRank ou calcul de voisins!")