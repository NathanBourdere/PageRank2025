import bz2
import hashlib
from collections import defaultdict
import sys
import os
import time
from tqdm import tqdm

def hash_uri(uri, num_partitions=10):
    """Hash cohérent pour assigner une URI à une partition"""
    return int(hashlib.md5(uri.encode('utf-8')).hexdigest(), 16) % num_partitions

def estimate_lines(input_file):
    """Estime le nombre de lignes dans le fichier compressé"""
    print("📊 Estimation de la taille du fichier...")
    
    # Lire un échantillon pour estimer
    sample_lines = 0
    sample_bytes = 0
    max_sample = 10 * 1024 * 1024  # 10 MB échantillon
    
    with bz2.open(input_file, 'rb') as f:
        while sample_bytes < max_sample:
            chunk = f.read(1024 * 1024)  # 1 MB à la fois
            if not chunk:
                break
            sample_bytes += len(chunk)
            sample_lines += chunk.count(b'\n')
    
    # Estimer le total basé sur la taille du fichier
    file_size = os.path.getsize(input_file)
    if sample_bytes > 0:
        estimated_lines = int((file_size / sample_bytes) * sample_lines)
        return estimated_lines
    return None

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
    start_time = time.time()
    
    print(f"🚀 Traitement de {input_file}")
    print(f"   Échantillonnage: {sample_rate*100}%, Partitions: {num_partitions}")
    
    # Estimation du nombre de lignes
    estimated_lines = estimate_lines(input_file)
    if estimated_lines:
        print(f"   Lignes estimées: {estimated_lines:,}")
        print(f"   Temps estimé: {estimated_lines / 100000 * 1.5:.1f} secondes\n")
    
    try:
        # Ouvrir les fichiers de sortie
        for i in range(num_partitions):
            partition_files[i] = open(f"{output_prefix}_partition_{i}.ttl", 'w', encoding='utf-8')
        
        # Barre de progression
        pbar = tqdm(
            total=estimated_lines if estimated_lines else None,
            desc="Traitement",
            unit=" lignes",
            unit_scale=True,
            smoothing=0.1
        )
        
        with bz2.open(input_file, 'rt', encoding='utf-8') as f:
            current_subject = None
            subject_partition = None
            subject_lines = []
            last_update = time.time()
            
            for line in f:
                total_triples += 1
                pbar.update(1)
                
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
                
                # Mise à jour de la description toutes les secondes
                current_time = time.time()
                if current_time - last_update > 1.0:
                    rate = total_triples / (current_time - start_time)
                    pbar.set_postfix({
                        'sujets': len(sampled_subjects),
                        'vitesse': f'{rate:.0f} l/s'
                    })
                    last_update = current_time
            
            # Traiter le dernier sujet
            if current_subject and subject_lines and current_subject in sampled_subjects:
                partition_files[subject_partition].writelines(subject_lines)
        
        pbar.close()
    
    finally:
        # Fermer tous les fichiers
        for f in partition_files.values():
            f.close()
    
    # Calcul du temps écoulé
    elapsed_time = time.time() - start_time
    
    print(f"\n✅ Terminé en {elapsed_time:.1f} secondes ({elapsed_time/60:.1f} minutes)")
    print(f"   Vitesse moyenne: {total_triples/elapsed_time:.0f} lignes/seconde")
    print(f"\n📈 Statistiques:")
    print(f"   Total triplets traités: {total_triples:,}")
    print(f"   Sujets échantillonnés: {len(sampled_subjects):,}")
    print(f"   Taux réel: {len(sampled_subjects)/total_triples*100:.2f}%")
    
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
