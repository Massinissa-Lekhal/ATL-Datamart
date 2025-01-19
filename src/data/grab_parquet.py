from minio import Minio
import urllib.request
import pandas as pd
import sys
import os 
import datetime as datetime 

from minio import Minio
import os

def main():
    #grab_data()
    write_data_minio()
    

def grab_data() -> None:
    """Grab the data from New York Yellow Taxi

    This method download x files of the New York Yellow Taxi. 
    
    Files need to be saved into "../../data/raw" folder
    This methods takes no arguments and returns nothing.
    """
    
    
    raw_url = r'C:\Users\massi\Documents\ATL-Datamart\data\raw'
    download_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/"

    # Vérification et création du dossier raw si nécessaire
    try:
        os.listdir(raw_url)
        print("Le dossier 'raw' existe.")
    except Exception as e:
        print(f"Erreur : {e}")
        os.makedirs(raw_url)

    # Spécifiez l'année
    year = (datetime.datetime.now().year)-1

    # Boucle pour télécharger les fichiers des mois 1 et 2
    for month in range(1, 3):  # Changez la plage ici pour limiter à 2 mois
        month_str = f"{month:02}"  
        file_name = f"yellow_tripdata_{year}-{month_str}.parquet"
        file_url = f"{download_url}{file_name}"
        local_path = os.path.join(raw_url, file_name)

        # Vérifie si le fichier parquet est déjà téléchargé
        if os.path.exists(local_path):
            print(f"{file_name} existe déjà. Téléchargement ignoré.")
            continue

        # Téléchargement du fichier parquet 
        try:
            print(f"Téléchargement de {file_name}...")
            urllib.request.urlretrieve(file_url, local_path)
            print(f"{file_name} téléchargé avec succès et enregistré dans {raw_url}.")
        except Exception as e:
            print(f"Erreur lors du téléchargement de {file_name}: {e}")


def write_data_minio():
    """
    Cette méthode envoie tous les fichiers Parquet du dossier 'raw' vers MinIO.
    """
    client = Minio(
        "localhost:9000",
        secure=False,
        access_key="minio",
        secret_key="minio123"
    )
    bucket: str = "yellowtaxibucket"
    found = client.bucket_exists(bucket)
    if not found:
        client.make_bucket(bucket)
    else:
        print("Bucket " + bucket + " existe déjà")
    raw_url = r'C:\Users\massi\Documents\ATL-Datamart\data\raw'
    try:
        files = os.listdir(raw_url)
        parquet_files = [f for f in files if f.endswith(".parquet")]
        
        # verifier si ya un fichier parquet 
        if not parquet_files:
            print("Aucun fichier Parquet trouvé dans le dossier 'raw'.")
            return
        
        #télécharger les  fichiers dans  MinIO
        for file_name in parquet_files:
            file_path = os.path.join(raw_url, file_name)
            print(f"Téléchargement de {file_name} vers MinIO...")
            client.fput_object(bucket, file_name, file_path)
            print(f"{file_name} uploadé avec succès dans le bucket '{bucket}'.")
    except Exception as e:
        print(f"Erreur lors de la lecture du dossier ou de l'upload : {e}")




if __name__ == '__main__':
    sys.exit(main())
