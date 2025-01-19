import gc
import os
from io import BytesIO
import sys
import pandas as pd
from sqlalchemy import create_engine
from minio import Minio

def get_minio_client():
    """ Crée un client MinIO pour interagir avec le serveur MinIO. """
    try:
        client = Minio(
            "localhost:9000",  # Changez l'adresse si nécessaire
            secure=False,
            access_key="minio",  # Remplacez avec votre clé d'accès
            secret_key="minio123"  # Remplacez avec votre clé secrète
        )
        print("Client MinIO créé avec succès !")
        return client
    except Exception as e:
        print(f"Erreur lors de la création du client MinIO : {e}")
        raise

def list_parquet_files(minio_client, bucket_name):
    """ Liste les fichiers Parquet dans un bucket MinIO. """
    try:
        files = [
            obj.object_name for obj in minio_client.list_objects(bucket_name)
            if obj.object_name.lower().endswith('.parquet')
        ]
        print(f"Fichiers Parquet trouvés dans le bucket '{bucket_name}' : {files}")
        return files
    except Exception as e:
        print(f"Erreur lors de la liste des fichiers dans le bucket {bucket_name} : {e}")
        raise

def get_parquet_dataframe(minio_client, bucket_name, object_name):
    """ Récupère un fichier Parquet depuis MinIO et le charge dans un DataFrame Pandas. """
    try:
        # Télécharger les données du fichier
        response = minio_client.get_object(bucket_name, object_name)
        raw_data = response.read()

        # Charger les données dans un DataFrame
        with BytesIO(raw_data) as buffer:
            dataframe = pd.read_parquet(buffer, engine='pyarrow')
        
        print(f"Fichier {object_name} chargé dans un DataFrame avec succès !")
        print(dataframe.head())  # Affiche les 5 premières lignes du DataFrame

        return dataframe
    except Exception as e:
        print(f"Erreur lors de la récupération ou du chargement du fichier {object_name} : {e}")
        raise

def write_data_postgres(dataframe: pd.DataFrame, table_name: str) -> bool:
    """ Enregistre un DataFrame dans une table PostgreSQL. """
    db_config = {
        "dbms_engine": "postgresql",
        "dbms_username": "postgres",
        "dbms_password": "admin",
        "dbms_ip": "localhost",
        "dbms_port": "15432",
        "dbms_database": "nyc_warehouse",
    }

    db_config["database_url"] = (
        f"{db_config['dbms_engine']}://{db_config['dbms_username']}:{db_config['dbms_password']}@"
        f"{db_config['dbms_ip']}:{db_config['dbms_port']}/{db_config['dbms_database']}"
    )
    
    try:
        # Connexion à la base de données
        engine = create_engine(db_config["database_url"])
        with engine.connect():
            print("Connexion à PostgreSQL réussie !")
            # Écriture des données dans PostgreSQL
            dataframe.to_sql(table_name, engine, index=False, if_exists='append')
        print(f"Les données ont été écrites dans la table {table_name} avec succès.")
        return True
    except Exception as e:
        print(f"Erreur lors de la connexion ou de l'écriture dans PostgreSQL : {e}")
        return False

def clean_column_name(dataframe: pd.DataFrame) -> pd.DataFrame:
    """ Nettoie les noms des colonnes du DataFrame en les mettant en minuscules et en remplaçant les espaces par des underscores. """
    try:
        dataframe.columns = [col.replace(" ", "_").lower() for col in dataframe.columns]
        print("Les noms des colonnes ont été nettoyés avec succès !")
        return dataframe
    except Exception as e:
        print(f"Erreur lors du nettoyage des noms des colonnes : {e}")
        raise

def process_parquet_files(minio_client, bucket_name, table_name):
    """ Récupère, nettoie et enregistre les fichiers Parquet dans PostgreSQL. """
    try:
        # Liste des fichiers Parquet
        parquet_files = list_parquet_files(minio_client, bucket_name)

        for file_name in parquet_files:
            try:
                # Charger le fichier dans un DataFrame
                dataframe = get_parquet_dataframe(minio_client, bucket_name, file_name)

                # Nettoyer les colonnes
                dataframe = clean_column_name(dataframe)

                # Écrire dans PostgreSQL
                if not write_data_postgres(dataframe, table_name):
                    print(f"Échec de l'importation du fichier {file_name} dans PostgreSQL")

            except Exception as e:
                print(f"Erreur lors du traitement du fichier {file_name} : {e}")
            finally:
                # Libérer la mémoire
                del dataframe
                gc.collect()

    except Exception as e:
        print(f"Erreur lors du traitement des fichiers Parquet dans le bucket {bucket_name} : {e}")
        raise

def main() -> None:
    """ Programme principal : lit les fichiers Parquet depuis MinIO, nettoie les données et les envoie à PostgreSQL. """
    try:
        bucket_name = "yellowtaxibucket"  # Nom du bucket MinIO
        table_name = "nyc_raw"  # Nom de la table PostgreSQL

        # Initialiser le client MinIO
        minio_client = get_minio_client()

        # Traiter les fichiers Parquet
        process_parquet_files(minio_client, bucket_name, table_name)
        
    except Exception as e:
        print(f"Une erreur est survenue dans le processus principal : {e}")

if __name__ == '__main__':
    try:
        sys.exit(main())
    except Exception as e:
        print(f"Erreur inattendue lors de l'exécution du programme : {e}")
