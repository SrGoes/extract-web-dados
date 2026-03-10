
from utils.conection import get_bucket
from config import GCS_BUCKET_NAME

def test_conection():
    bucket = get_bucket()
    blobs = list(bucket.list_blobs(max_results=3))
    print(f"Conexão bem-sucedida com o bucket: {GCS_BUCKET_NAME}")
    print("Alguns arquivos encontrados:")
    for blob in blobs:
        print(f"- {blob.name}")

if __name__ == "__main__":
    test_conection()
