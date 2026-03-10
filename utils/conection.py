import os
import json
import tempfile
from google.cloud import storage
from config import GCS_BUCKET_NAME
from utils.decode import decodificacao

_client = None
_bucket = None

def get_bucket():
	"""
	Retorna o bucket GCS autenticado usando a chave do .env.
	Reutiliza a conexão para evitar múltiplas autenticações.
	"""
	global _client, _bucket
	if _bucket is None:
		encoded_key = os.environ["SECRET_GCP_ENV"]
		creds_json = json.loads(decodificacao(encoded_key))
		tmp = tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False)
		json.dump(creds_json, tmp)
		tmp.close()
		_client = storage.Client.from_service_account_json(tmp.name)
		os.unlink(tmp.name)
		_bucket = _client.bucket(GCS_BUCKET_NAME)
	return _bucket
