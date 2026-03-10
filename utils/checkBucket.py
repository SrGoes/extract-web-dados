
from datetime import datetime
from utils.conection import get_bucket

def should_download(gcs_full_path: str) -> bool:
	"""
	Verifica se o arquivo deve ser baixado:
	- Se já existe no bucket e NÃO é do ano atual, retorna False (pula)
	- Se não existe ou é do ano atual, retorna True (baixa)
	Parâmetro:
		gcs_full_path: caminho completo do arquivo no bucket (ex: CPTM/bruto/2025/arquivo.pdf)
	"""
	import logging
	log = logging.getLogger("nyx.checkbucket")
	bucket = get_bucket()
	ano_atual = str(datetime.now().year)
	parts = gcs_full_path.split('/')
	if len(parts) < 4:
		log.warning(f"[checkbucket] Caminho inesperado: {gcs_full_path}")
		return True
	site, camada, ano = parts[0], parts[1], parts[2]
	# Checa se existe algum blob na pasta do ano
	prefix = f"{site}/{camada}/{ano}/"
	blobs = list(bucket.list_blobs(prefix=prefix, max_results=1))
	exists = len(blobs) > 0
	if exists:
		if ano == ano_atual:
			log.info(f"[checkbucket] Ano atual, será baixado: {gcs_full_path}")
			return True
		log.info(f"[checkbucket] Pasta do ano já existe, será pulado: {gcs_full_path}")
		return False
	log.info(f"[checkbucket] Pasta do ano não existe, será baixado: {gcs_full_path}")
	return True
