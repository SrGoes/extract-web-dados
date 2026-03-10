
def download_bruto_ano(site_name: str, ano: str, local_dir: Path, layer: str = "bruto") -> int:
    """
    Baixa todos os arquivos brutos de um ano do bucket para o diretório local temporário.
    Parâmetros:
        site_name: nome do site (ex: "CPTM")
        ano: ano desejado (ex: "2025")
        local_dir: diretório local temporário para salvar os arquivos
        layer: camada do medalhão (default: "bruto")
    Retorna a quantidade de arquivos baixados.
    """
    bucket = get_bucket()
    prefix = f"{site_name}/{layer}/{ano}/"
    blobs = list(bucket.list_blobs(prefix=prefix))
    baixados = 0
    for blob in blobs:
        # Monta caminho local de destino
        rel_path = Path(blob.name).relative_to(f"{site_name}/{layer}")
        dest_path = local_dir / rel_path
        dest_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            blob.download_to_filename(str(dest_path))
            log.info(f"  ⬇ Baixado do bucket: {blob.name} → {dest_path}")
            baixados += 1
        except Exception as e:
            log.error(f"  ✗ Erro ao baixar {blob.name}: {e}")
    return baixados
"""
Nyx — Módulo de storage (Google Cloud Storage).

Gerencia o upload de arquivos para o bucket GCS,
seguindo a estrutura de medalhão:
  <site>/stage/   — dados brutos coletados
  <site>/bronze/  — dados limpos
  <site>/silver/  — (futuro) dados transformados
  <site>/gold/    — (futuro) dados prontos para consumo
"""

import logging
from pathlib import Path

import json
import tempfile
from google.cloud import storage

from config import GCS_BUCKET_NAME
from utils.conection import get_bucket

log = logging.getLogger("nyx.storage")




def upload_file(local_path: Path, gcs_path: str) -> bool:
    """
    Faz upload de um arquivo local para o bucket GCS.

    Parâmetros:
        local_path: caminho do arquivo local
        gcs_path:   caminho de destino no bucket (ex: "CPTM/stage/2019/arquivo.xlsx")

    Retorna True se deu certo, False se deu erro.
    """
    try:
        bucket = get_bucket()
        blob = bucket.blob(gcs_path)
        blob.upload_from_filename(str(local_path))
        log.info("  ☁ Enviado: %s → gs://%s/%s", local_path.name, GCS_BUCKET_NAME, gcs_path)
        return True
    except Exception as e:
        log.error("  ✗ Erro ao enviar %s: %s", local_path.name, e)
        return False


def upload_directory(local_dir: Path, site_name: str, layer: str = "bruto") -> int:
    """
    Faz upload de todos os arquivos de um diretório local para o bucket GCS,
    preservando a estrutura de subpastas.

    Parâmetros:
        local_dir:  pasta local com os arquivos processados
        site_name:  nome do site (ex: "CPTM")
        layer:      camada do medalhão (stage, bronze, silver, gold)

    Retorna a quantidade de arquivos enviados com sucesso.
    """
    uploaded = 0
    for file_path in local_dir.rglob("*"):
        if not file_path.is_file():
            continue

        relative = file_path.relative_to(local_dir)
        gcs_path = f"{site_name}/{layer}/{relative.as_posix()}"

        if upload_file(file_path, gcs_path):
            uploaded += 1

    log.info("  ☁ %d arquivo(s) enviado(s) para gs://%s/%s/%s/",
             uploaded, GCS_BUCKET_NAME, site_name, layer)
    return uploaded
