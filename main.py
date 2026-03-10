"""
Nyx Crawler — Dispatcher principal (o "chefe" do projeto).

Este arquivo é o ponto de partida do programa. Ele:
  1. Lê a lista de sites que devem ser coletados (definidos em config.py)
  2. Verifica quais arquivos já existem no bucket (apenas ano atual é baixado obrigatoriamente)
  3. Chama o crawler específico para baixar os arquivos
  4. Processa para nível bronze usando os modelos (models/) 
  5. Faz upload dos arquivos brutos E bronze para o bucket GCS
  6. Mostra um resumo no final com o que foi coletado
"""


import time
import tempfile
import logging
from pathlib import Path
import requests
import re
import pandas as pd
import sys

from config import SITES, HEADERS, LOG_NIVEL_PRINCIPAL
from cptm.app import CptmCrawler
from utils.storage import upload_directory, upload_file
from utils.checkBucket import should_download

# ---------------------------------------------------------------------------
# Mapeamento de modelos para processamento bronze
# Cada modelo processa um tipo de arquivo específico para nível bronze
# ---------------------------------------------------------------------------
BRONZE_MODELS = {
    "CPTM": {
        ".pdf": "model.cptm_pdf.CPTMPDFExtractor",
        ".xlsx": "model.cptm_xlsx.CPTMXLSXExtractor",
    },
    # Para adicionar novo site:
    # "TVM": {
    #     ".pdf": "model.tvm_pdf.TVMPDFExtractor",
    # },
}

# ---------------------------------------------------------------------------
# Sessão HTTP compartilhada
# ---------------------------------------------------------------------------
SESSION = requests.Session()
SESSION.headers.update(HEADERS)



# Configuração de logging para arquivo e console
log = logging.getLogger("nyx")
log.setLevel(logging.INFO)

# Remove handlers antigos para evitar duplicidade
if log.hasHandlers():
    log.handlers.clear()

formatter = logging.Formatter('%(asctime)s | %(levelname)-8s | %(message)s', datefmt='%H:%M:%S')


# Handler para arquivo
file_handler = logging.FileHandler("nyx_run.log", encoding="utf-8")
file_handler.setFormatter(formatter)
file_handler.setLevel(getattr(logging, LOG_NIVEL_PRINCIPAL.upper(), logging.INFO))
log.addHandler(file_handler)

# Handler para console (evita duplicidade)
if not any(isinstance(h, logging.StreamHandler) for h in log.handlers):
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    console_handler.setLevel(logging.INFO)
    log.addHandler(console_handler)

# Redireciona todo o terminal (stdout e stderr) para o log principal
class StreamToLogger:
    def __init__(self, logger, level):
        self.logger = logger
        self.level = level
        self.linebuf = ''
    def write(self, buf):
        for line in buf.rstrip().splitlines():
            self.logger.log(self.level, line.rstrip())
    def flush(self):
        pass

sys.stdout = StreamToLogger(log, logging.INFO)
sys.stderr = StreamToLogger(log, logging.ERROR)


# ---------------------------------------------------------------------------
# Mapeamento nome → instância do crawler
# ---------------------------------------------------------------------------
CRAWLER_MAP = {
    "CPTM": CptmCrawler(),
}


def get_crawler(nome: str):
    """
    Procura o crawler certo para uma empresa pelo nome.
    Retorna a instância do crawler ou None se não existir.
    """
    if nome in CRAWLER_MAP:
        return CRAWLER_MAP[nome]
    nome_lower = nome.lower()
    for key, crawler in CRAWLER_MAP.items():
        if key.lower() in nome_lower or nome_lower in key.lower():
            return crawler
    return None


def get_bronze_extractor(site_name: str, file_path: Path):
    """
    Retorna o extrator bronze apropriado para o arquivo com base no site e extensão.
    """
    site_bronze = BRONZE_MODELS.get(site_name.upper())
    if not site_bronze:
        return None
    
    ext = file_path.suffix.lower()
    extractor_path = site_bronze.get(ext)
    if not extractor_path:
        return None
    
    # Importa dinamicamente o extrator
    try:
        module_path, class_name = extractor_path.rsplit(".", 1)
        module = __import__(module_path, fromlist=[class_name])
        extractor_class = getattr(module, class_name)
        return extractor_class(str(file_path))
    except Exception as e:
        log.error(f"Erro ao carregar extrator {extractor_path}: {e}")
        return None


def process_bronze(site_name: str, raw_dir: Path, bronze_base_dir: Path) -> dict:
    """
    Processa arquivos brutos para nível bronze usando os modelos.
    Retorna dict com {ano: [arquivos_bronze_path]}
    """
    from utils.checkBucket import should_download
    from utils.storage import upload_file, download_bruto_ano
    import shutil
    bronze_files = {}
    if site_name.upper() not in BRONZE_MODELS:
        log.info(f"  Nenhum modelo bronze definido para {site_name}")
        return bronze_files

    anos_disponiveis = set()
    arquivos_bucket = {}
    # Sempre busca anos e arquivos do bucket se raw_dir for None
    if raw_dir is None:
        from utils.storage import get_bucket
        bucket = get_bucket()
        prefix = f"{site_name}/bruto/"
        blobs = list(bucket.list_blobs(prefix=prefix))
        for blob in blobs:
            parts = Path(blob.name).parts
            # Esperado: <site>/bruto/<ano>/arquivo
            if len(parts) >= 4 and re.match(r"20\d{2}", parts[2]):
                ano = parts[2]
                anos_disponiveis.add(ano)
                arquivos_bucket.setdefault(ano, []).append(blob)
        log.info(f"  [BRONZE] Anos detectados no bucket: {sorted(anos_disponiveis)}")
    # Novo fluxo: processa todos os anos e gera um único CSV
    dfs = []
    total_processados = 0
    total_pulados = 0
    for ano in sorted(anos_disponiveis):
        log.info(f"  --- Ano {ano} ---")
        # Baixa todos os arquivos brutos do ano do bucket para o local temporário
        tmp_bruto_dir = bronze_base_dir / site_name.upper() / "tmp_bruto" / ano
        tmp_bruto_dir.mkdir(parents=True, exist_ok=True)
        if raw_dir is not None:
            arquivos_ano = arquivos_bucket.get(ano, [])
            for file_path in arquivos_ano:
                import shutil
                shutil.copy(file_path, tmp_bruto_dir / file_path.name)
            n_baixados = len(arquivos_ano)
        else:
            n_baixados = download_bruto_ano(site_name.upper(), ano, tmp_bruto_dir, layer="bruto")
        log.info(f"  Arquivos brutos baixados para {ano}: {n_baixados}")
        if n_baixados == 0:
            log.warning(f"  Nenhum arquivo bruto encontrado no bucket para o ano {ano}.")
            continue
        arquivos_processados = 0
        arquivos_pulados = 0
        for file_path in tmp_bruto_dir.rglob("*"):
            if not file_path.is_file():
                continue
            extractor = get_bronze_extractor(site_name, file_path)
            if not extractor:
                arquivos_pulados += 1
                log.info(f"  Pulando (sem extrator bronze): {file_path.name}")
                continue
            try:
                log.info(f"  Processando bronze: {file_path.name}")
                result = extractor.extract_bronze()
                df = result.get("table")
                if df is None:
                    df = result.get("df")
                if df is None or (hasattr(df, 'empty') and df.empty):
                    arquivos_pulados += 1
                    log.warning(f"  Bronze vazio para: {file_path.name}")
                    continue
                dfs.append(df)
                arquivos_processados += 1
            except Exception as e:
                log.error(f"  Erro ao processar bronze {file_path.name}: {e}")
                arquivos_pulados += 1
        log.info(f"  Arquivos processados para bronze ({ano}): {arquivos_processados}")
        log.info(f"  Arquivos pulados ({ano}): {arquivos_pulados}")
        total_processados += arquivos_processados
        total_pulados += arquivos_pulados
        # Limpa arquivos temporários locais do bruto
        try:
            shutil.rmtree(tmp_bruto_dir)
        except Exception as e:
            log.warning(f"  Não foi possível remover diretório temporário: {e}")

    if not dfs:
        log.warning(f"  Nenhum dado bronze válido encontrado.")
        return {}

    # Gera único CSV bronze
    bronze_dir = bronze_base_dir / site_name.upper() / "bronze"
    bronze_dir.mkdir(parents=True, exist_ok=True)
    bronze_name = f"{site_name}.csv"
    bronze_path = bronze_dir / bronze_name
    df_bronze = pd.concat(dfs, ignore_index=True)
    df_bronze.to_csv(bronze_path, index=False, encoding='utf-8-sig')
    log.info(f"  Bronze consolidado salvo: {bronze_path}")

    # Upload para o bucket
    gcs_bronze_path = f"{site_name.upper()}/bronze/{bronze_name}"
    if upload_file(bronze_path, gcs_bronze_path):
        log.info(f"  Bronze consolidado enviado para o bucket: {gcs_bronze_path}")
        return {"all": [bronze_path]}
    else:
        log.error(f"  Falha ao enviar bronze consolidado para o bucket: {gcs_bronze_path}")
        return {}
        log.info(f"  Arquivos processados para bronze ({ano}): {arquivos_processados}")
        log.info(f"  Arquivos pulados ({ano}): {arquivos_pulados}")
        if not dfs:
            log.warning(f"  Nenhum dado bronze válido para o ano {ano}.")

        # Concatena todos os DataFrames do ano

        df_ano = pd.concat(dfs, ignore_index=True)
        df_ano.to_csv(bronze_path, index=False, encoding='utf-8-sig')
        log.info(f"  Bronze anual salvo: {bronze_path}")

        # Upload para o bucket (antes de remover o diretório)
        if upload_file(bronze_path, gcs_bronze_path):
            log.info(f"  Bronze anual enviado para o bucket: {gcs_bronze_path}")
            bronze_files[ano] = [bronze_path]
        else:
            log.error(f"  Falha ao enviar bronze anual para o bucket: {gcs_bronze_path}")

        # Limpa arquivos temporários locais do bronze e bruto
        try:
            shutil.rmtree(bronze_dir)
            shutil.rmtree(tmp_bruto_dir)
        except Exception as e:
            log.warning(f"  Não foi possível remover diretório temporário: {e}")

    return bronze_files


# ---------------------------------------------------------------------------
# Função principal
# ---------------------------------------------------------------------------
def main():
    log.info("=" * 60)
    log.info("NYX CRAWLER — Coleta e Processamento de Dados")
    log.info("=" * 60)

    sites = SITES
    log.info("Sites encontrados: %d", len(sites))
    for s in sites:
        log.info("  • %s → %s", s["nome"], s["url"])
    log.info("-" * 60)

    resultados: dict[str, int] = {}


    for site in sites:
        nome = site["nome"]
        url = site["url"]
        engine = site.get("engine", "bs4")

        crawler = get_crawler(nome)
        if crawler is None:
            log.warning("⚠ Sem crawler implementado para '%s' (engine: %s) — pulando.", nome, engine)
            resultados[nome] = 0
            continue

        log.info("")
        log.info(">>> Coletando: %s (engine: %s)", nome, crawler.engine)
        log.info("    URL: %s", url)

        # --- FLUXO 1: CRAWLER ---
        with tempfile.TemporaryDirectory(prefix=f"nyx_{nome}_crawler_") as tmp_dir:
            tmp_path = Path(tmp_dir)
            dest_dir = tmp_path / "bruto"
            dest_dir.mkdir(parents=True, exist_ok=True)

            log.info("    [CRAWLER] Baixando arquivos...")
            total = crawler.crawl(SESSION, url, dest_dir)

            uploaded_bruto = 0
            if total > 0:
                log.info("    [CRAWLER] ☁ Enviando %d arquivo(s) bruto(s) para o bucket...", total)
                uploaded_bruto = upload_directory(dest_dir, nome, layer="bruto")

            # Limpa arquivos locais do crawler
            try:
                import shutil
                shutil.rmtree(dest_dir)
            except Exception as e:
                log.warning(f"    [CRAWLER] Não foi possível remover diretório temporário: {e}")

        # --- FLUXO 2: BRONZE ---
        with tempfile.TemporaryDirectory(prefix=f"nyx_{nome}_bronze_") as tmp_dir:
            tmp_path = Path(tmp_dir)
            bronze_base_dir = tmp_path / "bronze"
            bronze_base_dir.mkdir(parents=True, exist_ok=True)

            log.info("    [BRONZE] Processando arquivos para nível bronze...")
            bronze_files = process_bronze(nome, None, bronze_base_dir)  # None para forçar busca no bucket

            uploaded_bronze = sum(len(arquivos) for arquivos in bronze_files.values())
            if uploaded_bronze > 0:
                log.info("    [BRONZE] ☁ Enviado(s) %d arquivo(s) bronze para o bucket", uploaded_bronze)

            # Limpa arquivos locais do bronze
            try:
                import shutil
                shutil.rmtree(bronze_base_dir)
            except Exception as e:
                log.warning(f"    [BRONZE] Não foi possível remover diretório temporário: {e}")

        resultados[nome] = uploaded_bruto + uploaded_bronze

        log.info("    Total enviado: %d arquivo(s) (bruto: %d, bronze: %d)", 
                 resultados[nome], uploaded_bruto, uploaded_bronze)
        log.info("-" * 60)
        time.sleep(1)

    # Resumo
    log.info("")
    log.info("=" * 60)
    log.info("RESUMO DA COLETA")
    log.info("=" * 60)
    for nome, total in resultados.items():
        status = "OK" if total > 0 else "VAZIO"
        log.info("  [%s] %s: %d arquivo(s)", status, nome, total)

    total_geral = sum(resultados.values())
    log.info("-" * 60)
    log.info("Total geral: %d arquivo(s) enviado(s) ao bucket", total_geral)


if __name__ == "__main__":
    main()
