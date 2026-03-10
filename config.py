 # True = gera arquivos cptm_pdf_debug.log etc
"""
Nyx — Configuração central do projeto.

Este arquivo contém todas as configurações do projeto em um só lugar.
É como o "painel de controle" — aqui você define:
  - Bucket GCS de destino
  - Como o robô se identifica para os sites
  - Quais tipos de arquivo manter ou descartar
  - A lista de sites que devem ser coletados
"""

import os
import logging
from pathlib import Path

from dotenv import load_dotenv

# Carrega variáveis do .env
load_dotenv()

# ---------------------------------------------------------------------------
# Caminhos de pastas
# ---------------------------------------------------------------------------
BASE_DIR = Path(__file__).resolve().parent  # Pasta raíz do projeto (onde este arquivo está)

# ---------------------------------------------------------------------------
# Google Cloud Storage
# ---------------------------------------------------------------------------
GCS_BUCKET_NAME = os.environ["GCS_BUCKET_NAME"]

# ---------------------------------------------------------------------------
# Configurações de logging customizadas
# ---------------------------------------------------------------------------
# Nível do log principal (DEBUG, INFO, WARNING, ERROR)
LOG_NIVEL_PRINCIPAL = "DEBUG"  # Altere para "INFO" se quiser menos detalhes
# Ativa/desativa logs separados por extrator (PDF/XLSX)
LOG_EXTRATOR_SEPARADO = False 

# ---------------------------------------------------------------------------
# Cabeçalhos HTTP — "disfarce" do robô
# Quando o robô acessa um site, ele se apresenta com estes cabeçalhos.
# Isso faz o site pensar que quem está acessando é um navegador Chrome
# normal, e não um programa automático (alguns sites bloqueiam robôs).
# ---------------------------------------------------------------------------
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
}

# ---------------------------------------------------------------------------
# Extensões de arquivo
# ---------------------------------------------------------------------------
# Arquivos úteis que queremos MANTER após a extração dos ZIPs
# (são os dados reais: PDFs, planilhas, imagens, etc.)
KEEP_EXTENSIONS = {
    ".pdf", ".png", ".jpg", ".jpeg", ".bmp", ".tiff",
    ".xls", ".xlsx", ".xlsm", ".csv", ".ods", ".json", ".xml",
}

# Arquivos compactados — serão abertos (extraídos) e depois DELETADOS
# (já que o conteúdo foi salvo separadamente)
ARCHIVE_EXTENSIONS = {".zip", ".rar", ".7z", ".gz", ".tar"}

# ---------------------------------------------------------------------------
# Logging (sistema de "diário")
# Configura como as mensagens do programa aparecem no terminal.
# Formato: "HORA | NÍVEL | Mensagem"
# Ex: "14:30:05 | INFO    | Baixando arquivo..."
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(message)s",
    datefmt="%H:%M:%S",
)

# ---------------------------------------------------------------------------
# Lista de sites para coletar dados
# Cada item tem o "nome" da empresa e a "url" da página onde os dados estão.
# O programa vai percorrer essa lista e tentar coletar dados de cada um.
# ---------------------------------------------------------------------------
SITES: list[dict[str, str]] = [
    {
        "nome": "CPTM",
        "url": "https://www.cptm.sp.gov.br/cptm/transparencia/operacao",
        "engine": "bs4",  # Site estático — requests + BeautifulSoup
    },
    {
        "nome": "TVM",
        "url": "https://transparencia.metrosp.com.br/dataset/demanda",
        "engine": "bs4",
    },
    {
        "nome": "ViaQuatro",
        "url": "https://trilhos.motiva.com.br/viaquatro/historico-de-pessoas-transportadas/",
        "engine": "bs4",  # Site com JS — precisa de navegador real
    },
    {
        "nome": "ViaMobilidade_L8_L9", #linha 9 e 8
        "url": "https://trilhos.motiva.com.br/viamobilidade8e9/historico-de-pessoas-transportadas/",
        "engine": "bs4",
    },
    {
        "nome": "ViaMobilidade_L5", #linha 5
        "url": "https://trilhos.motiva.com.br/viamobilidade5/historico-de-pessoas-transportadas/",
        "engine": "bs4",
    },
    {
        "nome": "Anac_Fraport",
        "url": "https://www.gov.br/anac/pt-br/assuntos/dados-e-estatisticas/dados-estatisticos/dados-estatisticos",
        "engine": "bs4",
    },
]