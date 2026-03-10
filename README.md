# Crawler e Pipeline de Dados de Transporte

## Visão Geral

Sistema modular para coleta, padronização e organização de dados públicos de transporte (ex: CPTM), estruturado em camadas de medalhão (bruto, bronze, silver, gold) e integrado ao Google Cloud Storage (GCS).

## Camadas de Medalhão
- **Bruto**: Dados coletados diretamente dos sites, sem tratamento.
- **Bronze**: Dados extraídos e padronizados (usando langExtract para PDF/Excel), prontos para análises iniciais.
- **Silver**: (futuro) Dados transformados, limpos e enriquecidos.
- **Gold**: (futuro) Dados prontos para consumo analítico e dashboards.


## Estrutura do Projeto
- `main.py`: Dispatcher principal. Lê a lista de sites (em `config.py`), aciona o crawler de cada site, faz upload dos arquivos brutos para o GCS e gera um resumo da coleta.
- `config.py`: Configurações gerais, lista de sites, extensões permitidas, variáveis de ambiente.
- `cptm/app.py`: Crawler específico para a CPTM, herdando de `BaseCrawler` (em `utils/base.py`). Faz download, descompactação, padronização de nomes, organização por ano e limpeza dos arquivos da CPTM. Pode ser usado isoladamente para baixar e organizar todos os dados públicos da CPTM.
- `model/cptm_pdf.py`: Lógica de extração de tabelas dos PDFs da CPTM para a camada bronze, incluindo parser robusto para nomes de estação, mês, ano e colunas padronizadas.
- `utils/base.py`: Classe base para crawlers, com métodos utilitários de download, extração e organização.
- `utils/storage.py`: Gerencia o upload dos arquivos para o GCS, seguindo a estrutura de medalhão.
- `utils/conection.py`: Centraliza a autenticação e conexão com o bucket GCS.
- `utils/bronze.py`: Processa os arquivos brutos do bucket, utiliza langExtract para extrair e padronizar tabelas de arquivos PDF/Excel, gera arquivos bronze e faz upload para o GCS.
- `utils/checkBucket.py`: Lógica para evitar downloads redundantes do bucket.
## Como usar o crawler da CPTM

O arquivo `cptm/app.py` pode ser executado como módulo ou importado em outros scripts. Ele baixa todos os arquivos públicos de embarques acumulados da CPTM, organiza por ano e padroniza os nomes dos arquivos.

Exemplo de uso isolado:

```python
from cptm.app import crawler
from pathlib import Path
import requests

url = "https://www.cptm.sp.gov.br/cptm/transparencia/operacao"
dest_dir = Path("./dados_cptm")
session = requests.Session()

total = crawler.crawl(session, url, dest_dir)
print(f"Total de arquivos úteis baixados: {total}")
```

Os arquivos serão organizados em subpastas por ano e padronizados no formato:
`Embarcados Acumulados - 2025 - 01-Jan.pdf`

## Extração bronze (PDF → tabela)

Para extrair tabelas dos PDFs da CPTM para a camada bronze, utilize o método `extract_bronze` da classe `CPTMPDFExtractor` em `model/cptm_pdf.py`:

```python
from model.cptm_pdf import CPTMPDFExtractor

extractor = CPTMPDFExtractor("caminho/para/arquivo.pdf")
result = extractor.extract_bronze()
df = result["table"]  # DataFrame já padronizado com colunas: Estação, Linha, Mês, Ano, etc.
```

O DataFrame resultante já traz as colunas limpas, nomes de estação sem sigla, mês e ano extraídos do nome do arquivo, e todos os campos numéricos padronizados.


## Fluxo CPTM resumido
1. O crawler da CPTM baixa e organiza todos os arquivos públicos de embarques acumulados.
2. Os arquivos são organizados por ano e padronizados.
3. O parser bronze extrai tabelas dos PDFs, já com colunas limpas, mês e ano.
4. Os dados bronze podem ser enviados para o GCS ou usados em análises locais.

## Extensibilidade
- Para adicionar um novo site, basta criar um novo crawler herdando de `BaseCrawler` e registrar em `main.py` e `config.py`.
- O pipeline é facilmente adaptável para novas fontes e formatos.

## Observações
- O projeto segue boas práticas de logging, modularização e organização de dados.
- O uso de langExtract garante extração robusta de tabelas de PDFs e Excel para a camada bronze.

---

> Projeto desenvolvido para automação, padronização e governança de dados públicos de transporte.
