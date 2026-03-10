
# NYX — Crawler e Pipeline de Dados de Transporte Público

## Visão Geral

O NYX é um sistema modular para coleta, padronização e organização de dados públicos de transporte (ex: CPTM), estruturado em camadas de medalhão (bruto, bronze, silver, gold) e integrado ao Google Cloud Storage (GCS). O projeto é extensível, robusto e configurável via `config.py`.

---

## Camadas de Medalhão
- **Bruto**: Dados coletados diretamente dos sites, sem tratamento.
- **Bronze**: Dados extraídos e padronizados de acordo com a plataforma, prontos para análises iniciais.
- **Silver**: (futuro) Dados transformados, limpos e enriquecidos.
- **Gold**: (futuro) Dados prontos para consumo analítico e dashboards.

---

## Estrutura do Projeto

- `main.py`: Dispatcher principal. Lê a lista de sites (em `config.py`), aciona o crawler de cada site, faz upload dos arquivos brutos e bronze para o GCS e gera um resumo da coleta.
- `config.py`: Configurações gerais, lista de sites, extensões permitidas, variáveis de ambiente, logging.
- `cptm/app.py`: Crawler específico para a CPTM, herdando de `BaseCrawler` (em `utils/base.py`). Faz download, descompactação, padronização de nomes, organização por ano e limpeza dos arquivos da CPTM. Pode ser usado isoladamente.
- `model/cptm_pdf.py` e `model/cptm_xlsx.py`: Lógica de extração de tabelas dos PDFs e XLSX da CPTM para a camada bronze, incluindo parser robusto para nomes de estação, mês, ano e colunas padronizadas.
- `utils/base.py`: Classe base para crawlers, com métodos utilitários de download, extração e organização.
- `utils/storage.py`: Gerencia o upload dos arquivos para o GCS, seguindo a estrutura de medalhão.
- `utils/conection.py`: Centraliza a autenticação e conexão com o bucket GCS.
- `utils/checkBucket.py`: Lógica para evitar downloads redundantes do bucket.
- `tests/`: Testes automatizados para conexões, extração e lógica de download.

---

## Como Executar o Projeto Completo

1. **Configure as variáveis de ambiente** (.env):
	- `GCS_BUCKET_NAME`: Nome do bucket GCS.
	- `SECRET_GCP_ENV`: Chave de autenticação GCP (base64).
2. **Instale as dependências:**
	```bash
	pip install -r requirements.txt
	```
3. **Edite o `config.py`** conforme necessário (logging, sites, etc).
4. **Execute o pipeline completo:**
	```bash
	python main.py
	```
	O log completo da execução estará em `nyx_run.log`.

---

## Logging e Configuração

- O nível de logging e a separação dos logs por extrator são configuráveis em `config.py`:
  - `LOG_NIVEL_PRINCIPAL`: "DEBUG", "INFO", etc.
  - `LOG_EXTRATOR_SEPARADO`: Se `True`, gera logs separados para PDF/XLSX; se `False`, tudo vai para o log principal.
- Todos os logs relevantes (incluindo linhas ignoradas e motivos) são salvos em `nyx_run.log`.

---

## Extração Bronze (PDF/XLSX → tabela)

Para extrair tabelas dos arquivos da CPTM para a camada bronze, utilize os métodos `extract_bronze` das classes `CPTMPDFExtractor` e `CPTMXLSXExtractor`:

```python
from model.cptm_pdf import CPTMPDFExtractor
from model.cptm_xlsx import CPTMXLSXExtractor

# PDF
extractor_pdf = CPTMPDFExtractor("caminho/para/arquivo.pdf")
result_pdf = extractor_pdf.extract_bronze()
df_pdf = result_pdf["table"]

# XLSX
extractor_xlsx = CPTMXLSXExtractor("caminho/para/arquivo.xlsx")
result_xlsx = extractor_xlsx.extract_bronze()
df_xlsx = result_xlsx["table"]
```

Os DataFrames resultantes já trazem as colunas limpas, nomes de estação sem sigla, mês e ano extraídos do nome do arquivo, e todos os campos numéricos padronizados.

---

## Testes Automatizados

Execute os testes unitários para garantir o funcionamento dos extratores e da conexão com o bucket:

```bash
python -m unittest discover tests
```

---

## Extensibilidade
- Para adicionar um novo site, basta criar um novo crawler herdando de `BaseCrawler` e registrar em `main.py` e `config.py`.
- O pipeline é facilmente adaptável para novas fontes e formatos.

---

## Observações
- O projeto segue boas práticas de logging, modularização e organização de dados.
- O parser bronze é robusto para PDF e XLSX, com logging detalhado de linhas ignoradas e motivos.
- Integração nativa com Google Cloud Storage.
