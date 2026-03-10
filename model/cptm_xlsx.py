import logging
import os
# --- Configuração de logging flexível ---
xlsx_debug_log = logging.getLogger("cptm_xlsx_debug")
xlsx_debug_log.setLevel(logging.DEBUG)

# Se quiser logs separados por extrator, ative abaixo:
LOG_EXTRATOR_SEPARADO = False  # Altere para True para ativar arquivo próprio
if LOG_EXTRATOR_SEPARADO and not xlsx_debug_log.hasHandlers():
    root_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    log_path = os.path.join(root_dir, "cptm_xlsx_debug.log")
    handler = logging.FileHandler(log_path, encoding="utf-8")
    handler.setFormatter(logging.Formatter('%(asctime)s | %(levelname)-8s | %(message)s', datefmt='%H:%M:%S'))
    xlsx_debug_log.addHandler(handler)

# Sempre propaga para o logger principal (nyx)
from config import LOG_EXTRATOR_SEPARADO
from logging import getLogger
main_log = getLogger("nyx")
if not LOG_EXTRATOR_SEPARADO:
    for handler in main_log.handlers:
        if handler not in xlsx_debug_log.handlers:
            xlsx_debug_log.addHandler(handler)
    xlsx_debug_log.propagate = False
import pandas as pd
import os
import re

class CPTMXLSXExtractor:
    def __init__(self, xlsx_path):
        """
        Inicializa o extrator com o caminho do arquivo XLSX.
        Motivo: Seguir o padrão do extrator de PDF, facilitando reutilização e manutenção.
        """
        self.xlsx_path = xlsx_path
        self.df = None
        xlsx_debug_log.debug(f"[INIT] CPTMXLSXExtractor inicializado para arquivo: {xlsx_path}")

    def extract_bronze(self):
        xlsx_debug_log.debug(f"[INICIO] Processando arquivo: {self.xlsx_path}")
        """
        Extrai apenas as linhas relevantes (estações) da planilha XLSX, ignora totais, siglas e linhas em branco,
        e monta DataFrame limpo, igual ao PDF. Estrutura pronta para expansão anual.
        """
        try:
            # Leitura da planilha como texto (para parsing flexível)
            df_raw = pd.read_excel(self.xlsx_path, header=None, dtype=str)
            xlsx_debug_log.debug(f"[EXTRACT] Planilha lida com sucesso: {self.xlsx_path}")
        except Exception as e:
            xlsx_debug_log.error(f"[ERRO] Falha ao ler planilha {self.xlsx_path}: {e}")
            raise
        # Extrai mês e ano do nome do arquivo (prioridade menor)
        meses_pt = [
            'Janeiro', 'Fevereiro', 'Março', 'Abril', 'Maio', 'Junho',
            'Julho', 'Agosto', 'Setembro', 'Outubro', 'Novembro', 'Dezembro'
        ]
        meses_abrev = ['jan', 'fev', 'mar', 'abr', 'mai', 'jun', 'jul', 'ago', 'set', 'out', 'nov', 'dez']
        mes_nome = None
        ano_nome = None
        filename = os.path.basename(str(self.xlsx_path))
        xlsx_debug_log.debug(f"[EXTRACT] Nome do arquivo: {filename}")
        m_ano = re.search(r'(20\d{2})', filename)
        if m_ano:
            ano_nome = m_ano.group(1)
        m_mes = re.search(r'(\d{2})[-_](jan|fev|mar|abr|mai|jun|jul|ago|set|out|nov|dez|janeiro|fevereiro|março|abril|maio|junho|julho|agosto|setembro|outubro|novembro|dezembro)', filename, re.IGNORECASE)
        if m_mes:
            if m_mes.group(1):
                idx = int(m_mes.group(1)) - 1
                if 0 <= idx < 12:
                    mes_nome = meses_pt[idx]
            if not mes_nome and m_mes.group(2):
                abrev = m_mes.group(2).lower()
                if abrev in meses_abrev:
                    mes_nome = meses_pt[meses_abrev.index(abrev)]
                else:
                    mes_nome = abrev.capitalize()

        # Busca mês e ano no conteúdo (prioridade maior)
        mes = None
        ano = None
        for row in df_raw.itertuples(index=False):
            xlsx_debug_log.debug(f"[EXTRACT] Linha conteúdo: {row}")
            for val in row:
                if not isinstance(val, str):
                    continue
                # Busca padrão tipo "Embarcados Acumulados do Mês - janeiro/24" ou "janeiro/2025"
                m_mes_ano = re.search(r'(janeiro|fevereiro|março|abril|maio|junho|julho|agosto|setembro|outubro|novembro|dezembro)[^\d/]*(?:/|de|\-|\s)?\s*(\d{2,4})', val, re.IGNORECASE)
                if m_mes_ano:
                    mes = m_mes_ano.group(1).capitalize()
                    ano_raw = m_mes_ano.group(2)
                    # Corrige ano de 2 dígitos para 4 dígitos
                    if len(ano_raw) == 2:
                        ano = '20' + ano_raw
                    else:
                        ano = ano_raw
                    break
            if mes and ano:
                break
        # Se não achou no conteúdo, usa o nome do arquivo
        if not mes:
            mes = mes_nome
        if not ano:
            ano = ano_nome

        # Parsing das linhas relevantes (estações)
        linhas = []
        current_linha = None
        header = [
            'Grat. S.B.E.',
            'Pagantes',
            'Metro-v4-vm',
            'Não Tarifados',
            'Total Entradas',
            'Total'
        ]
        # Dicionário de padrões para identificar linhas ativas (robustez contra erros de digitação)
        padroes_linha = [
            re.compile(r"LINHA ?(\d+)", re.IGNORECASE),
            re.compile(r"INHA ?(\d+)", re.IGNORECASE),
        ]
        re_total = re.compile(r"totais? (do serviço|da linha|cptm)", re.IGNORECASE)
        re_sigla = re.compile(r"^[A-ZÀ-ÿ0-9\-]{3,}$")
        ignoradas = 0
        processadas = 0
        for i, row in df_raw.iterrows():
            valores = [str(x).strip() for x in row if pd.notnull(x)]
            line = ' '.join(valores).strip()
            motivo_ignorada = []
            if not line:
                motivo_ignorada.append('linha vazia')
                # current_linha = None  # Não limpa mais aqui
            elif re_total.search(line):
                motivo_ignorada.append('total')
                # current_linha = None  # Não limpa mais aqui
            else:
                m_linha = None
                for padrao in padroes_linha:
                    m_linha = padrao.match(line)
                    if m_linha:
                        break
                if m_linha:
                    current_linha = m_linha.group(1)
                # Critério: tem pelo menos 2 colunas, não é cabeçalho, e tem linha ativa
                if len(valores) >= 2 and current_linha and not any(h in valores for h in header):
                    if valores[0] == '' or re_sigla.match(valores[0]):
                        estacao = valores[1].strip()
                        dados_idx = 2
                    else:
                        estacao = valores[0].strip()
                        dados_idx = 1
                    estacao = re.sub(r'^[A-ZÀ-ÿ0-9\-]{2,}\s+', '', estacao)
                    row_dict = {'Estação': estacao, 'Linha': current_linha}
                    for idx, col in enumerate(header):
                        valor = valores[dados_idx + idx] if dados_idx + idx < len(valores) else ''
                        row_dict[col] = valor
                    row_dict['Total'] = valores[-1] if len(valores) > dados_idx + len(header) - 1 else ''
                    linhas.append(row_dict)
                    processadas += 1
                else:
                    if len(valores) < 2:
                        motivo_ignorada.append('colunas insuficientes')
                    if not current_linha:
                        motivo_ignorada.append('sem linha ativa')
                    if any(h in valores for h in header):
                        motivo_ignorada.append('é cabeçalho')
            if motivo_ignorada:
                xlsx_debug_log.debug(f"Linha ignorada: {valores} | Motivo: {', '.join(motivo_ignorada)}")
                ignoradas += 1
            # Limpa current_linha só se encontrar uma nova LINHA X (ou variação)
            for padrao in padroes_linha:
                if padrao.match(line):
                    # Se quiser limpar ao encontrar nova LINHA X, pode-se fazer aqui
                    pass
        expected_cols = ['Estação', 'Linha'] + header
        if 'Total' not in expected_cols:
            expected_cols.append('Total')
        result_df = pd.DataFrame(linhas)
        for col in expected_cols:
            if col not in result_df.columns:
                result_df[col] = None
        # Adiciona colunas Mês e Ano
        result_df['Mês'] = mes
        result_df['Ano'] = ano
        result_df = result_df[expected_cols + ['Mês', 'Ano']]
        xlsx_debug_log.info(f"[RESUMO] Processadas: {processadas} | Ignoradas: {ignoradas}")
        self.df = result_df
        return {"df": result_df, "table": result_df}
