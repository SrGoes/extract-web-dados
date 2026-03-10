
import re
import pandas as pd
import pdfplumber

import logging
import os

# --- Configuração de logging flexível ---
pdf_debug_log = logging.getLogger("cptm_pdf_debug")
pdf_debug_log.setLevel(logging.DEBUG)

# Se quiser logs separados por extrator, ative abaixo:
LOG_EXTRATOR_SEPARADO = False  # Altere para True para ativar arquivo próprio
if LOG_EXTRATOR_SEPARADO and not pdf_debug_log.hasHandlers():
    root_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
    log_path = os.path.join(root_dir, "cptm_pdf_debug.log")
    handler = logging.FileHandler(log_path, encoding="utf-8")
    handler.setFormatter(logging.Formatter('%(asctime)s | %(levelname)-8s | %(message)s', datefmt='%H:%M:%S'))
    pdf_debug_log.addHandler(handler)

# Sempre propaga para o logger principal (nyx)
from config import LOG_EXTRATOR_SEPARADO
from logging import getLogger
main_log = getLogger("nyx")
if not LOG_EXTRATOR_SEPARADO:
    for handler in main_log.handlers:
        if handler not in pdf_debug_log.handlers:
            pdf_debug_log.addHandler(handler)
    pdf_debug_log.propagate = False


class CPTMPDFExtractor:
    def __init__(self, pdf_path):
        self.pdf_path = pdf_path
        self.text = None

    def extract_text(self):
        if self.text is not None:
            return self.text
        try:
            with pdfplumber.open(self.pdf_path) as pdf:
                self.text = "\n".join(page.extract_text() or "" for page in pdf.pages)
        except Exception as e:
            pdf_debug_log.error(f"Erro ao abrir PDF {self.pdf_path}: {e}")
            self.text = ""
        return self.text

    def extract_bronze(self):
        pdf_debug_log.debug(f"[INICIO] Processando arquivo: {self.pdf_path}")
        """
        Extrai todas as tabelas do PDF, identifica a linha (ex: LINHA 7),
        monta DataFrame robusto a partir do texto extraído, ignorando totais e siglas.
        """
        text = self.extract_text()
        # Extrai mês e ano do nome do arquivo (prioritário) ou do texto
        meses_pt = [
            'Janeiro', 'Fevereiro', 'Março', 'Abril', 'Maio', 'Junho',
            'Julho', 'Agosto', 'Setembro', 'Outubro', 'Novembro', 'Dezembro'
        ]
        meses_abrev = ['jan', 'fev', 'mar', 'abr', 'mai', 'jun', 'jul', 'ago', 'set', 'out', 'nov', 'dez']
        mes = None
        ano = None
        filename = os.path.basename(str(self.pdf_path))
        # Busca ano (4 dígitos)
        m_ano = re.search(r'(20\d{2})', filename)
        if m_ano:
            ano = m_ano.group(1)
        # Busca mês (ex: 01, jan, janeiro)
        m_mes = re.search(r'(\d{2})[-_](jan|fev|mar|abr|mai|jun|jul|ago|set|out|nov|dez|janeiro|fevereiro|março|abril|maio|junho|julho|agosto|setembro|outubro|novembro|dezembro)', filename, re.IGNORECASE)
        if m_mes:
            if m_mes.group(1):
                # Se for número, converte para nome
                idx = int(m_mes.group(1)) - 1
                if 0 <= idx < 12:
                    mes = meses_pt[idx]
            if not mes and m_mes.group(2):
                abrev = m_mes.group(2).lower()
                if abrev in meses_abrev:
                    mes = meses_pt[meses_abrev.index(abrev)]
                else:
                    # Se já vier por extenso
                    mes = abrev.capitalize()
        # Fallback: busca no texto se não achou no nome
        if not mes or not ano:
            m_mes_ano = re.search(r'(janeiro|fevereiro|março|abril|maio|junho|julho|agosto|setembro|outubro|novembro|dezembro)[^\d]*(20\d{2})', text, re.IGNORECASE)
            if m_mes_ano:
                mes = m_mes_ano.group(1).capitalize()
                ano = m_mes_ano.group(2)
            else:
                m_ano = re.search(r'(20\d{2})', text)
                if m_ano:
                    ano = m_ano.group(1)
                m_mes = re.search(r'(janeiro|fevereiro|março|abril|maio|junho|julho|agosto|setembro|outubro|novembro|dezembro)', text, re.IGNORECASE)
                if m_mes:
                    mes = m_mes.group(1).capitalize()
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
        re_linha = re.compile(r"LINHA ?(\d+)", re.IGNORECASE)
        re_total = re.compile(r"totais? (do serviço|da linha|cptm)", re.IGNORECASE)
        re_sigla = re.compile(r"^[A-ZÀ-ÿ0-9\-]{3,}$")
        # Permite nomes de estação com º, ., vírgula, parênteses, etc
        re_dados = re.compile(r"^([A-Za-zÀ-ÿ0-9\-\.\sº,()']+)\s+([\d\.]+)\s+([\d\.]+)\s+([\d\.]+)\s+([\d\.]+)\s+([\d\.]+)\s+([\d\.]+)(?:\s+([\d\.]+))?$")
        ignoradas = 0
        processadas = 0
        for line in text.splitlines():
            line = line.strip()
            motivo_ignorada = []
            if not line:
                motivo_ignorada.append('linha vazia')
                current_linha = None
            elif re_total.search(line):
                motivo_ignorada.append('total')
                current_linha = None
            else:
                m_linha = re_linha.match(line)
                if m_linha:
                    current_linha = m_linha.group(1)
                m_dados = re_dados.match(line)
                if m_dados and current_linha:
                    estacao = m_dados.group(1).strip()
                    estacao = re.sub(r'^[A-ZÀ-ÿ0-9\-]{2,}\s+', '', estacao) if re.match(r'^[A-ZÀ-ÿ0-9\-]{2,}\s+', estacao) else estacao
                    valores = [m_dados.group(i) for i in range(2, 8)]
                    total = m_dados.group(8) if m_dados.lastindex == 8 and m_dados.group(8) is not None else valores[-1]
                    row_dict = {'Estação': estacao, 'Linha': current_linha}
                    for idx, col in enumerate(header):
                        if idx < len(valores):
                            row_dict[col] = valores[idx]
                    row_dict['Total'] = total
                    linhas.append(row_dict)
                    processadas += 1
                else:
                    if not m_dados:
                        motivo_ignorada.append('regex não bateu')
                    if not current_linha:
                        motivo_ignorada.append('sem linha ativa')
            if motivo_ignorada:
                pdf_debug_log.debug(f"Linha ignorada: '{line}' | Motivo: {', '.join(motivo_ignorada)}")
                ignoradas += 1
        expected_cols = ['Estação', 'Linha'] + header
        if 'Total' not in expected_cols:
            expected_cols.append('Total')
        try:
            result_df = pd.DataFrame(linhas)
            for col in expected_cols:
                if col not in result_df.columns:
                    result_df[col] = None
            # Adiciona colunas Mês e Ano
            result_df['Mês'] = mes
            result_df['Ano'] = ano
            result_df = result_df[expected_cols + ['Mês', 'Ano']]
        except Exception as e:
            pdf_debug_log.error(f"Erro ao montar DataFrame do PDF {self.pdf_path}: {e}")
            result_df = pd.DataFrame()
        pdf_debug_log.info(f"[RESUMO] Processadas: {processadas} | Ignoradas: {ignoradas}")
        return {"df": result_df, "table": result_df, "text": text}
