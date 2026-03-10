
import pandas as pd
from model.cptm_xlsx import CPTMXLSXExtractor
from pathlib import Path
import re

# Caminho para o XLSX de exemplo na pasta cptm
xlsx_path = Path("cptm/exemplo_xlsx_cptm_2025_01-jan.xlsx")

def test_cptm_xlsx_extractor():
    if not xlsx_path.exists():
        print(f"Arquivo de teste não encontrado: {xlsx_path}")
        return
    extractor = CPTMXLSXExtractor(str(xlsx_path))
    result = extractor.extract_bronze()
    print(f"Tabela extraída (shape): {result['table'].shape}")
    # Extrai mês e ano do nome do arquivo
    filename = str(xlsx_path.name)
    m_mes_ano = re.search(r'(\d{2})[-_](jan|fev|mar|abr|mai|jun|jul|ago|set|out|nov|dez|janeiro|fevereiro|março|abril|maio|junho|julho|agosto|setembro|outubro|novembro|dezembro)', filename, re.IGNORECASE)
    meses_pt = [
        'Janeiro', 'Fevereiro', 'Março', 'Abril', 'Maio', 'Junho',
        'Julho', 'Agosto', 'Setembro', 'Outubro', 'Novembro', 'Dezembro'
    ]
    meses_abrev = ['jan', 'fev', 'mar', 'abr', 'mai', 'jun', 'jul', 'ago', 'set', 'out', 'nov', 'dez']
    mes_xlsx = None
    ano_xlsx = None
    m_ano = re.search(r'(20\d{2})', filename)
    if m_ano:
        ano_xlsx = m_ano.group(1)
    if m_mes_ano:
        if m_mes_ano.group(1):
            idx = int(m_mes_ano.group(1)) - 1
            if 0 <= idx < 12:
                mes_xlsx = meses_pt[idx]
        if not mes_xlsx and m_mes_ano.group(2):
            abrev = m_mes_ano.group(2).lower()
            if abrev in meses_abrev:
                mes_xlsx = meses_pt[meses_abrev.index(abrev)]
            else:
                mes_xlsx = abrev.capitalize()
    print(f"[INFO] Mês e ano extraídos do nome: {mes_xlsx}, {ano_xlsx}")
    # Exibe toda a tabela extraída no console
    with pd.option_context('display.max_rows', None, 'display.max_columns', None, 'display.width', 1000):
        print(result['table'])
    # Salva a tabela extraída em CSV para conferência
    csv_path = Path("cptm/exemplo_xlsx_cptm.csv")
    result['table'].to_csv(csv_path, index=False, encoding='utf-8-sig')
    print(f"Tabela salva em: {csv_path.resolve()}")
    # Compara mês e ano extraídos do nome com os da tabela
    if mes_xlsx and ano_xlsx and not result['table'].empty:
        mes_col = result['table'].iloc[0].get('Mês')
        ano_col = result['table'].iloc[0].get('Ano')
        print(f"[CHECK] Mês/Ano na tabela extraída: {mes_col}, {ano_col}")
        if str(mes_col).lower().startswith(mes_xlsx.lower()[:3]) and str(ano_col) == ano_xlsx:
            print("[OK] Mês e ano da tabela batem com o nome do arquivo!")
        else:
            print("[ERRO] Mês e ano da tabela NÃO batem com o nome do arquivo!")

if __name__ == "__main__":
    test_cptm_xlsx_extractor()
