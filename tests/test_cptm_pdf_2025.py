

import pandas as pd
from model.cptm_pdf import CPTMPDFExtractor
from pathlib import Path
import textwrap

# Caminho para o PDF de exemplo na pasta cptm
pdf_path = Path("cptm/exemplo_pdf_cptm_2025_01-jan.pdf")

if not pdf_path.exists():
    print(f"Arquivo de teste não encontrado: {pdf_path}")
else:
    extractor = CPTMPDFExtractor(str(pdf_path))
    result = extractor.extract_bronze()
    print("Texto extraído:\n", result["text"][:1000], "...\n")
    print(f"Tabela extraída (shape): {result['table'].shape}")
    # Extrai mês e ano do texto do PDF
    import re
    text = result["text"]
    m_mes_ano = re.search(r"Embarcados Acumulados\s*do Mês\s*[-–]?\s*([a-zç]+)\s*/\s*(\d{4})", text, re.IGNORECASE)
    mes_pdf = ano_pdf = None
    if m_mes_ano:
        mes_pdf = m_mes_ano.group(1).capitalize()
        ano_pdf = m_mes_ano.group(2)
        print(f"[INFO] Mês e ano extraídos do PDF: {mes_pdf}, {ano_pdf}")
    else:
        print("[WARN] Não foi possível extrair mês e ano do texto do PDF.")
    # Exibe toda a tabela extraída no console
    with pd.option_context('display.max_rows', None, 'display.max_columns', None, 'display.width', 1000):
        print(result['table'])
    # Salva a tabela extraída em CSV para conferência
    csv_path = Path("cptm/exemplo_pdf_cptm.csv")
    result['table'].to_csv(csv_path, index=False, encoding='utf-8-sig')
    print(f"Tabela salva em: {csv_path.resolve()}")
    # Compara mês e ano extraídos do texto com os da tabela
    if mes_pdf and ano_pdf and not result['table'].empty:
        mes_col = result['table'].iloc[0].get('Mês')
        ano_col = result['table'].iloc[0].get('Ano')
        print(f"[CHECK] Mês/Ano na tabela extraída: {mes_col}, {ano_col}")
        if str(mes_col).lower().startswith(mes_pdf.lower()[:3]) and str(ano_col) == ano_pdf:
            print("[OK] Mês e ano da tabela batem com o texto do PDF!")
        else:
            print("[ERRO] Mês e ano da tabela NÃO batem com o texto do PDF!")
        # Removed the old call to extract_bronze and references to prompt/examples
