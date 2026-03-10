from utils.checkBucket import should_download
from utils.base import BaseCrawler
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import os
import re
import shutil
import time
import zipfile
import requests
"""
Crawler CPTM — https://www.cptm.sp.gov.br/cptm/transparencia/operacao

== O QUE ESTE ARQUIVO FAZ (explicação simples) ==

Este código é o robô especialista da CPTM. Ele herda as funcionalidades
comuns da classe BaseCrawler (download, descompactar, organizar por ano)
e implementa apenas a lógica ESPECÍFICA da CPTM:
  - Como encontrar os links de download na página
  - Como filtrar só os dados de "Embarcados Acumulados"
  - Como padronizar os nomes dos arquivos da CPTM
  - Como injetar o ano nos arquivos que não têm

A página lista tabelas com links de download para ZIPs contendo dados
de Embarcados Acumulados e Pagantes por Tipo de Bilhete, por ano.

Os links usam o CMS do governo de SP:
  https://admin.cms.sp.gov.br/dx/api/dam/v1/collections/.../renditions/...?binary=true

Estrutura dos ZIPs:
  ZIP anual → ZIP mensal (meses antigos) + PDF (meses recentes) → PDF/xlsx final
"""
"""
Crawler CPTM — https://www.cptm.sp.gov.br/cptm/transparencia/operacao

Este módulo implementa o crawler para baixar, organizar e padronizar os arquivos públicos de embarques acumulados da CPTM.
Pode ser executado isoladamente ou importado em outros scripts.

Principais funções:
- Download recursivo de todos os arquivos públicos de embarques acumulados.
- Organização dos arquivos em subpastas por ano.
- Padronização dos nomes dos arquivos para facilitar o processamento posterior.
- Normalização de datas (mês/ano) e nomes de arquivos.

Como configurar:
- Defina o diretório de destino (dest_dir) para salvar os arquivos.
- Forneça uma sessão requests autenticada, se necessário.
- O método principal é `crawl(session, url, dest_dir)`.

Exemplo de uso:
    from cptm.app import crawler
    from pathlib import Path
    import requests
    url = "https://www.cptm.sp.gov.br/cptm/transparencia/operacao"
    dest_dir = Path("./dados_cptm")
    session = requests.Session()
    total = crawler.crawl(session, url, dest_dir)
    print(f"Total de arquivos úteis baixados: {total}")

Estrutura dos ZIPs:
  ZIP anual → ZIP mensal (meses antigos) + PDF (meses recentes) → PDF/xlsx final
"""
# Dicionário que traduz nomes de meses para (número, abreviação)
# Necessário porque os arquivos da CPTM usam nomes escritos de formas
# diferentes ("Jan", "Janeiro", "jan", "mar+ºo", etc.)
_MONTH_MAP: dict[str, tuple[str, str]] = {
    "janeiro": ("01", "Jan"), "jan": ("01", "Jan"),
    "fevereiro": ("02", "Fev"), "fev": ("02", "Fev"),
    "março": ("03", "Mar"), "mar": ("03", "Mar"), "mar+ºo": ("03", "Mar"),
    "abril": ("04", "Abr"), "abr": ("04", "Abr"),
    "maio": ("05", "Mai"), "mai": ("05", "Mai"),
    "junho": ("06", "Jun"), "jun": ("06", "Jun"),
    "julho": ("07", "Jul"), "jul": ("07", "Jul"),
    "agosto": ("08", "Ago"), "ago": ("08", "Ago"),
    "setembro": ("09", "Set"), "set": ("09", "Set"),
    "outubro": ("10", "Out"), "out": ("10", "Out"),
    "novembro": ("11", "Nov"), "nov": ("11", "Nov"),
    "dezembro": ("12", "Dez"), "dez": ("12", "Dez"),
}


# =========================================================================
# Crawler da CPTM
# =========================================================================

class CptmCrawler(BaseCrawler):
    """
    Crawler específico para o site da CPTM.
    Herda funcionalidades comuns de BaseCrawler e implementa
    a lógica particular deste site.
    """

    engine = "bs4"
    name = "CPTM"

    # ------------------------------------------------------------------
    # Métodos específicos da CPTM
    # ------------------------------------------------------------------

    def _inject_year(self, fname: str, year: str) -> str:
        """
        Se o nome do arquivo não contém o ano, adiciona o ano nele.

        Alguns arquivos dentro dos ZIPs não têm o ano no nome
        (ex: "Embarcados Acumulados - 02-Fev.xlsx"), mas sabemos o ano
        pelo nome do ZIP pai. Então injetamos o ano.
        """
        if self.extract_year(fname):
            return fname

        stem, ext = os.path.splitext(fname)

        m = re.match(r"(Embarcados Acumulados\s*-\s*)(.*)", stem, re.I)
        if m:
            return f"{m.group(1)}{year} - {m.group(2)}{ext}"

        return f"{stem} {year}{ext}"

    def _normalize_filename(self, filename: str, fallback_year: str | None = None) -> str | None:
        """
        Padroniza o nome de um arquivo para o formato:
            "Embarcados Acumulados - 2019 - 01-Jan.xlsx"

        Retorna None se o arquivo deve ser descartado (ex: Pagantes).
        """
        name_lower = filename.lower()

        if "pagantes" in name_lower:
            return None

        if "embarcados" not in name_lower:
            return filename

        stem, ext = os.path.splitext(filename)

        year = self.extract_year(stem)
        if not year and fallback_year:
            year = fallback_year
        if not year:
            return filename

        month_num = None
        month_abbr = None

        # Padrão "MM-Mes" como "01-Jan", "02-Fev"
        m = re.search(r"(\d{2})\s*[-]\s*([A-Za-zçã+º]+)", stem)
        if m:
            num_candidate = m.group(1)
            text_candidate = m.group(2).strip().lower()
            if text_candidate in _MONTH_MAP:
                month_num, month_abbr = _MONTH_MAP[text_candidate]
            elif num_candidate.isdigit() and 1 <= int(num_candidate) <= 12:
                for k, (mn, ma) in _MONTH_MAP.items():
                    if mn == num_candidate:
                        month_num, month_abbr = mn, ma
                        break

        # Padrão "mês YYYY" ou "mês" por extenso
        if not month_num:
            for month_name, (mn, ma) in _MONTH_MAP.items():
                if month_name in stem.lower():
                    month_num, month_abbr = mn, ma
                    break

        if not month_num:
            return filename

        return f"Embarcados Acumulados - {year} - {month_num}-{month_abbr}{ext}"

    def _normalize_all_files(self, dest_dir: Path):
        """
        Percorre todos os arquivos da pasta (e subpastas) e:
        - Padroniza os nomes
        - Remove arquivos de "Pagantes"
        - Remove duplicatas
        """
        dirs_to_process = [dest_dir] + [d for d in dest_dir.iterdir() if d.is_dir()]

        total_renamed = 0
        total_removed = 0

        for folder in dirs_to_process:
            fallback_year = folder.name if re.match(r"^20\d{2}$", folder.name) else None

            files = [f for f in folder.iterdir() if f.is_file()]
            for f in files:
                new_name = self._normalize_filename(f.name, fallback_year)

                if new_name is None:
                    f.unlink()
                    self.log.info("    🗑 Descartado: %s", f.name)
                    total_removed += 1
                    continue

                if new_name != f.name:
                    dest = folder / new_name
                    same_file = (
                        dest.exists()
                        and f.name.lower() == new_name.lower()
                    )
                    if same_file:
                        tmp = folder / (f.name + ".tmp")
                        f.rename(tmp)
                        tmp.rename(dest)
                        self.log.info("    ✏ Renomeado: %s → %s", f.name, new_name)
                        total_renamed += 1
                    elif dest.exists():
                        f.unlink()
                        self.log.info("    🗑 Duplicata removida: %s", f.name)
                        total_removed += 1
                    else:
                        f.rename(dest)
                        self.log.info("    ✏ Renomeado: %s → %s", f.name, new_name)
                        total_renamed += 1

        self.log.info("  %d renomeado(s), %d removido(s)", total_renamed, total_removed)

    # ------------------------------------------------------------------
    # Override: extração com herança de ano (específico da CPTM)
    # ------------------------------------------------------------------

    def extract_archive(self, archive_path: Path, dest_dir: Path) -> list[Path]:
        """
        Extrai ZIP/RAR herdando o ano do nome do arquivo pai
        nos membros que não possuem ano (comportamento específico da CPTM).
        """
        extracted: list[Path] = []
        ext = archive_path.suffix.lower()
        parent_year = self.extract_year(archive_path.name)

        def _process_member(fname: str, data_stream) -> None:
            fname = self.safe_filename(os.path.basename(fname))
            if not fname:
                return
            if parent_year and not self.extract_year(fname):
                fname = self._inject_year(fname, parent_year)
            dest = dest_dir / fname
            with open(dest, "wb") as dst:
                shutil.copyfileobj(data_stream, dst)
            extracted.append(dest)
            self.log.info("    📦 Extraído: %s", fname)

        try:
            if ext == ".zip":
                with zipfile.ZipFile(archive_path, "r") as zf:
                    for member in zf.namelist():
                        if member.endswith("/"):
                            continue
                        with zf.open(member) as src:
                            _process_member(member, src)
            else:
                self.log.warning("    Formato não suportado para extração: %s", ext)

        except Exception as e:
            self.log.error("    ✗ Erro ao extrair %s: %s", archive_path.name, e)

        return extracted

    # ------------------------------------------------------------------
    # Método principal de coleta
    # ------------------------------------------------------------------

    def crawl(self, session: requests.Session, url: str, dest_dir: Path) -> int:
        """
        Acessa o site da CPTM, encontra os links de download, baixa os ZIPs,
        descompacta recursivamente, organiza por ano e padroniza os nomes.

        Retorna a quantidade de arquivos úteis coletados.
        """
        # self.log.info("[%s] Acessando: %s", self.name, url)

        try:
            resp = session.get(url, timeout=30)
            resp.raise_for_status()
        except Exception as e:
            self.log.error("[%s] Erro ao acessar página: %s", self.name, e)
            return 0

        soup = BeautifulSoup(resp.text, "lxml")
        dest_dir.mkdir(parents=True, exist_ok=True)

        # --- ETAPA 1: Encontra os links de download na página ---
        download_items: list[dict] = []
        seen_urls: set[str] = set()

        for a_tag in soup.find_all("a", href=True):
            href = a_tag["href"].strip()
            if not href or href.startswith("#") or href.startswith("javascript"):
                continue

            full_url = urljoin(url, href)
            if full_url in seen_urls:
                continue

            link_text = a_tag.get_text(strip=True)
            alt = (a_tag.get("alt") or "").lower()

            is_binary = "binary=true" in full_url.lower()
            is_baixar = "baixar" in alt
            has_ext = bool(re.search(
                r"\.(zip|rar|pdf|csv|xls|xlsx|ods|json|xml)\b", link_text, re.I
            ))

            if is_binary or is_baixar or has_ext:
                if link_text and "pagantes" in link_text.lower():
                    continue
                fname = self.safe_filename(link_text) if link_text else "arquivo_sem_nome"
                download_items.append({"url": full_url, "filename": fname})
                seen_urls.add(full_url)

        # self.log.info("[%s] %d arquivo(s) encontrado(s) para download", self.name, len(download_items))

        # --- ETAPA 2: Baixa os arquivos encontrados ---
        downloaded = 0

        for item in download_items:
            raw_fname = item["filename"]
            fname = self._normalize_filename(raw_fname)
            if fname is None:
                # self.log.info("  → Ignorado (nome não elegível): %s", raw_fname)
                continue
            dest = dest_dir / fname

            # Extrai o ano do nome do arquivo
            match = re.search(r"(20\d{2})", fname)
            ano = match.group(1) if match else None
            if ano:
                gcs_path = f"CPTM/bruto/{ano}/{fname}"
            else:
                gcs_path = f"CPTM/bruto/{fname}"

            # Checa no bucket se deve baixar
            if not should_download(gcs_path):
                # self.log.info("  → Já existe no bucket (pulado): %s", fname)
                continue

            if dest.exists():
                # self.log.info("  → Já existe local: %s", fname)
                downloaded += 1
                continue

            # self.log.info("  ↓ Baixando: %s", fname)
            if self.download(session, item["url"], dest):
                downloaded += 1
            time.sleep(1)

        if downloaded == 0:
            self.log.warning("[%s] Nenhum arquivo baixado.", self.name)
            return 0

        # --- ETAPA 3: Descompacta recursivamente ---
        self.extract_recursive(dest_dir)

        # --- ETAPA 4: Organiza por ano ---
        self.organize_by_year(dest_dir)

        # --- ETAPA 5: Padroniza nomes (específico CPTM) ---
        self._normalize_all_files(dest_dir)

        # Conta total final
        total_uteis = sum(1 for f in dest_dir.rglob("*") if f.is_file())

        # self.log.info("[%s] Concluído: %d arquivo(s) úteis em %s", self.name, total_uteis, dest_dir.name)
        return total_uteis


# Instância do crawler — o app.py usa isso
crawler = CptmCrawler()


def crawl(session: requests.Session, url: str, dest_dir: Path) -> int:
    """Função de compatibilidade — chama o crawler da instância."""
    return crawler.crawl(session, url, dest_dir)
