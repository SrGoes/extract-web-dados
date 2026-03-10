"""
Classe base para todos os crawlers do Nyx.

Contém os métodos compartilhados que qualquer crawler pode usar:
  - download: baixar arquivo da internet
  - extract_archive: descompactar ZIP/RAR
  - extract_recursive: descompactar recursivamente (ZIPs dentro de ZIPs)
  - organize_by_year: organizar arquivos em subpastas por ano
  - safe_filename: limpar nomes de arquivo
  - extract_year: encontrar o ano no nome do arquivo

Cada crawler específico (CPTM, TVM, etc.) herda desta classe
e implementa apenas o método `crawl()` com a lógica do seu site.
"""

import os
import re
import time
import shutil
import logging
import zipfile
from pathlib import Path

import requests

from config import KEEP_EXTENSIONS, ARCHIVE_EXTENSIONS

log = logging.getLogger("nyx.base")


class BaseCrawler:
    """
    Classe base com funcionalidades comuns a todos os crawlers.

    Para criar um novo crawler, basta herdar esta classe e implementar
    o método `crawl(session, url, dest_dir) -> int`.
    """

    # Cada crawler define o "engine" que usa (para registro no config)
    engine: str = "bs4"

    # Nome legível do crawler (usado nos logs)
    name: str = "Base"

    def __init__(self):
        self.log = logging.getLogger(f"nyx.{self.name.lower()}")

    # ------------------------------------------------------------------
    # Utilitários de texto/arquivo
    # ------------------------------------------------------------------

    @staticmethod
    def safe_filename(text: str) -> str:
        """
        Converte texto em nome de arquivo seguro para o Windows.
        Remove caracteres proibidos como < > : " | ? *
        """
        fname = re.sub(r'[<>:"/\\|?*]', "_", text.strip())
        fname = re.sub(r"\s+", " ", fname).strip()
        return fname

    @staticmethod
    def extract_year(filename: str) -> str | None:
        """
        Procura um ano (2000–2099) no nome do arquivo.
        Retorna o ano como string ou None.
        """
        match = re.search(r"(20\d{2})", filename)
        return match.group(1) if match else None

    # ------------------------------------------------------------------
    # Download
    # ------------------------------------------------------------------

    def download(self, session: requests.Session, url: str, dest: Path) -> bool:
        """
        Baixa um arquivo da internet via streaming e salva em `dest`.
        Retorna True se deu certo, False se deu erro.
        """
        try:
            resp = session.get(url, stream=True, timeout=120, allow_redirects=True)
            resp.raise_for_status()

            dest.parent.mkdir(parents=True, exist_ok=True)
            with open(dest, "wb") as f:
                for chunk in resp.iter_content(chunk_size=8192):
                    f.write(chunk)

            size_kb = dest.stat().st_size / 1024
            self.log.info("  ✓ Baixado: %s (%.1f KB)", dest.name, size_kb)
            return True
        except Exception as e:
            self.log.error("  ✗ Erro ao baixar %s: %s", url, e)
            return False

    # ------------------------------------------------------------------
    # Extração de compactados
    # ------------------------------------------------------------------

    def extract_archive(self, archive_path: Path, dest_dir: Path) -> list[Path]:
        """
        Extrai um ZIP para dest_dir.
        Retorna lista de arquivos extraídos.
        """
        extracted: list[Path] = []
        ext = archive_path.suffix.lower()

        try:
            if ext == ".zip":
                with zipfile.ZipFile(archive_path, "r") as zf:
                    for member in zf.namelist():
                        if member.endswith("/"):
                            continue
                        fname = self.safe_filename(os.path.basename(member))
                        if not fname:
                            continue
                        dest = dest_dir / fname
                        with zf.open(member) as src:
                            with open(dest, "wb") as dst:
                                shutil.copyfileobj(src, dst)
                        extracted.append(dest)
                        self.log.info("    📦 Extraído: %s", fname)
            else:
                self.log.warning("    Formato não suportado para extração: %s", ext)

        except Exception as e:
            self.log.error("    ✗ Erro ao extrair %s: %s", archive_path.name, e)

        return extracted

    def extract_recursive(self, dest_dir: Path) -> int:
        """
        Descompacta todos os compactados na pasta recursivamente.
        Remove os compactados após extração e descarta arquivos inúteis.
        Retorna quantidade de arquivos úteis restantes.
        """
        max_depth = 5
        for depth in range(max_depth):
            archives = [
                f for f in dest_dir.iterdir()
                if f.is_file() and f.suffix.lower() in ARCHIVE_EXTENSIONS
            ]
            if not archives:
                break

            self.log.info("  Extraindo %d compactado(s) (nível %d)...", len(archives), depth + 1)
            for arc in archives:
                extracted = self.extract_archive(arc, dest_dir)
                if extracted:
                    arc.unlink()
                    self.log.info("    🗑 Removido: %s", arc.name)
                else:
                    self.log.warning("    ⚠ Nada extraído de %s, mantendo arquivo", arc.name)

        # Faxina: remove arquivos que não são dados úteis
        removed = 0
        kept = 0
        for f in dest_dir.iterdir():
            if not f.is_file():
                continue
            if f.suffix.lower() in KEEP_EXTENSIONS:
                kept += 1
            elif f.suffix.lower() in ARCHIVE_EXTENSIONS:
                kept += 1
            else:
                self.log.info("    🗑 Arquivo descartado (tipo não útil): %s", f.name)
                f.unlink()
                removed += 1

        if removed:
            self.log.info("  %d arquivo(s) descartado(s), %d mantido(s)", removed, kept)

        return kept

    # ------------------------------------------------------------------
    # Organização por ano
    # ------------------------------------------------------------------

    def organize_by_year(self, dest_dir: Path) -> int:
        """
        Move arquivos soltos na pasta para subpastas por ano (2019/, 2020/, ...).
        Retorna quantidade de arquivos processados.
        """
        moved = 0
        no_year = 0

        files = [f for f in dest_dir.iterdir() if f.is_file()]
        for f in files:
            year = self.extract_year(f.name)
            if year:
                year_dir = dest_dir / year
                year_dir.mkdir(exist_ok=True)
                dest = year_dir / f.name
                if dest.exists():
                    f.unlink()
                else:
                    f.rename(dest)
                moved += 1
            else:
                no_year += 1
                self.log.warning("    ⚠ Sem ano identificado: %s", f.name)

        if moved:
            years = sorted({d.name for d in dest_dir.iterdir() if d.is_dir()})
            self.log.info("  📂 Organizado em %d pasta(s): %s", len(years), ", ".join(years))

        return moved + no_year

    # ------------------------------------------------------------------
    # Método principal (deve ser sobrescrito pelos crawlers filhos)
    # ------------------------------------------------------------------

    def crawl(self, session: requests.Session, url: str, dest_dir: Path) -> int:
        """
        Método principal de coleta. Deve ser implementado por cada crawler.

        Parâmetros:
            session:  requests.Session configurada com headers
            url:      URL da página a ser coletada
            dest_dir: pasta de destino dos arquivos

        Retorna:
            Quantidade de arquivos úteis coletados.
        """
        raise NotImplementedError(
            f"O crawler '{self.name}' não implementou o método crawl()."
        )
