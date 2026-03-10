
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utils.checkBucket import should_download


def test_should_download():
    class DummyBlob:
        def __init__(self, exists):
            self._exists = exists
        def exists(self):
            return self._exists
    class DummyBucket:
        def list_blobs(self, prefix=None, max_results=None):
            # Simula existência de arquivos na pasta do ano
            if prefix == "CPTM/bruto/2026/":
                # Ano atual: sempre baixa
                return ["algum-arquivo.xlsx"]
            if prefix == "CPTM/bruto/2019/":
                # Pasta antiga existe
                return ["arquivo_antigo.xlsx"]
            # Pasta não existe
            return []
    # Monkeypatch manual: sobrescreve get_bucket
    import utils.checkBucket as checkBucket
    original_get_bucket = checkBucket.get_bucket if hasattr(checkBucket, 'get_bucket') else None
    checkBucket.get_bucket = lambda: DummyBucket()

    try:
        # Ano atual deve baixar sempre
        assert should_download("CPTM/bruto/2026/arquivo_2026.xlsx") is True
        # Pasta antiga já existente deve ser pulada
        assert should_download("CPTM/bruto/2019/arquivo_antigo.xlsx") is False
        # Pasta inexistente deve baixar
        assert should_download("CPTM/bruto/2025/novo_arquivo.xlsx") is True
        print("test_should_download: OK")
    finally:
        # Restaura função original
        if original_get_bucket:
            checkBucket.get_bucket = original_get_bucket


def run():
    try:
        test_should_download()
    except AssertionError as e:
        print("test_should_download: FALHOU", e)
    except Exception as ex:
        print(f"test_should_download: ERRO INESPERADO: {ex}")
    else:
        print("test_should_download: OK")

if __name__ == "__main__":
    run()
