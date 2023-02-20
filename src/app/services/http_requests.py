import requests

from app.utils.logs import log, error

class HttpRequests():
    """Classe para fazer download de arquivos da web."""

    @staticmethod
    def download(url, compression=None):
        """Baixa um arquivo da web.

        Args:
            url (str): A URL do arquivo a ser baixado.
            compression (str, optional): O tipo de compressão, se houver. Padrão é None.

        Returns:
            str: O nome do arquivo baixado.
        """ 
        
        # Verifica se a URL é válida
        if not url:
            raise ValueError("URL inválida")


        log(f'Baixando de {url}...')
        
        # Tenta fazer o download do arquivo
        try:
            response = requests.get(url)
            response.raise_for_status()

        except requests.exceptions.RequestException as e:
            error(f"Erro ao baixar arquivo: {e}")
            raise
            

        return response