from app.services.imdb_downloader import IMDbDownloader
from app.utils.logs import log

class Integrator():
    
    @classmethod
    def main(cls):
        log('Inciando pipeline...')
        
        cls.coleta_de_dados()

    
    @classmethod
    def coleta_de_dados(cls):
        
        log('Iniciando coleta de dados...')
        
        imdb_downloader = IMDbDownloader()
        
        datasets_file_names = imdb_downloader.download_all_datasets()
        
        log(f'Dados coletados: {datasets_file_names}')