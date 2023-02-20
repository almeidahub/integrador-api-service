from app.services.imdb_downloader import IMDbDownloader
from app.utils.logs import log, error, warning
import re
import nbformat
from nbconvert.preprocessors import ExecutePreprocessor

class IMDBETL():
    
    @classmethod
    def main(cls):
        log('Inciando ETL...')
        
        arquives = cls.extract()
        cls.transform_and_load(arquives)


    
    @classmethod
    def extract(cls) -> list:
        
        log('Iniciando coleta de dados...')
        
        imdb_downloader = IMDbDownloader()
        
        datasets_file_names = imdb_downloader.download_all_datasets()
        
        log(f'Dados coletados: {datasets_file_names}')
        
        return datasets_file_names
    
    
    @classmethod
    def transform_and_load_data(cls, dataset: str) -> None:
        
        try:
            
            data_name = re.findall(r'.+/(\w+\.\w+)\.tsv', dataset)[0]
            
            log(f'Processando {data_name}')
            
            if data_name in 'name.basics':
                warning(f'{data_name} é processado junto do title.principals')
                
                return
            
            with open(f'app/notebooks/{data_name}.ipynb') as ff:
                nb_in = nbformat.read(ff, nbformat.NO_CONVERT)
                
            ep = ExecutePreprocessor(timeout=600, kernel_name='python3')
            ep.preprocess(nb_in)
            
            log('Ok!')
            
        except Exception as e:
            error(e)
            
            
    @classmethod
    def transform_and_load(cls, datasets: list[str]) -> None:
        
        log('Iniciando etapa de transformação e carregamento dos dados...')
        
        for dataset in datasets:
            cls.transform_and_load_data(dataset)
            
    
    
