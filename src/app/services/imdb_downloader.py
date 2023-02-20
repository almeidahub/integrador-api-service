from app.services.http_requests import HttpRequests
from multiprocessing.pool import ThreadPool
from app.utils.logs import log, error
from app.utils.decompress import Decompressor

from app.settings import DATA_PATH

class IMDbDownloader():

    def __init__(self):
        self.http_requests = HttpRequests()
        self.url = 'https://datasets.imdbws.com/'
        
        self.DATASET_NAMES = ['title.basics', 'title.akas', 'title.principals', 'name.basics']
    
    
    def download_dataset(self, dataset_name: str) -> str:
        
        if dataset_name not in self.DATASET_NAMES:
            raise ValueError(f'dataset_name inválido: {dataset_name}')
        
        file_name = f'{DATA_PATH}{dataset_name}.tsv.gz'
        endpoint = f'{self.url}{dataset_name}.tsv.gz'
        
        log(f'Baixando dataset {dataset_name} em {endpoint}...')
        
        try:
        
            response = self.http_requests.download(endpoint)
            
            with open(file_name, 'wb') as f:
                f.write(response.content)
            
            try:
                file_name = Decompressor.decompress_gzip(file_name)
            except (FileExistsError, FileNotFoundError) as e:
                error(e)
            
        except Exception as e:
            error(e)
        
        return file_name
    
    def download_all_datasets(self) -> list:
        
        log('Baixando todos os datasets')
        
        threads = len(self.DATASET_NAMES)
        
        results = ThreadPool(threads).imap_unordered(self.download_dataset, self.DATASET_NAMES)
        
        files_names = []
        
        for result in results:
            files_names.append(result)
            
        return files_names
    
    def download_title_basics(self) -> str:
        return self.download_dataset('title.basics')
        
    def download_title_akas(self) -> str:
        return self.download_dataset('title.akas')
    
    def download_title_principals(self) -> str:
        return self.download_dataset('title.principals')
    
    def download_name_basics(self) -> str:
        return self.download_dataset('name.basics')
        