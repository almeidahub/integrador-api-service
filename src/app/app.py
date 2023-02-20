from app.services.imdb_etl import IMDBETL


class App():
    
    @classmethod
    def start_app(cls):
        IMDBETL.main()