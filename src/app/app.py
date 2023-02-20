from app.services.imdb_etl import IMDBETL


class App():
    def start_app():
        IMDBETL.main()