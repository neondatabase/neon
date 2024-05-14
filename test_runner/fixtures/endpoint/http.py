import requests
from requests.adapters import HTTPAdapter

class EndpointHttpClient(requests.Session):
    def __init__(
        self,
        port: int,
    ):
        super().__init__()
        self.port = port

        self.mount("http://", HTTPAdapter())
    
    def catalog_objects(self):
        res = self.get(f"http://localhost:{self.port}/catalog/objects")
        res.raise_for_status()
        return res.json()
    
    def catalog_schema_ddl(self, database: str):
        res = self.get(f"http://localhost:{self.port}/catalog/database_ddl?database={database}")
        res.raise_for_status()
        return res.text