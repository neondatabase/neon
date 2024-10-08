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

    def dbs_and_roles(self):
        res = self.get(f"http://localhost:{self.port}/dbs_and_roles")
        res.raise_for_status()
        return res.json()

    def database_schema(self, database: str):
        res = self.get(f"http://localhost:{self.port}/database_schema?database={database}")
        res.raise_for_status()
        return res.text

    def installed_extensions(self):
        res = self.get(f"http://localhost:{self.port}/installed_extensions")
        res.raise_for_status()
        return res.json()
