"""
|               |                                   |
| -             | -                                 |
| DAS           | https://cmsweb.cern.ch/das/       |
| Rucio         | https://rucio.cern.ch/            |
| DBS3-Client   | https://github.com/dmwm/DBSClient |
"""

from dbs.apis.dbsClient import DbsApi
from rucio.client import Client as RucioClient

__all__ = ["DASError", "DAS"]


class DASError(Exception):
    __module__ = Exception.__module__


class DAS:
    dbs3 = DbsApi("https://cmsweb.cern.ch/dbs/prod/global/DBSReader/")
    rucio = RucioClient()

    @classmethod
    def query(cls, dataset: str):
        # DBS3 query
        files = cls.dbs3.listFiles(dataset=dataset, detail=True)
        if not files:
            raise DASError(f'no files found for dataset "{dataset}"')
        files = {
            file["logical_file_name"]: {
                "path": file["logical_file_name"],
                "nevents": file["event_count"],
            }
            for file in files
        }
        # Rucio query
        for replicas in cls.rucio.list_replicas(
            dids=[{"scope": "cms", "name": file} for file in files], schemes=["root"]
        ):
            files[replicas["name"]]["site"] = [
                site
                for site, status in replicas["states"].items()
                if status == "AVAILABLE"
            ]
        return {"files": list(files.values())}
