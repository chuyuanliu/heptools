'''
|               |                                   |
| -             | -                                 |
| DAS           | https://cmsweb.cern.ch/das/       |
| client        | https://github.com/dmwm/DBSClient |
'''
# TODO migrate to DBSClient
# url https://cmsweb.cern.ch/dbs/prod/global/DBSReader/
import json
from multiprocessing import Pool
from subprocess import check_output

__all__ = ['DASError', 'DAS']

class DASError(Exception):
    __module__ = Exception.__module__

class DAS:
    @staticmethod
    def client(*queries: str, to_json = True, **kwargs: str):
        if not queries or not kwargs:
            raise DASError(f'invalid query "{queries} {kwargs}"')
        result = check_output(f'dasgoclient{" -json" if to_json else ""} -query="{",".join(queries)} {" ".join(f"{k}={v}" for k, v in kwargs.items())}"', shell=True)
        if to_json:
            result = json.loads(result)
        else:
            result = result.decode('utf-8')
        return result

    @staticmethod
    def query_file_mp(file):
        return {'path': file['file'][0]['name'],
                'nevents': file['file'][0]['nevents'],
                'site': DAS.client('site', file = file['file'][0]['name'], to_json = False).split('\n')}

    @staticmethod
    def query_dataset(query: str):
        files = DAS.client('file', dataset = query)
        if not files:
            raise DASError(f'no files found for dataset "{query}"')
        with Pool(processes = len(files)) as pool:
            return {'files': pool.map(DAS.query_file_mp, files)}