'''
|               |                                     |
| -             | -                                   |
| DAS           | https://cmsweb.cern.ch/das/         |
| client        | https://github.com/dmwm/dasgoclient |
'''

import json
from multiprocessing import Pool
from subprocess import check_output

__all__ = ['DASError',
           'client', 'query_dataset', 'query_file_mp']

class DASError(Exception):
    __module__ = Exception.__module__

def client(*queries: str, to_json = True, **kwargs: str):
    if not queries or not kwargs:
        raise DASError(f'invalid query "{queries} {kwargs}"')
    result = check_output(f'dasgoclient{" -json" if to_json else ""} -query="{",".join(queries)} {" ".join(f"{k}={v}" for k, v in kwargs.items())}"', shell=True)
    if to_json:
        result = json.loads(result)
    else:
        result = result.decode('utf-8')
    return result

def query_file_mp(file):
    return {'path': file['file'][0]['name'],
            'nevents': file['file'][0]['nevents'],
            'site': client('site', file = file['file'][0]['name'], to_json = False).split('\n')}

def query_dataset(query: str):
    files = client('file', dataset = query)
    if not files:
        raise DASError(f'no files found for dataset "{filelist}"')
    filelist = {
        'nevents': 0,
        'nfiles': len(files),
        'files': [],
        'path': files[0]['file'][0]['dataset'],
        'das_query': query,
        'site': client('site', dataset = query, to_json = False)}
    with Pool(processes = len(files)) as pool:
        files = pool.map(query_file_mp, files)
    for file in files:
        filelist['files'].append(file)
        filelist['nevents'] += file['nevents']
    return filelist