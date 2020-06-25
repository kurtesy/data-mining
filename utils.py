import json
import time
import pandas as pd
from datetime import datetime

logs = []


def performance_logger(func):
    def inner_func(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        logs.append({func.__name__: end_time - start_time})
        logger()
        return result
    return inner_func


def logger():
    with open('logs.txt', 'a+') as f:
        f.write(json.dumps(logs[-1]))
        f.write('\n')


def read_company_domain_list():
    df = pd.read_csv('company_data/company_list_joined.csv')
    return df['domain']


def load_org_data():
    df = pd.read_csv('all_companies.csv')
    return df


def join_csv(files):
    dfs = []
    for file in files:
        dfs.append(pd.read_csv(file))
    final_df = pd.concat(dfs)
    write_data(final_df, 'company_data/company_list_joined.csv')


def get_epoch():
    return time.mktime(datetime.now().timetuple())


def write_data(dataFrame, file_name, directory=None, append=False):
    if directory:
        path = '/'.join([directory, file_name])
    else:
        path = file_name
    dataFrame.to_csv(path, encoding='utf-8', index=False, mode='a' if append else 'w',
                     header=False if append else dataFrame.columns)

# join_csv(['company_list_1.csv', 'company_list_101.csv'])


