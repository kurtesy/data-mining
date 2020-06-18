import requests
import pprint
import json
import pandas as pd
from Queue import Queue
from threading import Thread
from time import sleep
from contextlib import closing
from utils import performance_logger, write_data, read_company_domain_list, load_org_data

BASE_URL = 'https://api.apollo.io/v1/'
KEY1 = 'W4NvoAW8V-TqJcStCanr9g'
KEY2 = 'A2629-avgEoxHQ8UJv7uKg'
API_KEY = KEY2  #'_liZuW6ZrPokGrHv-ZoOGg'
required_fields = ['id', 'name', 'industry', 'website_url', 'linkedin_url', 'facebook_url',
                       'twitter_url', 'raw_address', 'short_description', 'estimated_num_employees',
                       'annual_revenue']
people_fields = ['name', 'title', 'city', 'linkedin_url', 'email', 'organization_id']
page_limit = 10
rows = {}
df = {}
data = {}
orgDataFrame = None
orgRow = {}


class CompanyWorker(Thread):
    def __init__(self, queue):
        Thread.__init__(self)
        self.queue = queue

    def run(self):
        while True:
            page_number = self.queue.get()
            try:
                get_companies_list(page_number)
            finally:
                self.queue.task_done()


class PeopleWorker(Thread):
    def __init__(self, queue):
        Thread.__init__(self)
        self.queue = queue

    def run(self):
        while True:
            company_id = self.queue.get()
            try:
                get_peoples_data(company_id)
            finally:
                self.queue.task_done()


def swap_api_key():
    global API_KEY
    if API_KEY == KEY1:
        API_KEY = KEY2
    else:
        API_KEY = KEY1


def generate_api_key():
    url = BASE_URL+'teams/generate_api_key'

@performance_logger
def get_company_data(company):
    params = {'domain': company, 'api_key': API_KEY}
    response = requests.get(url=BASE_URL+'organizations/enrich', params=params)
    if response.status_code == 401:
        swap_api_key()
    try:
        company_data = response.json()
        company_data = company_data['organization']
    except Exception as err:
        print err
        return {fields: '' for fields in required_fields}
    result = {field: str(company_data[field]).rstrip().replace('"', '') if type(company_data[field]) == str else company_data[field]
              for field in required_fields if field in company_data}
    result.update({'domain': company})
    return result


@performance_logger
def get_peoples_data(company):
    url = BASE_URL + 'mixed_people/search'
    rows = []
    orgRow = orgDataFrame[orgDataFrame.domain == company]
    print orgRow.head(5)
    for page in range(1, page_limit):
        print page
        params = {'api_key': API_KEY, 'q_organization_domains': company, 'per_page': 200}
        response = requests.post(url=url, params=params)
        data = response.json()
        for person in data['people']:
            temp = {fields: person[fields] for fields in people_fields}
            temp.update()
            rows.append(temp)
    df = pd.DataFrame(rows)
    df['key'] = 0
    orgRow['key'] = 0
    result = pd.merge(df, orgRow, how='outer', on=['key'])
    write_data(result, 'final_list_{}.csv', directory='people_data', append=True)


@performance_logger
def get_users_list():
    params = {'api_key': API_KEY}
    response = requests.post(url=BASE_URL + 'users/search', params=params)
    print json.dumps(response.json())


@performance_logger
def get_companies_list(page_start=1):
    alphabetic_sampling = 'abcdefghijklmnopqrstuvwxyz'
    rows[page_start] = []
    url = BASE_URL + 'mixed_companies/search'
    retry = 0
    failed_pages = []
    for page in range(page_start, page_start+page_limit):
        if retry > 3:
            failed_pages.append(page)
            break
        print page
        try:
            params = {'api_key': API_KEY, 'q_organization_name': 'a', 'page': page, 'per_page': 10}
            response = requests.post(url=url, params=params)
            if response.status_code == 401:
                swap_api_key()
            data[page_start] = response.json()
            # print json.dumps(data)
            for org in data[page_start]['organizations']:
                domain = org['primary_domain']
                if domain not in orgRow:
                    orgRow[domain] = get_company_data(domain)
                rows[page_start].append({'id': org['id'], 'name': org['name'], 'domain': domain})
            retry = 0
        except (ValueError, KeyError) as value_error:
            retry += 1
            print 'Error', value_error
            continue
    df[page_start] = pd.DataFrame(rows[page_start], columns=['id', 'name', 'domain'])
    write_data(df[page_start], 'company_list_{}.csv'.format(page_start), directory='company_data')
    print 'Done'


def main_company():
    starts = range(1, 201, 100)
    # Create a queue to communicate with the worker threads
    queue = Queue()
    # Create 2 worker threads
    for thre in range(2):
        worker = CompanyWorker(queue)
        worker.daemon = True
        worker.start()
    for start in starts:
        queue.put(start)
    queue.join()
    print json.dumps(orgRow)
    orgData = pd.DataFrame(columns=required_fields + ['domain'])
    orgData = orgData.append(orgRow.values())
    write_data(orgData, 'all_companies.csv')


def main_people():
    global orgDataFrame
    company = read_company_domain_list()[10:20]
    orgDataFrame = load_org_data()
    # Create a queue to communicate with the worker threads
    queue = Queue()
    # Create 2 worker threads
    for thre in range(1):
        worker = PeopleWorker(queue)
        worker.daemon = True
        worker.start()
    for domain in company:
        queue.put(domain)
    queue.join()


if __name__ == '__main__':
    # main_company()
    main_people()
    # get_peoples_data('54a11d5b69702d97c1b31201')
    # get_companies_list(12)
    # print json.dumps(orgRow)
    # orgData = pd.DataFrame(columns=required_fields + ['domain'])
    # orgData = orgData.append(orgRow.values())
    # write_data(orgData, 'all_companies.csv', append=True)
