import sys
import argparse
import os
import traceback
import pandas as pd
import requests
import json


class SaveRowException(Exception):
    def __init__(self, row_number=None, traceback_log=None, message=None):
        '''
        Custom hack exception for logging
        '''
        with open("last_row.txt","w+") as f:
            f.write(str(traceback_log) + '\n')
            f.write(str(message) + '\n')
            f.write(str(row_number))

class HuntFlowApi:
    def __init__(self, token):
        self.token = token

    def test_token(self):
        url = 'https://dev-100-api.huntflow.dev/accounts'
        self.headers = {
            'User-Agent': 'App/1.0 (incaseoffire@example.com)',
            'Authorization': f'Bearer {self.token}'
        }
        response = requests.get(url=url, headers=self.headers)
        if response.status_code != 200:
            raise Exception(response.text)
        return response

    def upload_file(self, path_to_file):
        url = "https://dev-100-api.huntflow.dev/account/2/upload"
        self.headers = {
                'User-Agent': 'App/1.0 (incaseoffire@example.com)',
                'X-File-Parse': 'true',
                'Authorization': f'Bearer {self.token}'
        }
        file_extens = {
            'doc': 'msword',
            'pdf': 'pdf'
        }
        files = [
            (
                'file', 
                (
                    os.path.basename(path_to_file),
                    open(path_to_file,'rb'),
                    'application/' + file_extens.get(os.path.splitext(path_to_file)[1][1:], '')
                ),
            )
        ]
        response = requests.post(url, headers=self.headers, files=files)
        if response.status_code != 200:
            raise Exception(response.text)
        return response

    def add_applicant_to_db(self, data):
        url = "https://dev-100-api.huntflow.dev/account/2/applicants"
        self.headers = {
                'User-Agent': 'App/1.0 (incaseoffire@example.com)',
                'Authorization': f'Bearer {self.token}'
        }
        response = requests.post(url, json=data, headers=self.headers)
        if response.status_code != 200:
            raise Exception(response.text)
        return response

    def get_vacancies(self):
        url = "https://dev-100-api.huntflow.dev/account/2/vacancies"
        self.headers = {
                'User-Agent': 'App/1.0 (incaseoffire@example.com)',
                'Authorization': f'Bearer {self.token}'
        }
        response = requests.get(url, headers=self.headers)
        if response.status_code != 200:
            raise Exception(response.text)
        return response

    def get_company_statuses(self):
        url = "https://dev-100-api.huntflow.dev/account/2/vacancy/statuses"
        self.headers = {
                'User-Agent': 'App/1.0 (incaseoffire@example.com)',
                'Authorization': f'Bearer {self.token}'
        }
        response = requests.get(url, headers=self.headers)
        if response.status_code != 200:
            raise Exception(response.text)
        return response

    def add_applicant_to_vacancy(self, data):
        url = f"https://dev-100-api.huntflow.dev/account/2/applicants/{data['id']}/vacancy"
        self.headers = {
                'User-Agent': 'App/1.0 (incaseoffire@example.com)',
                'Authorization': f'Bearer {self.token}'
        }
        response = requests.post(url, json=data, headers=self.headers)
        if response.status_code != 200:
            raise Exception(response.text)
        return response
    

def createParser ():
    '''
    Add neccessary options for command line launching
    '''
    parser = argparse.ArgumentParser()
    parser.add_argument ('-t', '--token')
    parser.add_argument ('-p', '--path', default=os.path.dirname(os.path.abspath(__file__)))
    parser.add_argument ('-c', '--cont')
    return parser


def get_db_xlsx_if_exists(root: str, files: list, db_ext: str, match_db: list):
    '''
    Find and read xlsx db
    '''
    for file in files:
        if not (file.endswith(db_ext) and any(x in file for x in match_db)):
            continue
        # troubles with memory
        df = pd.read_excel(os.path.join(root,file), engine='openpyxl')
        df.columns = ['position', 'full_name', 'money', 'comment', 'status']
    
    return df.to_dict('records')


def get_files_n_questionnaires(base_root: str, db_ext: str, match_db: list) -> dict:
    '''
    Read and process path
    '''
    vacansy_files = {}
    for (root, dirs, files) in os.walk(base_root, topdown=True):

        if root == base_root:
            data = get_db_xlsx_if_exists(root, files, db_ext, match_db)
            continue

        dir_name = os.path.basename(root).strip()
        vacansy_files[dir_name] = {}

        for file in files:
            vacansy_files[dir_name].update({
                os.path.splitext(file)[0]: os.path.join(root, file)
            })

    # add file path to db dict
    for row in data:
        vacancy = row['position'].strip()
        if vacancy not in vacansy_files:
            continue
        first_name, last_name = row['full_name'].split()[:2]
        for file_name, file_path in vacansy_files[vacancy].items():
            if first_name in file_name and last_name in file_name:
                row['file'] = file_path
    return data


def prepare_data(row: dict) -> dict:
    '''
    Prepare data from file for adding applicant to huntflow db
    '''
    try:
        if not row.get('file', ''):
            raise Exception(f'There is no file for {row}')

        resp = json.loads(
            huntflow_obj.upload_file(
                path_to_file=row['file']
            ).text
        )
        
        file_data = {
            'externals': [
                {
                    'data': {
                        'body': resp.get('text', ''),
                    },
                    'auth_type': 'NATIVE',
                    'files': [
                        {
                            'id': resp['id']
                        }
                    ],
                }
            ]
        }

        if type(resp['fields'].get('name', '')) is dict:
            
            file_data.update({
                'last_name': resp['fields']['name'].get('last', ''),
                'first_name': resp['fields']['name'].get('first', ''),
                'middle_name': resp['fields']['name'].get('middle', ''),
            })

        if type(resp['fields'].get('phones', '')) is list:
            file_data.update({'phone': resp.get('phones', '')})

        if resp['fields'].get('email', ''):
            file_data.update({'email': resp['fields'].get('email', '')})
        
        if type(resp['fields'].get('birthdate', '')) is dict:
            file_data.update({
                'birthday_day': resp['fields']['birthdate'].get('day', ''),
                'birthday_month': resp['fields']['birthdate'].get('month', ''),
                'birthday_year': resp['fields']['birthdate'].get('year', ''),
            })

        if type(resp['photo']) is dict:
            file_data.update({'photo': resp['photo'].get('id', '')})
        file_data['position'] = row['position']
        file_data['money'] = row['money']
        row['add_applicant'] = file_data
    except:
        raise Exception(traceback.format_exc())
    return row


def get_vacancies_n_statuses_ids(huntflow_obj) -> tuple:
    '''
    Get vacancies n statuses ids from api
    '''
    # create dict vacancy: id
    vacancies_dict = {}
    for vacancy in json.loads(huntflow_obj.get_vacancies().text)['items']:
        vacancies_dict[vacancy['position']] = vacancy['id']

    # create dict status: id
    statuses_dict = {}
    for vacancy in json.loads(huntflow_obj.get_company_statuses().text)['items']:
        statuses_dict[vacancy['name']] = vacancy['id']
    return vacancies_dict, statuses_dict


def prepare_data_to_add_to_vacancy(row: dict, vacancies_dict: dict, statuses_dict: dict) -> dict: 
    '''
    Preparing json for adding applicant to vacancy
    '''
    for_add_to_vacancy = {}
    for_add_to_vacancy['status'] = statuses_dict.get(row['status'], '')
    for_add_to_vacancy['vacancy'] = vacancies_dict.get(row['position'], '')
    for_add_to_vacancy['files'] = row['add_applicant']['externals'][0]['files']
    for_add_to_vacancy['id'] = row['applicant_id']
    for_add_to_vacancy['comment'] = row.get('comment', '')
    row['for_add_to_vacancy'] = for_add_to_vacancy
    return row


if __name__ == '__main__':
    parser = createParser()
    namespace = parser.parse_args(sys.argv[1:])
    token = namespace.token

    db_ext = r".xlsx"
    match_db = ['база', 'database']
 
    db_data = get_files_n_questionnaires(
        base_root=namespace.path, 
        db_ext=db_ext, 
        match_db=match_db
    )
    if namespace.cont:
        try:
            with open("last_row.txt","r") as f:
                    line_number = int(f.readlines()[-1])
        except:
            raise SaveRowException(
                traceback_log=traceback.format_exc()
            )
 
    huntflow_obj = HuntFlowApi(token)
    vacancies_dict, statuses_dict = get_vacancies_n_statuses_ids(huntflow_obj)

    try:
        for i, row in enumerate(db_data):
            if namespace.cont and 'line_number' in locals():
                if i <= line_number:
                    continue

            row = prepare_data(row)

            resp = json.loads(
                    huntflow_obj.add_applicant_to_db(data=row['add_applicant']).text
            )
            row['applicant_id'] = resp['id']

            row = prepare_data_to_add_to_vacancy(
                row=row, 
                vacancies_dict=vacancies_dict, 
                statuses_dict=statuses_dict
            )

            # json.loads(huntflow_obj.add_applicant_to_vacancy(data=row['for_add_to_vacancy']).text)
            huntflow_obj.add_applicant_to_vacancy(data=row['for_add_to_vacancy'])
    except:
        print(f'{i+1}/{len(db_data)} lines successfully uploaded')
        raise SaveRowException(
            row_number=i, 
            traceback_log=traceback.format_exc()
        )
    print(f'{i+1}/{len(db_data)} lines successfully uploaded')