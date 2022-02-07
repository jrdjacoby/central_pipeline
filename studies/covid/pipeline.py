## IMPORTS
from subprocess import check_output
from prefect import Flow, task, unmapped
from prefect.engine.results.local_result import LocalResult
from functools import partial
from shared import *
from prefect.utilities.debug import raise_on_exception
from redcap import Project

## STUDY-WIDE CONSTANTS
constants = {}
constants['name'] = 'covid'
constants['data_dir'] = DATA_ROOT + f'{constants["name"]}/data'
constants['scripts_dir'] = STUDY_SCRIPTS_ROOT + f'{constants["name"]}/'
constants['prefect_results_root'] = constants['scripts_dir'] + 'prefect_results/'
constants['log_dir'] = constants['scripts_dir'] + 'logs/'
constants['log_file'] = constants['log_dir'] + f'{date.today().strftime(r"%Y_%m_%d")}.log'

#logger = generate_logger(LOG_FILE)
result = LocalResult(dir=constants['prefect_results_root'])

@task(target='{task_name}/{today}', checkpoint=True, nout=3)
def get_study_data():
    # redcap_api_url = 'https://redcap.partners.org/redcap/api/'
    # redcap_api_token = 'D541E086C73133E66F8D2F29AB2C3F95'
    # project = Project(redcap_api_url, redcap_api_token)
    # records = project.export_records()
    
    # scan_ids = [record['scanid'] for record in records]
    # participant_ids = [scan_id[:9] for scan_id in scan_ids]
    # visit_dates = [scan_id[10:20] for scan_id in scan_ids]

    participant_ids = ['COHM50', 'MCGK58', 'BARE62', 'BAND19762', 'COHM50']
    scan_ids = ['COHM50_V2', '21.10.25-10:47:45-DST-1.3.12.2.1107.5.2.43.67026', 'BARE62', 'BAND19762_2021_12_10_COVID', 'COHM50']
    visit_dates = ['2021_08_24', '2021_10_25', '2021_11_22', '2021_12_10', '2021_07_28']

    return participant_ids, scan_ids, visit_dates

## STUDY PIPELINE DEFINTION
with Flow(constants['name'], result=result) as flow:
    participant_ids, scan_ids, visit_dates = get_study_data()
    paths = bourget2bids.map(participant_ids, scan_ids, visit_dates, unmapped(constants))
    copy_DICOMS.map(participant_ids, scan_ids, paths, unmapped(constants))

## RUN THE PIPELINE
with raise_on_exception():
    flow.run()

