## IMPORTS
from os import environ
from subprocess import check_output
from prefect import Flow, task, unmapped
from prefect.engine.results.local_result import LocalResult
from functools import partial
from shared import *
from prefect.utilities.debug import raise_on_exception
from redcap import Project
from typing import Dict

## STUDY-WIDE CONSTANTS
constants = {}
constants['name'] = 'apoe'
constants['data_dir'] = DATA_ROOT + f'{constants["name"]}/data/'
constants['SUBJECTS_DIR'] = constants['data_dir'] + 'derivatives/freesurfer'
constants['scripts_dir'] = STUDY_SCRIPTS_ROOT + f'{constants["name"]}/'
constants['prefect_results_root'] = constants['scripts_dir'] + 'prefect_results/'
constants['log_dir'] = constants['scripts_dir'] + 'logs/'
constants['log_file'] = constants['log_dir'] + f'{date.today().strftime(r"%Y_%m_%d")}.log'

#logger = generate_logger(LOG_FILE)
result = LocalResult(dir=constants['prefect_results_root'])
print(environ['PREFECT__FLOWS__CHECKPOINTING'])

@task(target='{task_name}/{today}', checkpoint=True, nout=3)
def get_study_data():
    redcap_api_url = 'https://redcap.partners.org/redcap/api/'
    redcap_api_token = 'D541E086C73133E66F8D2F29AB2C3F95'
    project = Project(redcap_api_url, redcap_api_token)
    records = project.export_records()

    scan_ids = [record['scanid'] for record in records]
    participant_ids = [scan_id[:9] for scan_id in scan_ids]
    visit_dates = [scan_id[10:20] for scan_id in scan_ids]

    return participant_ids, scan_ids, visit_dates

@task(target='{task_name}/{scan_id}', checkpoint=True)
def add_TRUST_eTE(participant_id: str, scan_id: str, paths: Dict[str, str]):
    BIDS_session_path_perfusion = join(paths['BIDS'], f'sub-{participant_id}_ses-{scan_id}', 'perf')
    perfusion_files = listdir(BIDS_session_path_perfusion)
    perfusion_TRUST_jsons = [join(BIDS_session_path_perfusion, file) for file in perfusion_files if '.json' in file and 'TRUST' in file and '_asl' in file]
    for json_file in perfusion_TRUST_jsons:
        metadata = read_json(json_file)
        metadata['EffectiveEchoTime'] = [0, 0, 0, 0, 0, 0, 40, 40, 40, 40, 40, 40, 80, 80, 80, 80, 80, 80, 120, 120, 120, 120, 120, 120]
        write_json(metadata, json_file)

    return 'done'

## STUDY PIPELINE DEFINTION
with Flow(constants['name'], result=result) as flow:
    participant_ids, scan_ids, visit_dates = get_study_data()
    paths = bourget2bids.map(participant_ids, scan_ids, visit_dates, unmapped(constants))
    copy_DICOMS.map(participant_ids, scan_ids, paths, unmapped(constants))
    add_TRUST_eTE.map(participant_ids, scan_ids, paths)

## RUN THE PIPELINE
#with raise_on_exception():
flow.run()
