## IMPORTS
from os import environ
from subprocess import check_output
from prefect import Flow, task, unmapped
from prefect.engine.results.local_result import LocalResult
from functools import partial
from shared import *
from prefect.utilities.debug import raise_on_exception

## STUDY-WIDE CONSTANTS
constants = {}
constants['name'] = 'kyoto_preliminary'
constants['data_dir'] = DATA_ROOT + f'{constants["name"]}/data/'
constants['SUBJECTS_DIR'] = constants['data_dir'] + 'derivatives/freesurfer'
constants['scripts_dir'] = STUDY_SCRIPTS_ROOT + f'{constants["name"]}/'
constants['prefect_results_root'] = constants['scripts_dir'] + 'prefect_results/'
constants['log_dir'] = constants['scripts_dir'] + 'logs/'
constants['log_file'] = constants['log_dir'] + f'{date.today().strftime(r"%Y_%m_%d")}.log'

#logger = generate_logger(LOG_FILE)
result = LocalResult(dir=constants['prefect_results_root'])
print(environ['PREFECT__FLOWS__CHECKPOINTING'])

@task(target='{task_name}/{task_name}', checkpoint=True, nout=3)
def get_study_data():
    participant_ids = ['BROP45', 'WARR89', 'NOTS94', 'LANM82', 'VALJ80', 'LALM61', 'MACB51', 'METD50']
    scan_ids = ['BROP45', 'WARR89', 'NOTS94', 'LANM82', 'VALJ80', 'LALM61', 'MACB51', 'METD50']
    visit_dates = ['2021_12_17', '2021_12_22', '2021_12_14', '2021_12_29', '2021_12_22', '2021_12_16', '2021_12_13', '2021_12_20']

    return participant_ids, scan_ids, visit_dates

@task(target='{task_name}/{scan_id}', checkpoint=True)
def run_recon(participant_id: str, scan_id: str, scan_date: str, paths: Dict[str, str], study_constants: Dict[str, str], reconable_suffix: str):
    anat_dir = f'{paths["BIDS"]}/sub-{participant_id}_ses-{scan_id}/anat'
    anat_scans = listdir(anat_dir)
    full_paths = [join(anat_dir, anat_scan) for anat_scan in anat_scans]
    anat_scans = [scan for scan in full_paths if f'{reconable_suffix}.nii.gz' in scan]

    if not anat_scans:
        log('ERROR', 'Unable to locate T1 image for scan {scan_id}. Unable to perform recon-all.', study_constants['name'])
        raise ReconAllError(f'Could not find a recon-able scan for {scan_id}')

    T1_to_recon = ''
    if len(anat_scans) > 1:
        recon_config = read_json(join(study_constants['scripts_dir'], 'resources/recon_config.json'))
        for scan in recon_config:
            if scan['scan_id'] == scan_id and participant_id == scan['participant_id'] and scan_date == scan['scan_date']:
                T1_to_recon = scan['T1']
    else:
        T1_to_recon = anat_scans[0]

    if not T1_to_recon:
        log('ERROR', f'Could not find a recon-able scan for {scan_id}', study_constants['name'])
        raise ReconAllError(f'Could not find a recon-able scan for {scan_id}')

    environ['SUBJECTS_DIR'] = study_constants['SUBJECTS_DIR']
    output = run(f'recon-all -subjid sub-{participant_id}_ses-{scan_id} -i {T1_to_recon} -all', shell=True, stdout=PIPE, stderr=PIPE)
    if output.returncode != 0:
        log('ERROR', output.stderr.decode('utf-8'), study_constants['name'])
        raise ReconAllError(f'Unable to perform recon for {scan_id}')

    log('INFO', f'Successfully completed recon-all for {scan_id}', study_constants['name'])

## STUDY PIPELINE DEFINTION
with Flow(constants['name'], result=result) as flow:
    participant_ids, scan_ids, visit_dates = get_study_data()
    paths = bourget2bids.map(participant_ids, scan_ids, visit_dates, unmapped(constants))
    copy_DICOMS.map(participant_ids, scan_ids, paths, unmapped(constants))
    run_recon.map(participant_ids, scan_ids, visit_dates, paths, unmapped(constants), unmapped('uni-images_MP2RAGE'))

## RUN THE PIPELINE
with raise_on_exception():
    flow.run()

