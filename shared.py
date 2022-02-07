## IMPORTS
import logging
import pandas as pd
from os.path import join, exists
from os import makedirs, chdir, listdir
from prefect.utilities.logging import get_logger
from prefect import task
from subprocess import PIPE, getoutput, run, CalledProcessError
from my_utils import read_json, write_json
from datetime import date, datetime
from glob import glob
from typing import Dict
from organize_dicoms import organize_dicoms

## PIPELINE-WIDE OPTIONS

PIPELINE_ROOT = '/autofs/vast/bandlab/'
DATA_ROOT = PIPELINE_ROOT + 'studies/'
SCRIPTS_ROOT = PIPELINE_ROOT + 'scripts/'
STUDY_SCRIPTS_ROOT = SCRIPTS_ROOT + 'studies/'

## LOGGING SETUP

class FilewriteLogger(logging.StreamHandler):
    def __init__(self, logfile):
        self.logfile = logfile

    def emit(self, record):
        with open(str(self.logfile), 'a') as l:
            l.write(f'{record.levelname} | {record.asctime} | {record.msg}\n')

def generate_logger(logfile: str):
    logger = get_logger()
    logger.addHandler(FilewriteLogger(logfile))
    logger.setLevel('DEBUG')
    return logger

def log(level: str, msg: str, study_name: str):
    logfile = f'/autofs/vast/bandlab/scripts/studies/{study_name}/logs/{date.today().strftime(r"%Y_%m_%d")}.log'
    if level not in ['INFO', 'WARNING', 'ERROR']:
        raise Exception(f'Unrecognized logging level: {level}')
    with open(str(logfile), 'a') as l:
        l.write(f'{level} | {datetime.now().strftime(r"%Y-%m-%d %H:%M:%S-0400")} | {msg}\n')

## PIPELINE TASK DEFINITIONS

class BourgetError(Exception):
    """Exception raised when there's an issue retrieving data from Bourget"""
    pass

class ScanNotFoundError(BourgetError):
    """Exception raised when a scan isn't found on Bourget"""
    pass

class MultipleScansFoundError(BourgetError):
    """Exception raised when multiple scans matching a search are found on Bourget"""
    pass

class Dcm2BidsError(Exception):
    """Exception raised when dcm2bids fails"""
    pass

class T1ImageNotFound(Exception):
    pass

class ReconAllError(Exception):
    pass

def get_from_bourget(participant_id: str, scan_id: str, scan_date: str, study_name: str, study_scripts_dir):
    paths = getoutput(f"findsession -I {scan_id} -o {scan_date} | grep -Po '(?<=PATH   :  ).*'").splitlines()
    operators = getoutput(f"findsession -I {scan_id} -o {scan_date} | grep -P 'OPERATR'").splitlines()
    paths = [path for path,operator in zip(paths, operators) if ' : (' not in operator]

    if not paths:
        log('ERROR', f'No scan found on Bourget with scan ID {scan_id} and date {scan_date}', study_name)
        raise ScanNotFoundError()
    elif len(paths) > 1:
        bourget2bids_config = read_json(join(study_scripts_dir, 'resources/bourget2bids_config.json'))
        for scan in bourget2bids_config:
            if scan_id == scan['scan_id'] and participant_id == scan['participant_id'] and scan_date == scan['scan_date']:
                paths = [path for path in paths if path == scan['bourget_path']]

        if not paths:
            log('ERROR', f'Multiple scans found for {scan_id} on {scan_date} and no pointer found in config file.', study_name)
            raise MultipleScansFoundError()

    path = paths[0]

    log('INFO', f'Located scan {scan_id} in Bourget. Copying...', study_name)

    tmp_BIDS_dir = join(study_scripts_dir, f'tmp/bourget2bids/{scan_id}')
    if exists(tmp_BIDS_dir):
        run(f'rm -r {tmp_BIDS_dir}', shell=True)
    makedirs(tmp_BIDS_dir)
    chdir(tmp_BIDS_dir)

    output = run('dcm2bids_scaffold', shell=True, stdout=PIPE, stderr=PIPE)
    if output.returncode != 0:
        log('ERROR', output.stderr.decode('utf-8'), study_name)
        raise Dcm2BidsError()

    output = run(f'cp -r {path} ./sourcedata/', shell=True, stdout=PIPE, stderr=PIPE)
    if output.returncode != 0:
        log('ERROR', output.stderr.decode('utf-8'), study_name)
        run(f'rm -r {tmp_BIDS_dir}', shell=True)
        raise Dcm2BidsError()

    makedirs('./tmp_dcm2bids/helper/')

    output = run(f'dcm2niix -m y -ba n -o ./tmp_dcm2bids/helper/ ./sourcedata/', shell=True, stdout=PIPE, stderr=PIPE)
    if output.returncode not in  [0, 8]:
        log('ERROR', output.stderr.decode('utf-8'), study_name)
        run(f'rm -r {tmp_BIDS_dir}', shell=True)
        raise Dcm2BidsError()
    elif output.returncode == 8:
        log('WARNING', f'dcm2niix stderr: {output.stderr.decode("utf-8")}', study_name)

    run('rm ./tmp_dcm2bids/helper/*a.json', shell=True)
    run('rm ./tmp_dcm2bids/helper/*a.nii', shell=True)

    output = run(f'dcm2bids -d {path} -p {participant_id} -s {scan_id} -c {join(study_scripts_dir, "resources/dcm2bids_config.json")}', shell=True, stdout=PIPE, stderr=PIPE)
    if output.returncode != 0:
        log('ERROR', output.stderr.decode('utf-8'), study_name)
        run(f'rm -r {tmp_BIDS_dir}', shell=True)
        raise Dcm2BidsError()

    # Remove duplicates (for instance, in case of re-push to Bourget from the scanner)
    scan_session_dir = f'sub-{participant_id}/ses-{scan_id}'
    chdir(scan_session_dir)
    BIDS_subdirs = listdir()
    for subdir in BIDS_subdirs:
        chdir(subdir)
        BIDS_jsons = [file for file in listdir() if file.endswith('.json')]
        json_texts = [open(json).read() for json in BIDS_jsons]
        jsons_to_delete = []
        for i, json_text in enumerate(json_texts):
            for compared_json in json_texts[i + 1:]:
                if json_text == compared_json:
                    jsons_to_delete.append(compared_json)
                    continue

        jsons_to_delete = list(set(jsons_to_delete))
        niftis_to_delete = [json.replace('.json', '.nii') for json in jsons_to_delete]

        for json in jsons_to_delete:
            run(f'rm {json}', shell=True)

        for nifti in niftis_to_delete:
            run(f'rm {nifti}', shell=True)
        chdir('..')

    return path

def add_ASL_metadata(scan_id: str, study_name: str):
    def add_for_one_file(file_str: str, ASL_type: str, PLD, BGSuppression: bool,
        M0Type: str, TotalPairs: int, LD: float, context_list):

        asl_file = glob(f'./*/*/perf/*{file_str}*.nii.gz')
        if not asl_file:
            log('WARNING', f'No {file_str} scan found for {scan_id}', study_name)
        else:
            asl_file = asl_file[0]
            tsv = asl_file.replace('asl.nii.gz', 'aslcontext.tsv')
            with open(tsv, 'w') as f:
                f.writelines(context_list)
            json = asl_file.replace('.nii.gz', '.json')
            metadata = read_json(json)
            metadata['ArterialSpinLabelingType'] = ASL_type
            metadata['PostLabelingDelay'] = PLD
            metadata['BackgroundSuppression'] = BGSuppression
            metadata['M0Type'] = M0Type
            metadata['TotalAcquiredPairs'] = TotalPairs
            metadata['LabelingDuration'] = LD

            write_json(metadata, json)

    add_for_one_file('ep2dPCASL', 'PCASL', 1.2, False, 'Absent', 10, 1.514,
        ['volume_type\n'] + (['control\n', 'label\n'] * 10))
    add_for_one_file('mbPCASLhr', 'PCASL', [0.2, 0.7, 1.2, 1.7, 2.2], False, 'Included', 43, 1.5,
        ['volume_type\n'] + (['control\n', 'label\n'] * 43) + (['M0\n'] * 4))
    add_for_one_file('TRUST_rec', 'PASL', 1.022, False, 'Absent', 12, 1.5,
        ['volume_type\n'] + (['control\n', 'label\n'] * 12))

    log('INFO', f'Successfully completed dcm2bids conversion for {scan_id}.', study_name)


def copy_to_study_data_folder(participant_id: str, scan_id: str, tmp_BIDS_dir: str, study_name: str, study_data_dir: str):
    participant_exists = f'sub-{participant_id}' in listdir(join(study_data_dir, 'rawdata'))

    copy_from = join(tmp_BIDS_dir, f'sub-{participant_id}', f'ses-{scan_id}')
    copy_to = join(study_data_dir, f'rawdata/sub-{participant_id}')
    copy_to_ses = f'{copy_to}/sub-{participant_id}_ses-{scan_id}'

    if exists(copy_to_ses):
        log('INFO', f'{copy_to_ses} found to already exist. Deleting...', study_name)
        run(f'rm -r {copy_to_ses}', shell=True)
        log('INFO', f'{copy_to_ses} successfully deleted.', study_name)

    makedirs(copy_to, exist_ok=True)
    run(f'cp -r {copy_from} {copy_to_ses}', shell=True)
    log('INFO', f'{scan_id} BIDS directory successfully copied from temporary storage.', study_name)

    participant_table = join(study_data_dir, 'participants.tsv')
    df = pd.read_csv(participant_table, delimiter='\t')
    participant_in_table = participant_id in df['participant_id'].tolist()

    if not (participant_exists or participant_in_table):
        df = df.append({'participant_id': participant_id}, ignore_index=True)
        df.to_csv(participant_table, index=False, sep='\t')

        log('INFO', f'{participant_id} successfully added to participants.tsv', study_name)
    else:
        log('INFO', f'Participant {participant_id} already exists. Not adding to participants.tsv.', study_name)

    chdir('..')
    run(f'rm -r {scan_id}', shell=True)

    log('INFO', f'{scan_id} temporary BIDS folder successfully deleted.', study_name)
    return copy_to


@task(target='{task_name}/{scan_id}', checkpoint=True, nout=2)
def bourget2bids(participant_id: str, scan_id: str, scan_date: str, study_constants):
    tmp_BIDS_dir = join(study_constants['scripts_dir'], f'tmp/bourget2bids/{scan_id}')
    try:
        bourget_path = get_from_bourget(participant_id, scan_id, scan_date, study_constants['name'], study_constants['scripts_dir'])
        add_ASL_metadata(scan_id, study_constants['name'])
        BIDS_path = copy_to_study_data_folder(participant_id, scan_id, tmp_BIDS_dir, study_constants['name'], study_constants['data_dir'])
    except Exception as e:
        run(f'rm -r {tmp_BIDS_dir}', shell=True)
        raise e

    return dict(bourget=bourget_path, BIDS=BIDS_path)

@task(target='{task_name}/{scan_id}', checkpoint=True)
def copy_DICOMS(participant_id: str, scan_id: str, paths: Dict[str, str], study_constants: Dict):
    BIDS_sourcedata_path = paths['BIDS'].replace('rawdata', 'sourcedata')
    BIDS_sourcedata_session_path = join(BIDS_sourcedata_path, f'sub-{participant_id}_ses-{scan_id}')
    BIDS_sourcedata_session_path_DICOMs = join(BIDS_sourcedata_session_path,'DICOMs')

    if exists(BIDS_sourcedata_session_path_DICOMs):
        log('INFO', f'{BIDS_sourcedata_session_path_DICOMs} found to already exist. Deleting...', study_constants['name'])
        run(f'rm -r {BIDS_sourcedata_session_path_DICOMs}', shell=True)
        log('INFO', f'{BIDS_sourcedata_session_path_DICOMs} successfully deleted.', study_constants['name'])

    makedirs(BIDS_sourcedata_session_path, exist_ok=True)

    output = run(f'cp -r {paths["bourget"]} {BIDS_sourcedata_session_path_DICOMs}', shell=True, stdout=PIPE, stderr=PIPE)
    if output.returncode != 0:
        log('ERROR', output.stderr.decode('utf-8'), study_constants['name'])
        raise CalledProcessError(f'Failed to copy DICOMS from Bourget to sourcedata for {scan_id}.')

    run(f'chmod o+rx {BIDS_sourcedata_session_path_DICOMs}', shell=True)
    log('INFO', f'Successfully copied DICOMS from Bourget to sourcedata for {scan_id}.', study_constants['name'])

    organize_dicoms(BIDS_sourcedata_session_path_DICOMs, BIDS_sourcedata_session_path_DICOMs)
    log('INFO', f'Successfully organized DICOMS in sourcedata for {scan_id}.', study_constants['name'])
    return BIDS_sourcedata_path

def run_recon(scan_id: str, BIDS_path):
    pass
