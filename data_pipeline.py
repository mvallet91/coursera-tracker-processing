import subprocess
import os
import gzip
import shutil
from zipfile import ZipFile
from datetime import date, timedelta, datetime

def check_status(log_file, user_id_hashing):
    today = (date.today()).strftime('%Y-%m-%d')
    yesterday = (date.today() - timedelta(1)).strftime('%Y-%m-%d')
    to_request = ['TABLES', 'CLICKSTREAM']
    downloaded = []
    p = subprocess.Popen(['/usr/local/bin/courseraresearchexports', 'jobs', 'get_all'], stdout=subprocess.PIPE)
    out, err = p.communicate()
    lines = out.strip().decode().split('\n')

    for line in lines:
        line = line.strip().split()
        job_id = line[2]
        export_type = line[4]
        if today in line:
            to_request.remove(export_type)
            if 'SUCCESSFUL' in line:
                if export_type in line:
                    download_job(job_id, log_file)
                    downloaded.append(export_type)
    if 'TABLES' in to_request:
        get_tables(log_file, user_id_hashing)
    if 'CLICKSTREAM' in to_request:
        get_clickstream(log_file, yesterday, yesterday)
    return downloaded

def get_tables(path, user_id_hashing):
    course_slug = 'assessment-higher-education'
    purpose = 'diy widget ' + str(date.today())

    request_tables = ['/usr/local/bin/courseraresearchexports', 'jobs', 'request',
        'tables', '--course_slug', course_slug,
        '--purpose', purpose,
        '--user_id_hashing', user_id_hashing]

    with open(path, 'a') as f:
        f.write(str(datetime.now())+'\n')
        p = subprocess.Popen(request_tables, stdout=subprocess.PIPE)
        out, err = p.communicate()
        with open(path, 'a') as f:
            f.write(out.strip().decode())
            if err:
                f.write(err.strip().decode())

def get_clickstream(path, interval_start, interval_end):
    course_slug = 'assessment-higher-education'
    purpose = 'diy widget ' + str(date.today())
    yesterday = (date.today() - timedelta(1)).strftime('%Y-%m-%d')
    interval_start = yesterday
    interval_end = yesterday

    request_clickstream = ['/usr/local/bin/courseraresearchexports', 'jobs', 'request',
        'clickstream', '--course_slug', course_slug,
        '--interval', interval_start, interval_end,
        '--purpose', purpose]

    with open(path, 'a') as f:
        f.write(str(datetime.now())+'\n')
        p = subprocess.Popen(request_clickstream, stdout=subprocess.PIPE)
        out, err = p.communicate()
        with open(path, 'a') as f:
            f.write(out.strip().decode())
            if err:
                f.write(err.strip().decode())

def download_job(job_id, log_path):
    download_command = ['/usr/local/bin/courseraresearchexports', 'jobs', 'download', job_id]
    p = subprocess.Popen(download_command, stdout=subprocess.PIPE)
    out, err = p.communicate()
    with open(log_path, 'a') as f:
        f.write(out.strip().decode())
        if err:
            f.write(err.strip().decode())

def process_files():
    filedates = []
    total_access = []
    for file in os.listdir('.'):
        if file.endswith('csv.gz'):
            try:
                with gzip.open(file, 'rt') as input_file:
                    content = input_file.read()
                    file_name = file.split('.')[0] + '.csv'
                    file_path = '/home/learning-tracker/automater/clickstreams'
                    with open(os.path.join(file_path, file_name), 'w') as output_file:
                        output_file.write(content)
                shutil.move(file, '/home/learning-tracker/automater/clickstreams_zipped')
            except:
                pass
        if file.endswith('.zip'):
            try:
                with ZipFile(file, 'r') as zipObj:
                    zipObj.extractall('tables')
                os.remove(file)
            except:
                print(file, 'is not a zip file')

if __name__ == '__main__':

    log_file = '/home/learning-tracker/automater/logger.txt'
    user_id_hashing = 'isolated'

    with open(log_file, 'r') as f:
        log_content = f.readlines()

    if log_content[-1] == 'READY ' + str(date.today()):
        import analysis
    else:
        downloaded = check_status(log_file, user_id_hashing)
        if len(downloaded) == 2:
            process_files()
            with open(log_file, 'a') as f:
                success = 'READY ' + str(date.today())
                f.write(success)
