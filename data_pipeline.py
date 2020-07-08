import subprocess
import os
import gzip
import shutil
from config import course_list, working_directory
from zipfile import ZipFile
from datetime import date, timedelta, datetime


def check_status(log_file, course_slug, user_id_hashing_type):
    today = (date.today()).strftime('%Y-%m-%d')
    yesterday = (date.today() - timedelta(1)).strftime('%Y-%m-%d')
    to_request = ['TABLES', 'CLICKSTREAM']
    downloaded_types = []
    p = subprocess.Popen(['/usr/local/bin/courseraresearchexports', 'jobs', 'get_all'], stdout=subprocess.PIPE)
    out, err = p.communicate()
    lines = out.strip().decode().split('\n')

    for line in lines:
        line = line.strip().split()
        job_id = line[2]
        export_type = line[4]
        if today in line and course_map[course_slug] in line:
            to_request.remove(export_type)
            if 'SUCCESSFUL' in line:
                if export_type in line:
                    download_job(job_id, log_file)
                    downloaded_types.append(export_type)
    if 'TABLES' in to_request:
        get_tables(log_file, course_slug, user_id_hashing_type)
    if 'CLICKSTREAM' in to_request:
        get_clickstream(log_file, course_slug, yesterday, yesterday)
    return downloaded_types


def process_data_request(path, request):
    with open(path, 'a') as f:
        f.write(str(datetime.now()) + '\n')
        p = subprocess.Popen(request, stdout=subprocess.PIPE)
        out, err = p.communicate()
        with open(path, 'a') as f:
            f.write(out.strip().decode())
            if err:
                f.write(err.strip().decode())


def get_tables(path, course_slug, user_id_hashing_type):
    purpose = 'diy widget ' + str(date.today())

    request_tables = ['/usr/local/bin/courseraresearchexports', 'jobs', 'request',
                      'tables', '--course_slug', course_slug,
                      '--purpose', purpose,
                      '--user_id_hashing', user_id_hashing_type]

    process_data_request(path, request_tables)


def get_clickstream(path, course_slug, interval_start, interval_end):
    purpose = 'diy widget ' + str(date.today())

    request_clickstream = ['/usr/local/bin/courseraresearchexports', 'jobs', 'request',
                           'clickstream', '--course_slug', course_slug,
                           '--interval', interval_start, interval_end,
                           '--purpose', purpose]

    process_data_request(path, request_clickstream)


def download_job(job_id, log_path):
    download_command = ['/usr/local/bin/courseraresearchexports', 'jobs', 'download', job_id]
    process_data_request(log_path, download_command)


def process_files(working_directory):
    for file in os.listdir('.'):
        if file.endswith('csv.gz'):
            try:
                with gzip.open(file, 'rt') as input_file:
                    content = input_file.read()
                    file_name = file.split('.')[0] + '.csv'
                    file_path = os.path.join(working_directory, 'clickstreams')
                    with open(os.path.join(file_path, file_name), 'w') as output_file:
                        output_file.write(content)
                shutil.move(file, os.path.join(working_directory, 'clickstreams_zipped'))
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

    user_id_hashing = 'isolated'
    
    log_file_path = os.path.join(working_directory, 'logger.txt')
    
    course_map = {'sdgbusiness': 'zn6pvDaiEemqUwqQt78jjg',
                  'assessment-higher-education': 'K9cwvyTbEeenjw6oiOFT7g'}

    for current_course_slug in course_list:
        with open(log_file_path, 'r') as f:
            log_content = f.readlines()

        if log_content[-1] == 'READY ' + str(date.today()):
            import analysis
        else:
            downloaded = check_status(log_file_path, current_course_slug, user_id_hashing)
            if len(downloaded) == 2:
                process_files(working_directory)
                with open(log_file_path, 'a') as f:
                    success = 'READY ' + str(date.today())
                    f.write(success)
