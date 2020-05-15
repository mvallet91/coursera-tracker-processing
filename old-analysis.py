import datetime, json, os, csv, gzip
from collections import defaultdict
from operator import itemgetter
from pymongo import MongoClient
from random import randint

client = MongoClient('localhost', 27017)
db = client.course_x_db
standard_data = db.standard_data
user_data = db.user_data

csv.register_dialect(
    'coursera-postgres-format',
    delimiter=',',
    doublequote=False,
    escapechar='\\',
    lineterminator='\n',
    quotechar='"')

url_header_main = ['', '', '', '', 'course', 'type', 'item_id', 'item_name', 'desc', 'discussion_type', 'discussion_id']
url_header_home = ['', '', '', '', 'course', 'type', 'item_name', 'section', 'detail']
url_header_discussion = ['', '', '', '', 'course', 'type', 'timespan', 'timespan_number', 'discussion_type', 'discussion_id']

def process_url(url):
    url_detail = {}
    url_parts = url.split('/')
    try:
        if url_parts[5] == 'home':
            url_header = url_header_home
        elif url_parts[5] == 'discussion':
            url_header = url_header_discussion
        else:
            url_header = url_header_main
        for j, url_part in enumerate(url_parts):
            url_detail[url_header[j]] = url_parts[j]
        return(url_detail)
    except:
        pass

path = '/home/learning-tracker/automater/clickstreams'
yesterday = (datetime.date.today() - datetime.timedelta(1)).strftime('%Y-%m-%d')
filename = 'access-' + yesterday + '.csv'
with open(os.path.join(path, filename), 'r') as f:
    reader = csv.reader(f, dialect='coursera-postgres-format')
    access = list(reader)

header = ['hashed_user_id', 'hashed_session_id', 'timestamp', 'hashed_ip', 'user_agent', 'url', 'referrer', 'lang',
          'course_branch_id', 'country', 'region', 'timezone', 'OS', 'browser', 'key', 'json_event']

learner_access = defaultdict(list)
for log in access:
    item = {}
    for i, field in enumerate(log):
        item[header[i]] = log[i]
        if header[i] == 'timestamp':
            item[header[i]] = datetime.datetime.strptime(log[i], '%Y-%m-%d %H:%M:%S.%f')
        if header[i] == 'url':
            item['url_detail'] = process_url(log[i])
    learner_access[item['hashed_user_id']].append(item)

max_access = 0
for learner in learner_access:
    max_access = max(max_access, len(learner_access[learner]))

metrics = ["metric_1","metric_2","metric_3","metric_4","metric_5","metric_6",
           "metric_7","metric_8","metric_9","metric_10","metric_11","metric_12"]
i = 1
metric_values = {}
for metric in metrics:
    value = max_access * i
    i = i + 1
    scaled_value = max_access / (max_access + 15 * randint(1,15)) * 10
    metric_values[metric] = {
        'name': metric,
        'value': value,
        'scaled_value': scaled_value
    }

daily_value = {
    'course_branch_id': 'branch_A',
    'hashed_user_id': 'user_123',
    'standard': 'completer',
    'metrics': metric_values
}

#user_data.insert_one(daily_value)
print(daily_value)
