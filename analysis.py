import csv
import datetime
import gzip
import json
import math
import os
from collections import defaultdict
from config import tables_path, current_cohort_id, early_activity_limit, clickstream_path, \
    previous_cohort_max_forum_score, previous_cohort_max_time_on_platform
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

url_header_main = ['', '', '', '', 'course', 'type', 'item_id', 'item_name', 'desc',
                   'discussion_type', 'discussion_id']
url_header_home = ['', '', '', '', 'course', 'type', 'item_name', 'section', 'detail']
url_header_discussion = ['', '', '', '', 'course', 'type', 'timespan', 'timespan_number',
                         'discussion_type', 'discussion_id']


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
        return url_detail
    except:
        pass


def process_coursera_csv_table(file_location, preferred_id=0):
    with open(file_location, 'r', encoding='utf-8') as f:
        reader = csv.reader(f, dialect='coursera-postgres-format')
        header = next(reader)
        rows = list(reader)
    table = {}
    for row in rows:
        item = {}
        for i, value in enumerate(row):
            item[header[i]] = row[i]
            if '_ts' in header[i] and len(row[i]) > 1:
                try:
                    item[header[i]] = datetime.datetime.strptime(row[i], '%Y-%m-%d %H:%M:%S.%f')
                except ValueError:
                    item[header[i]] = datetime.datetime.strptime(row[i], '%Y-%m-%d %H:%M:%S')
        table[row[preferred_id]] = item
    return table


def process_coursera_csv_table_no_id(file_location):
    with open(file_location, 'r', encoding='utf-8') as f:
        reader = csv.reader(f, dialect='coursera-postgres-format')
        header = next(reader)
        rows = list(reader)
    table = []
    for row in rows:
        item = {}
        for i, value in enumerate(row):
            item[header[i]] = row[i]
            if '_ts' in header[i] and len(row[i]) > 1:
                try:
                    item[header[i]] = datetime.datetime.strptime(row[i], '%Y-%m-%d %H:%M:%S.%f')
                except ValueError:
                    item[header[i]] = datetime.datetime.strptime(row[i], '%Y-%m-%d %H:%M:%S')
        table.append(item)
    #     print(len(table))
    return table


#######################################################################################################################
# Tables ##############################################################################################################
#######################################################################################################################
peer_comments = process_coursera_csv_table(os.path.join(tables_path, 'peer_comments.csv'))
course_progress = process_coursera_csv_table_no_id(os.path.join(tables_path, 'course_progress.csv'))
course_items = process_coursera_csv_table(os.path.join(tables_path, 'course_items.csv'), 1)
course_lessons = process_coursera_csv_table(os.path.join(tables_path, 'course_lessons.csv'), 1)
course_modules = process_coursera_csv_table(os.path.join(tables_path, 'course_modules.csv'), 1)
course_grades = process_coursera_csv_table(os.path.join(tables_path, 'course_grades.csv'), 1)
item_types = process_coursera_csv_table(os.path.join(tables_path, 'course_item_types.csv'))

item_list = []
for item in course_items:
    item_dict = course_items[item]
    item_lesson = course_lessons[item_dict['course_lesson_id']]
    item_dict['course_lesson_order'] = item_lesson['course_lesson_order']
    item_module = course_modules[item_lesson['course_module_id']]
    item_dict['course_module_order'] = item_module['course_module_order']
    item_list.append(item_dict)
items_weekly_sorted = sorted(item_list, key=lambda i: i['course_module_order'])

######################################################################################################################
# Cohorts and Dates ##################################################################################################
######################################################################################################################
cohort_sessions = process_coursera_csv_table(os.path.join(tables_path, 'on_demand_sessions.csv'), 1)
cohort_membership = process_coursera_csv_table_no_id(os.path.join(tables_path, 'on_demand_session_memberships.csv'))

cohort_members = {}
current_cohort = cohort_sessions[current_cohort_id]
current_cohort['starting_week'] = current_cohort['on_demand_sessions_start_ts'].isocalendar()[1]
current_cohort['finishing_week'] = current_cohort['on_demand_sessions_end_ts'].isocalendar()[1]
for member in cohort_membership:
    if member['on_demand_session_id'] == current_cohort_id:
        cohort_members[member['erasmus_user_id']] = member

cohort_start = datetime.datetime.strptime(str(current_cohort['on_demand_sessions_start_ts'].date()), '%Y-%m-%d')

module_deadlines = {}
for x in range(len(course_modules) + 1):
    module_deadlines[x] = datetime.datetime.strptime(
        str(current_cohort['on_demand_sessions_start_ts'].date() +
            datetime.timedelta(days=7) * (x + 1)),
        '%Y-%m-%d') - datetime.timedelta(0, 0, 1)

module_starts = {}
for x in range(len(course_modules) + 1):
    module_starts[x] = datetime.datetime.strptime(
        str(current_cohort['on_demand_sessions_start_ts'].date() +
            datetime.timedelta(days=7) * x),
        '%Y-%m-%d') - datetime.timedelta(0, 0, 1)

#######################################################################################################################
# Access Clickstream and Sessions #####################################################################################
#######################################################################################################################
file_dates = []
total_access = []
for file in os.listdir(clickstream_path):
    if file.startswith('access') and file.endswith('csv.gz'):
        file_dates.append(datetime.datetime.strptime(file[7:17], '%Y-%m-%d'))
        with gzip.open(os.path.join('clickstream_downloads', file), 'rt') as f:
            reader = csv.reader(f, dialect='coursera-postgres-format')
            access = list(reader)
        for line in access:
            total_access.append(line)

file_dates = []
total_access = []
for file in os.listdir(clickstream_path):
    if file.startswith('access') and file.endswith('csv'):
        file_dates.append(datetime.datetime.strptime(file[7:17], '%Y-%m-%d'))
        with open(os.path.join(clickstream_path, file), 'r') as f:
            reader = csv.reader(f, dialect='coursera-postgres-format')
            access = list(reader)
        for line in access:
            total_access.append(line)

header = ['hashed_user_id', 'hashed_session_id', 'timestamp', 'hashed_ip', 'user_agent', 'url', 'referrer', 'lang',
          'course_branch_id', 'country', 'region', 'timezone', 'OS', 'browser', 'key', 'json_event']

learner_access = defaultdict(list)
for log in total_access:
    item = {}
    for i, field in enumerate(log):
        item[header[i]] = log[i]
        if header[i] == 'timestamp':
            item[header[i]] = datetime.datetime.strptime(log[i], '%Y-%m-%d %H:%M:%S.%f')
        if header[i] == 'url':
            item['url_detail'] = process_url(log[i])
    learner_access[item['hashed_user_id']].append(item)

sorted_access = {}
for learner in learner_access:
    current_session = ''
    sorted_clicks = sorted(learner_access[learner], key=lambda i: i['timestamp'])
    sorted_access[learner] = sorted_clicks

#######################################################################################################################
# Course Progress  ####################################################################################################
#######################################################################################################################
course_progress = process_coursera_csv_table_no_id(os.path.join(tables_path, 'course_progress.csv'))
learner_activities = defaultdict(list)
header = ['course_id', 'course_item_id', 'erasmus_user_id', 'course_progress_state_type_id', 'course_progress_ts']
for log in course_progress:
    log['timestamp'] = log['course_progress_ts']
    if log['erasmus_user_id'] in cohort_members:

        if early_activity_limit == 'learner_enrollment':
            # Date of activities before session start is moved to individual learner session start
            if log['course_progress_ts'] < cohort_members[log['erasmus_user_id']]['on_demand_sessions_membership_start_ts']:
                log['course_progress_ts'] = cohort_members[log['erasmus_user_id']][
                    'on_demand_sessions_membership_start_ts']

        elif early_activity_limit == 'cohort_enrollment':
            # Date of activities before session start is moved to official session start
            if log['course_progress_ts'] < current_cohort['on_demand_sessions_start_ts']:
                log['course_progress_ts'] = current_cohort['on_demand_sessions_start_ts']

        learner_activities[log['erasmus_user_id']].append(log)

all_actions = {}
for learner in learner_activities:
    learner_activities[learner] = sorted(learner_activities[learner], key=lambda i: i['course_progress_ts'])

    actions = learner_activities[learner]
    if learner in sorted_access:
        actions = actions + sorted_access[learner]

    all_actions[learner] = sorted(actions, key=lambda i: i['timestamp'])

active_items = []
active_by_type = defaultdict(list)
items_by_type = defaultdict(list)

for item in course_items:
    item_type = course_items[item]['course_item_type_id']
    category = item_types[item_type]['course_item_type_desc']
    items_by_type[category].append(item)
    item_week = int(course_items[item]['course_module_order']) + current_cohort['starting_week']
    if item_week <= datetime.datetime.today().isocalendar()[1]:
        active_items.append(item)
        active_by_type[category].append(item)

passing_cohort_learners = []
for learner in learner_activities:
    if learner in course_grades:
        passing_cohort_learners.append(learner)

############################################################################################################################
# Forum Interaction ########################################################################################################
############################################################################################################################                    

discussion_items = []
for item_type in item_types:
    if item_types[item_type]['course_item_type_category'] == 'discussionPrompt':
        discussion_items.append(item_type)

discussion_item_ids = []
for item in course_items:
    item_type_id = course_items[item]['course_item_type_id']
    if item_type_id in discussion_items:
        discussion_item_ids.append(item)

discussion_questions = process_coursera_csv_table(os.path.join(tables_path, 'discussion_questions.csv'))
discussion_answers = process_coursera_csv_table(os.path.join(tables_path, 'discussion_answers.csv'))

discussion_posts = defaultdict(list)
for question in discussion_questions:
    post = discussion_questions[question]
    learner = discussion_questions[question]['erasmus_discussions_user_id']
    post['forum_activity_type'] = 'post_question'
    post['timestamp'] = post['discussion_question_created_ts']
    discussion_posts[learner].append(post)

for answer in discussion_answers:
    post = discussion_answers[answer]
    learner = discussion_answers[answer]['erasmus_discussions_user_id']
    post['forum_activity_type'] = 'post_answer'
    post['timestamp'] = post['discussion_answer_created_ts']
    discussion_posts[learner].append(post)

############################################################################################################################
# Current Grades ###########################################################################################################
############################################################################################################################

item_grades = process_coursera_csv_table_no_id(os.path.join(tables_path, 'course_item_grades.csv'))
grade_weights = {'UKxma': 0.18, 'Vf6Fp': 0.07, 'oQlU3': 0.5, '5NGTi': 0.25}

############################################################################################################################
# learner Processing  ######################################################################################################
############################################################################################################################

completed_percentages_by_learner_total = {}
completed_percentages_by_learner_discussion = {}
completed_percentages_by_learner_assignment = {}
completed_percentages_by_learner_readings = {}
completed_percentages_by_learner_videos = {}
started_percentages_by_learner = {}
timeliness_by_learner = {}
early_start_by_learner = {}
percentage_reviewed_by_learner = {}
weighted_forum_score_by_learner = {}
current_grade_by_learner = {}
efficiency_by_learner = {}
sessions_by_learner = {}
time_spent_by_learner = {}

for learner in learner_activities:
    ##############################################################################
    # Completion Rates ###########################################################
    ##############################################################################
    completed_items = {}
    started_items = {}
    for activity in learner_activities[learner]:

        if activity['course_item_id'] in active_items:
            if activity['course_progress_state_type_id'] == '1':
                started_items[activity['course_item_id']] = activity
            elif activity['course_progress_state_type_id'] == '2':
                started_items[activity['course_item_id']] = activity
                completed_items[activity['course_item_id']] = activity

    completed_by_type = defaultdict(list)
    started_by_type = defaultdict(list)

    for item in completed_items:
        item_type = course_items[item]['course_item_type_id']
        category = item_types[item_type]['course_item_type_desc']
        completed_by_type[category].append(item)

    for item in started_items:
        item_type = course_items[item]['course_item_type_id']
        category = item_types[item_type]['course_item_type_desc']
        started_by_type[category].append(item)

    started_percentages = {}
    completed_percentages = {}

    percentage_completed = round(len(completed_items) / len(course_items) * 100, 3)
    percentage_started = round(len(started_items) / len(course_items) * 100, 3)

    completed_percentages['total'] = (percentage_completed, len(completed_items))
    started_percentages['total'] = percentage_started

    for category in active_by_type:
        percentage_active_completed = round(len(completed_by_type[category]) / len(active_by_type[category]) * 100, 2)
        percentage_active_started = round(len(started_by_type[category]) / len(active_by_type[category]) * 100, 2)

    for category in items_by_type:
        percentage_completed = round(len(completed_by_type[category]) / len(items_by_type[category]) * 100, 2)
        completed_percentages[category] = (percentage_completed, len(completed_by_type[category]))

        percentage_started = round(len(started_by_type[category]) / len(items_by_type[category]) * 100, 2)
        started_percentages[category] = percentage_started

    completed_percentages_by_learner_total[learner] = completed_percentages['total']
    completed_percentages_by_learner_discussion[learner] = completed_percentages['discussion prompt']
    completed_percentages_by_learner_assignment[learner] = completed_percentages['phased peer']
    completed_percentages_by_learner_readings[learner] = completed_percentages['supplement']
    completed_percentages_by_learner_videos[learner] = completed_percentages['lecture']

    started_percentages_by_learner[learner] = started_percentages

    ##############################################################################
    # Timeliness - time from completing activity to end of week ##################
    ##############################################################################

    timeliness = []
    for activity in learner_activities[learner]:
        if activity['course_progress_state_type_id'] != '2': continue
        item_id = activity['course_item_id']
        module_deadline = module_deadlines[int(course_items[activity['course_item_id']]['course_module_order'])]
        difference = module_deadline - activity['course_progress_ts']
        timeliness.append(difference)

    if len(timeliness) > 0:

        timeliness_avg = sum(timeliness, datetime.timedelta(0)) / len(timeliness)

        if timeliness_avg.days > 0:
            phrase = 'hours in advance'
            timeliness_by_learner[learner] = min(7, timeliness_avg.days)
        else:
            phrase = 'hours late'
            timeliness_by_learner[learner] = 0

    ##############################################################################
    # Early Start - time from starting activity to end of week ###################
    ##############################################################################

    starting_times = []
    for activity in learner_activities[learner]:
        if activity['course_progress_state_type_id'] != '1': continue
        item_id = activity['course_item_id']
        module_start = module_starts[int(course_items[activity['course_item_id']]['course_module_order'])]
        difference = module_start - activity['course_progress_ts']
        starting_times.append(difference)

    if len(starting_times) > 0:
        starting_times_avg = sum(starting_times, datetime.timedelta(0)) / len(starting_times)

        if starting_times_avg.days > 0:
            phrase = 'hours before the start of the week'
            early_start_by_learner[learner] = min(7, starting_times_avg.days)
        else:
            phrase = 'hours after the start of the week'
            early_start_by_learner[learner] = 0

    ##############################################################################
    # Reviewing ##################################################################
    ##############################################################################

    items_accessed = defaultdict(set)

    if learner in sorted_access:
        for access in sorted_access[learner]:
            if 'url_detail' in access:
                if access['url_detail'] and 'item_id' in access['url_detail']:
                    day = str(access['timestamp'].date())
                    items_accessed[day].add(access['url_detail']['item_id'])

        items_reviewed = defaultdict(list)
        daily_reviews = []
        for day in items_accessed:
            for item in items_accessed[day]:
                if item in completed_items:
                    if datetime.datetime.strptime(day, '%Y-%m-%d').date() > completed_items[item][
                        'course_progress_ts'].date():
                        items_reviewed[day].append(item)
            daily_reviews.append(len(items_reviewed[day]) / len(items_accessed[day]))

        if len(daily_reviews) < 1:
            percentage_reviewed_by_learner[learner] = 0
        else:
            percentage_reviewed_by_learner[learner] = round(sum(daily_reviews) / len(daily_reviews) * 100, 3)

        ##############################################################################
        # Forum Score ################################################################
        ##############################################################################

        forum_activities = []

        for access in sorted_access[learner]:
            if 'url_detail' in access:
                if access['url_detail'] and 'item_id' in access['url_detail']:
                    if access['url_detail']['item_id'] == 'weeks' or access['url_detail']['type'] == 'discussionPrompt':
                        access['forum_activity_type'] = 'access'
                        forum_activities.append(access)

        for post in discussion_posts[learner]:
            forum_activities.append(post)

        own_posts = []
        forum_visits = []
        answer_own_thread = []
        answer_others = []
        post_question = []

        for activity in sorted(forum_activities, key=lambda i: i['timestamp']):
            if activity['forum_activity_type'] == 'access':
                forum_visits.append(activity['timestamp'])
            if 'discussion_answer_id' in activity:
                if activity['discussion_answer_id'] in own_posts or activity['discussion_question_id'] in own_posts:
                    answer_own_thread.append(activity['timestamp'])
                else:
                    answer_others.append(activity['timestamp'])
                own_posts.append(activity['discussion_answer_id'])
            if 'discussion_question_title' in activity:
                own_posts.append(activity['discussion_question_id'])
                post_question.append(activity['timestamp'])

        if len(forum_visits) == 0:
            weighted_forum_score_by_learner[learner] = 0
        else:
            weighted_forum_score_by_learner[learner] = (len(post_question) * 0.5 + len(answer_own_thread) * 0.2 + len(
                answer_others) * 0.3) / len(forum_visits)

    ##############################################################################
    # Current Grades #############################################################
    ##############################################################################

    learner_grades = {}
    for graded_item in item_grades:
        if graded_item['erasmus_user_id'] == learner:
            learner_grades[graded_item['course_item_id']] = graded_item['course_item_grade_overall']
    current_grade = 0
    for item in learner_grades:
        current_grade = current_grade + (float(learner_grades[item]) * grade_weights[item])
    current_grade_by_learner[learner] = current_grade

    ##############################################################################
    # Average Daily Efficiency ###################################################
    ##############################################################################

    items_completed = {}
    for activity in learner_activities[learner]:
        item_id = activity['course_item_id']
        items_completed[item_id] = {'started': '', 'completed': ''}

    active_days = set()
    for activity in learner_activities[learner]:
        item_id = activity['course_item_id']
        if activity['course_progress_state_type_id'] == '2':
            items_completed[item_id]['completed'] = activity['course_progress_ts'].date()
            active_days.add(str(activity['course_progress_ts'].date()))
        if activity['course_progress_state_type_id'] == '1':
            items_completed[item_id]['started'] = activity['course_progress_ts'].date()
            active_days.add(str(activity['course_progress_ts'].date()))

    items_completed_same_day = defaultdict(set)
    items_not_completed_same_day = defaultdict(set)

    for item in items_completed:
        if items_completed[item]['started'] == '':
            items_completed_same_day[str(items_completed[item]['completed'])].add(item)
        elif items_completed[item]['started'] == items_completed[item]['completed']:
            items_completed_same_day[str(items_completed[item]['completed'])].add(item)
        else:
            items_not_completed_same_day[str(items_completed[item]['started'])].add(item)

    efficiency_daily = []
    for day in list(active_days):
        if len(items_completed_same_day[day]) > 0:
            efficiency_daily.append(len(items_completed_same_day[day]) / (
                    len(items_completed_same_day[day]) + len(items_not_completed_same_day)))
        else:
            efficiency_daily.append(0)
    efficiency_avg = sum(efficiency_daily) / len(efficiency_daily)

    efficiency_by_learner[learner] = efficiency_avg

    ##############################################################################
    # Sessions ###################################################################
    ##############################################################################
    start_time = ''
    end_time = ''
    learner_sessions = []
    event_logs = all_actions[learner]
    for i in range(len(event_logs)):
        if start_time == "":
            # Initialization
            start_time = event_logs[i]["timestamp"]
            end_time = event_logs[i]["timestamp"]

        else:

            if event_logs[i]["timestamp"] > end_time + datetime.timedelta(hours=0.5):

                session_id = learner + "_" + str(start_time) + "_" + str(end_time)
                duration = (end_time - start_time).seconds

                if duration > 5:
                    array = (session_id, learner, start_time, end_time, duration)
                    learner_sessions.append(array)

                    # Re-initialization
                session_id = ""
                start_time = event_logs[i]["timestamp"]
                end_time = event_logs[i]["timestamp"]

            else:
                if i == len(event_logs) - 1:
                    end_time = event_logs[i]["timestamp"]

                    session_id = learner + "_" + str(start_time) + "_" + str(end_time)
                    duration = (end_time - start_time).seconds

                    if duration > 5:
                        array = (session_id, learner, start_time, end_time, duration)
                        learner_sessions.append(array)

                    # Re-initialization
                    session_id = ""
                    start_time = ""
                    end_time = ""

                else:
                    end_time = event_logs[i]["timestamp"]

    sessions_by_learner[learner] = learner_sessions
    durations = []
    for session in learner_sessions:
        durations.append(session[4])
    time_spent_by_learner[learner] = sum(durations) / 60

metrics = {
    'metric_1': percentage_reviewed_by_learner,
    'metric_2': weighted_forum_score_by_learner,
    'metric_3': efficiency_by_learner,
    'metric_4': time_spent_by_learner,
    'metric_5': early_start_by_learner,
    'metric_6': timeliness_by_learner,
    'metric_7': completed_percentages_by_learner_total,
    'metric_8': completed_percentages_by_learner_discussion,
    'metric_9': completed_percentages_by_learner_assignment,
    'metric_10': completed_percentages_by_learner_readings,
    'metric_11': completed_percentages_by_learner_videos,
    'metric_12': current_grade_by_learner,
}


def scale_metric(metric, value):
    scaled_value = str(value) + ' not scaled'
    if metric in ['metric_1', 'metric_3', 'metric_12']:
        scaled_value = value * 10
    if metric in ['metric_7', 'metric_8', 'metric_9', 'metric_10', 'metric_11']:
        scaled_value = value / 10
    if metric in ['metric_5', 'metric_6']:
        scaled_value = value * 10 / 7
    if metric == 'metric_4':
        scaled_value = value * 10 / previous_cohort_max_time_on_platform
    if metric == 'metric_2':
        scaled_value = value * 10 / previous_cohort_max_forum_score
    return round(scaled_value, 2)

print(len(learner_activities), 'items to insert')

for learner in learner_activities:
    metric_values = {}
    for metric in metrics:
        value = 0
        scaled_value = 0
        if learner in metrics[metric]:
            if type(metrics[metric][learner]) != tuple:
                value = metrics[metric][learner]
                absolute_value = ''
            else:
                value = metrics[metric][learner][0]
                absolute_value = metrics[metric][learner][1]
            scaled_value = scale_metric(metric, value)
        metric_values[metric] = {
            'name': metric,
            'value': value,
            'absolute_value': absolute_value,
            'scaled_value': scaled_value
        }

    daily_value = {
        'course_branch_id': current_cohort['course_branch_id'],
        'hashed_user_id': learner,
        'timestamp': datetime.datetime.now(),
        'metrics': metric_values
    }
    user_data.insert_one(daily_value)
