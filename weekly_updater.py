import datetime
from pymongo import MongoClient

client = MongoClient('localhost', 27017)
db = client.course_x_db
widget_data = db.widget_data

active_learners = set()
for res in widget_data.find({}):
    active_learners.add(res['hashed_user_id'])
    
for learner in active_learners:
    latest_record = widget_data.find({'hashed_user_id': learner}).sort('timestamp', -1).limit(1)
    for res in latest_record:
        widget_settings = {
        'course_branch_id': res['course_branch_id'],
        'hashed_user_id': learner,
        'consent': res['consent'],
        'SRL_quest': res['SRL_quest'],
        'goal': res['goal'],
        'selected_metrics': res['selected_metrics'],
        'update_goal_flag': True,
        'update_indicators_flag': True,
        'timestamp': datetime.datetime.now()
        }

        widget_data.insert_one(widget_settings)
