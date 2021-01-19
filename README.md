# Coursera Learning Tracker Data Processing

This is the processing and analytics module used in 
**The Quantum of Choice: How learners' feedback monitoring decisions, goals and self-regulated learning skills are related** 
by Jivet, Wong, Scheffel, Valle Torre, Specht and Drachsler (LAK21).
It consists of 3 main parts: the `data_pipeline`, the `analysis` and the `weekly_updater`.

## Data Pipeline
The data available from Coursera can be retrieved on a daily basis, using their [`courseraresearchexports`](https://github.com/coursera/courseraresearchexports) 
module (written in Python 2.7) via command line. Please refer to their documentation for more details on usage and authentication. The retrieval process is as follows:
1. The first step is to generate a request for `tables` and `clickstreams` with the corresponding flags such as `--COURSE_SLUG` and `--PURPOSE`  
2. The next step is to check the status of the requests with `courseraresearchexports jobs get_all` until they are ready for download, since there's no notification. 
3. Once the status is `READY` the files can be downloaded: tables are a group of `csv` files in a zip, while clickstreams are individual gzip files.
4. The files have to be extracted or unzipped, and then they can be processed
To complete these steps, `data_pipeline.py` is executed using `cron` 3 times every day, with an hour in between. In over 3 months of execution, there were minimal errors
caused by not available jobs from Coursera or changes in some table headings, but files were always ready within the hour.

## Analysis
In this script the 12 metrics available in **The Quantum of Choice** are calculated using the tables and clickstream files obtained from the research exports. 
```
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
    'metric_12': current_grade_by_learner
}
```
These calculations only apply for the particular schema of this data from Coursera, and each metric contains 3 values: 
the **value**, the actual metric obtained by the learner such as a percentage or count; 
the **absolute_value** with a total amount achievable for each metric (only when aplicable, such as 2 tests completed *out of 3*); 
and the **scaled_value**, the value on a scale of 0 to 10, to be displayed in the dashboard in Coursera. 

## Weekly Updater
As part of the study, learners had the ability to update their indicator selection at the beginning of every week, this script enables that functionality, 
executed by `cron` every Sunday evening.

