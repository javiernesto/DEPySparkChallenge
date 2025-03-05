# Data Engineer PySpark Challenge: User Engagement Analysis

## Problem Statement:

You are given a dataset containing user engagement data for a web application. The dataset has the following columns:
- user_id: Unique identifier for each user.
- timestamp: Timestamp of the user activity.
- page: The page the user visited (e.g., 'home', 'dashboard', 'profile').
- duration_seconds: Duration of the user's activity in seconds.

Your task is to use PySpark to analyze the user engagement data and provide the following insights:
- Average Duration Per Page: Calculate the average duration spent by users on each page.
- Most Engaging Page: Determine the page where users, on average, spend the most time.

Implement the PySpark code to achieve these tasks. You can assume that the input data is available in a CSV file named *user_engagement.csv*. Your implementation should be object-oriented, following *PEP-8* standards, following Test Driven Development.

Sample Data:
``````
user_id,timestamp,page,duration_seconds
1,2022-01-01 12:00:00,home,30
2,2022-01-01 12:05:00,dashboard,45
3,2022-01-01 12:10:00,profile,60
1,2022-01-01 12:15:00,home,20
2,2022-01-01 12:20:00,profile,30
3,2022-01-01 12:25:00,dashboard,40
``````

## Expected Output when main is called.

(replace ??? With actual values)

Average Duration Per Page:

| page | avg_duration_sec |
|-----:|-----------------:|
|   ???|               ???|
|   ???|               ???|
|   ???|               ???|


Most engaging page: ??? (average duration: ??? seconds) 
