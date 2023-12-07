show databases;


#                                         CASE STUDY-1
create database operation_analytics;
use  operation_analytics;
select * from job_data ;
drop table job_data;
describe job_data;

------------------------------------------------------------------------------------------------------------------
# A. Jobs Reviewed Over Time:

# Objective: Calculate the number of jobs reviewed per hour for each day in November 2020.
# Your Task: Write an SQL query to calculate the number of jobs reviewed per hour for each day in November 2020.


alter table job_data add column dates date;
update job_data set dates=STR_TO_DATE(ds,'%m/%d/%Y');
alter table job_data drop column ds;
alter table job_data change column dates ds date;

select * from job_data;
SELECT 
    DATE_FORMAT(ds, '%d-%M-%Y') AS Day,
    round(COUNT(job_id) / sum(time_spent)*3600) AS No_of_Jobs_Reveiwed
FROM
    job_data
WHERE
    ds BETWEEN '2020-11-01' AND '2020-11-30'
GROUP BY 1
ORDER BY 1;


-- B: Throughput Analysis:

-- Objective: Calculate the 7-day rolling average of throughput (number of events per second).
-- Your Task: Write an SQL query to calculate the 7-day rolling average of throughput. Additionally, explain whether you prefer using the daily metric or the 7-day rolling average for throughput, and why.



select round((count(event)/sum(time_spent)),2) as Weekly_throughput 
from job_data;

select ds,
       throughput,
       avg(throughput)over(order by ds rows between 6 preceding and current row) as Daily_throughput
from(
select ds, count(event)/sum(time_spent)  as throughput from job_data
where ds between '2020-11-24' and '2020-11-30'group by ds order by ds ) as a;


-- C: Language Share Analysis:

-- Objective: Calculate the percentage share of each language in the last 30 days.
-- Your Task: Write an SQL query to calculate the percentage share of each language over the last 30 days.

SELECT 
    language,
    COUNT(job_id) AS jobs,
    ROUND(100 * COUNT(job_id) / (SELECT 
                    COUNT(job_id)
                FROM
                    job_data),
            2) AS percentage_share
FROM
    job_data
GROUP BY language;


-- D. Duplicate Rows Detection:
select * from job_data;

SELECT 
    actor_id, COUNT(actor_id) AS No_of_Duplicates
FROM
    job_data
GROUP BY actor_id
HAVING No_of_Duplicates > 1;


---------------------------------------------------------------------------------------------------------------------------

#                               CASE STUDY-2

-- Table -1 Users

create table users(
user_id int,
created_at varchar(100),
company_id int,
language varchar(50),
activated_at varchar(100),
state varchar(50));

SHOW VARIABLES LIKE 'secure_file_priv';

LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/users.csv'
INTO TABLE users
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES terminated by '\n'
IGNORE 1 ROWS;

select * from users;
alter table users add column temp datetime;
update users set temp=str_to_date(created_at,'%d-%m-%Y %H:%i');
alter table users drop column created_at;
alter table users change column temp created_at datetime;

alter table users add column temp datetime;
update users set temp=str_to_date(activated_at,'%d-%m-%Y %H:%i');
alter table users drop column activated_at;
alter table users change column temp activated_at datetime;

# Table -2 events

create table events(
user_id int,
occurred_at varchar(100),
event_type varchar(100),
event_name varchar(100),
location varchar(100),
device varchar(100),
user_type int);

LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/events.csv'
INTO TABLE events
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES terminated by '\n'
IGNORE 1 ROWS;

select * from events;
alter table events add column temp datetime;
update events set temp=str_to_date(occurred_at,'%d-%m-%Y %H:%i');
alter table events drop column occurred_at;
alter table events change column temp occurred_at datetime;

# Table-3 email_events

create table email_events(
user_id int,
occurred_at varchar(100),
action varchar(100),
user_type int);

LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/email_events.csv'
INTO TABLE email_events
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES terminated by '\n'
IGNORE 1 ROWS;

select * from email_events;
alter table email_events add column temp datetime;
update email_events set temp=str_to_date(occurred_at,'%d-%m-%Y %H:%i');
alter table email_events drop column occurred_at;
alter table email_events change column temp occurred_at datetime;


-- A. Weekly User Engagement:
-- Objective: Measure the activeness of users on a weekly basis.
-- Your Task: Write an SQL query to calculate the weekly user engagement.

select * from users;
select * from events;
select * from email_events;


SELECT 
    EXTRACT(WEEK FROM occurred_at) AS Week_No,
    COUNT(DISTINCT user_id) AS Engaged_Users
FROM
    events
WHERE
    event_type = 'engagement'
GROUP BY Week_No
ORDER BY Week_No;

-- B. User Growth Analysis:
-- Objective: Analyze the growth of users over time for a product.
-- Your Task: Write an SQL query to calculate the user growth for the product.
select * from users;
describe users;
describe events;


select year,week_no,no_of_active_users,sum(no_of_active_users) 
over(order by year, week_no rows between unbounded preceding and current row) 
as total_active_users from(
select extract(year from activated_at) as year, extract(week from activated_at) as week_no,
count(distinct user_id) as no_of_active_users
 from users   group by year,week_no order by year,week_no)a;

-- C. Weekly Retention Analysis:
-- Objective: Analyze the retention of users on a weekly basis after signing up for a product.
-- Your Task: Write an SQL query to calculate the weekly retention of users based on their sign-up cohort.


SELECT 
    EXTRACT(WEEK FROM occurred_at) AS week_no,
    COUNT(DISTINCT user_id) AS no_of_users
FROM
    events
WHERE
    event_type = 'signup_flow'
        AND event_name = 'complete_signup'
GROUP BY week_no
ORDER BY week_no;

-- D. Weekly Engagement Per Device:
-- Objective: Measure the activeness of users on a weekly basis per device.
-- Your Task: Write an SQL query to calculate the weekly engagement per device.

select * from events;

SELECT 
    device,
    EXTRACT(WEEK FROM occurred_at) AS week_no,
    COUNT(DISTINCT user_id) AS no_of_users
FROM
    events
WHERE
    event_type = 'engagement'
GROUP BY device , week_no
ORDER BY week_no;

-- E. Email Engagement Analysis:
-- Objective: Analyze how users are engaging with the email service.
-- Your Task: Write an SQL query to calculate the email engagement metrics.
select * from email_events;
select * from events;

select count(action) as action_count, action from email_events group by action;
SELECT 
    (SUM(CASE
        WHEN email_category = 'email_opened' THEN 1 ELSE 0 END) / SUM(CASE
        WHEN email_category = 'email_sent' THEN 1 ELSE 0 END)) * 100 AS email_open_rate,
    (SUM(CASE
        WHEN email_category = 'email_clicked' THEN 1 ELSE 0 END) / SUM(CASE
        WHEN email_category = 'email_sent' THEN 1 ELSE 0
    END)) * 100 AS email_clicked_rate
FROM
    (SELECT *,
            CASE
                WHEN action IN ('sent_weekly_digest' , 'sent_reengagement_email') THEN ('email_sent')
                WHEN action IN ('email_open') THEN ('email_opened')
                WHEN action IN ('email_clickthrough') THEN ('email_clicked')
            END AS email_category
    FROM
        email_events) AS a;
