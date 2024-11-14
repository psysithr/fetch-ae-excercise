# fetch-ae-excercise

## Steps Taken to Run Locally
1) Create db to load data into. While working on this exercise I used a postgres db
2) Unzip json files and place in data_load/
3) Run load.py
  - This script loops through each json file, creates a table in the db, and reads the data in
  - The `conn` variable in line 8 is used to define the connection to the db created in step 1
4) Create dbt project
  - `fetch_exercise_dbt/profiles.yml` is where I define the connection to the db created in step 1
5) Run dbt models
