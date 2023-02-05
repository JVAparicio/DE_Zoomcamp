## Week 2 Homework

## Question 1. Load January 2020 data

Using the `etl_web_to_gcs.py` flow that loads taxi data into GCS as a guide, create a flow that loads the green taxi CSV dataset for January 2020 into GCS and run it. Look at the logs to find out how many rows the dataset has.

Q: How many rows does that dataset have?

A: 447,770

Code:

Update the `etl_web_to_gcs_hw.py` with:
```
if __name__ == '__main__':
color = 'green'
months = [1]
year = 2020
```

![image](https://user-images.githubusercontent.com/20862376/216659794-bdfd7c36-e5df-49c4-8162-d6a5e8f2d1e3.png)



## Question 2. Scheduling with Cron

Q: Using the flow in `etl_web_to_gcs_hw.py`, create a deployment to run on the first of every month at 5am UTC. What’s the cron schedule for that?

A: `* * 5 1 0`

Code:

```
prefect deployment build ./etl_web_to_gcs_hw.py:etl_parent_flow -n "etl3" --cron "0 5 1 * *" -a
```

![image](https://user-images.githubusercontent.com/20862376/216661350-56831c5e-91db-4662-a92f-d7fc4aec2db8.png)


## Question 3. Loading data to BigQuery 

Q:How many rows did your flow code process?

A: 14,851,920

Code:

Update the 'etl_gcs_to_bq_hw.py' to be arametrized:

```
@flow()
def elt_parent_flow(months: list[int] = [1,2], year: int = 2021, color: str = 'yellow'):

    for month in months:
        etl_gcs_to_bq(year, month, color)
```

Build the deployment:

```
prefect deployment build ./etl_gcs_to_bq_hw.py:elt_parent_flow -n "Parameterized ELT"
```

Apply the deployment:

```
prefect deployment apply elt_parent_flow-deployment.yaml 
```

Run the the deployment with the parameters:

```
prefect deployment run  "elt-parent-flow/Parameterized ELT" -p "months=[2,3]" -p "year=2019" -p "color=yellow"
```

Start the agent:
```
prefect agent start -q default
```

Check the results on GCP:

![image](https://user-images.githubusercontent.com/20862376/216673521-442c085a-8c54-4869-9bfe-8510db4e8beb.png)



## Question 4. Github Storage Block


Q: How many rows were processed by the script?

A: 88,605

Code:

Create a github block on the Prefect UI

Create a deploy using the github block 
```
prefect deployment build ./etl_web_to_gcs_hw.py:etl_parent_flow \
  -n test_github \
  -sb github/github-zoom \
  -o prefect-github-deployment \
  --apply
```

Run the deployment with the parameters needed:
```
prefect deployment run etl-parent-flow/test_github -p "months=[11]" -p "year=2020" -p "color=green"
```



## Question 5. Email or Slack notifications


Q: How many rows were processed by the script?

A: 514,392


## Question 6. Secrets

Q: How many characters are shown as asterisks (*) on the next page of the UI?

A: 8

![image](https://user-images.githubusercontent.com/20862376/216845108-7dd0125b-f080-4189-afd6-756748cbdd98.png)



