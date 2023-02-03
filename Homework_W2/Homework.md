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








## Question 4. Github Storage Block

Using the `web_to_gcs` script from the videos as a guide, you want to store your flow code in a GitHub repository for collaboration with your team. Prefect can look in the GitHub repo to find your flow code and read it. Create a GitHub storage block from the UI or in Python code and use that in your Deployment instead of storing your flow code locally or baking your flow code into a Docker image. 

Note that you will have to push your code to GitHub, Prefect will not push it for you.

Run your deployment in a local subprocess (the default if you don’t specify an infrastructure). Use the Green taxi data for the month of November 2020.

How many rows were processed by the script?

- 88,019
- 192,297
- 88,605
- 190,225



## Question 5. Email or Slack notifications

Q5. It’s often helpful to be notified when something with your dataflow doesn’t work as planned. Choose one of the options below for creating email or slack notifications.

The hosted Prefect Cloud lets you avoid running your own server and has Automations that allow you to get notifications when certain events occur or don’t occur. 

Create a free forever Prefect Cloud account at app.prefect.cloud and connect your workspace to it following the steps in the UI when you sign up. 

Set up an Automation that will send yourself an email when a flow run completes. Run the deployment used in Q4 for the Green taxi data for April 2019. Check your email to see the notification.

Alternatively, use a Prefect Cloud Automation or a self-hosted Orion server Notification to get notifications in a Slack workspace via an incoming webhook. 

Join my temporary Slack workspace with [this link](https://join.slack.com/t/temp-notify/shared_invite/zt-1odklt4wh-hH~b89HN8MjMrPGEaOlxIw). 400 people can use this link and it expires in 90 days. 

In the Prefect Cloud UI create an [Automation](https://docs.prefect.io/ui/automations) or in the Prefect Orion UI create a [Notification](https://docs.prefect.io/ui/notifications/) to send a Slack message when a flow run enters a Completed state. Here is the Webhook URL to use: https://hooks.slack.com/services/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

Test the functionality.

Alternatively, you can grab the webhook URL from your own Slack workspace and Slack App that you create. 


How many rows were processed by the script?

- `125,268`
- `377,922`
- `728,390`
- `514,392`


## Question 6. Secrets

Prefect Secret blocks provide secure, encrypted storage in the database and obfuscation in the UI. Create a secret block in the UI that stores a fake 10-digit password to connect to a third-party service. Once you’ve created your block in the UI, how many characters are shown as asterisks (*) on the next page of the UI?

- 5
- 6
- 8
- 10


## Submitting the solutions

* Form for submitting: https://forms.gle/PY8mBEGXJ1RvmTM97
* You can submit your homework multiple times. In this case, only the last submission will be used. 

Deadline: 6 February (Monday), 22:00 CET


## Solution

We will publish the solution here
