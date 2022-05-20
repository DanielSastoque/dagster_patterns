from typing import List
import requests
import csv

from dagster import op, job, Output, MetadataValue, Out, schedule, repository, RunRequest

# Pip now wants to avoid manual triggering
# then he decides to schedule


@op
def download_cereals() -> List[dict]:
    """Downloads cereal data.

    Args:

    Returns:
      List of rows from downloaded data
    """
    response = requests.get('https://docs.dagster.io/assets/cereal.csv')
    lines = response.text.split('\n')
    cereals = [row for row in csv.DictReader(lines)]

    return cereals


@op(out={'cereal_name': Out(str)})
def find_highest_whatever(cereals: List[dict], metric: str) -> str:
    """Finds the name of the cereal with the highest value to the
    given metric.

    Attributes:
        cereals: List of rows with cereal data

    Returns:
      Name of the cereal with the highest value
    """
    sorted_cereals = sorted(cereals, 
                        key=lambda cereal: cereal[metric])
    sorted_cereals = list(sorted_cereals)
    cereal_name = sorted_cereals[-1]['name']

    yield Output(
        output_name='cereal_name',
        value=cereal_name,
        metadata={
            f'cereal with the highest {metric}': cereal_name,
            'dashboard_url': MetadataValue.url('https://www.google.com/'),
            'cereal_count': len(sorted_cereals),
        },
    )


@job
def job_tutorial():
    """
        This is the job to download cereal data and find
        the most caloric cereal.
    """
    find_highest_whatever(
        cereals=download_cereals(),
        )


@schedule(job=job_tutorial, cron_schedule='*/5 * * * *')
def configurable_job_schedule():
    return RunRequest(
        run_key=None,
        run_config={
            'ops': {'find_highest_whatever': {'inputs': {'metric': 'calories'}}}
        },
    )


# Now that Pip has a schedule object, he will need a repository 
@repository
def tutorial_repo():
    return [job_tutorial, configurable_job_schedule]


#  *Show them the UI and see the scheduled job*

# From the terminal:
# dagit -f step_4.py -p 3004

# From another terminal, start the daemon:
# dagit-daemon run

# http://127.0.0.1:3004
