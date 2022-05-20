from typing import List
import requests
import csv

from dagster import op, job, Output, MetadataValue, Out

# "What is all the fuzz about metadata" says Pip
# Then he adds metadata to the code


# He adds type annotations and docstring
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


# Also metadata about the output
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


# Pip sees a nice UI!

#  *Show them the UI and make a run*

# From the terminal:
# dagit -f step_3.py -p 3003

# http://127.0.0.1:3003
