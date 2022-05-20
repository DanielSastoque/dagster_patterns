from typing import List
from datetime import datetime
import requests
import csv
import pandas as pd

from dagster import op, job, Output, MetadataValue, Out, IOManager, io_manager, daily_partitioned_config

# Pip notices that he can download data by date
# Then, he adds the date CONFIG argument to the download method

class DataframeTableIOManager(IOManager):
    def handle_output(self, context, obj: List[dict]):
        file_name = context.metadata['file_name']
        pd.DataFrame(obj).to_csv(f'output_folder/{file_name}.csv', index=False)

    def load_input(self, context):
        file_name = context.upstream_output.metadata['file_name']
        return pd.read_csv(f'output_folder/{file_name}.csv').to_dict(orient='records')


@io_manager
def io_manager_definition(_):
    return DataframeTableIOManager()


@op(out=Out(io_manager_key='dataframe_io', metadata={'file_name': 'csv_op'}),
    config_schema={'date_value': str},
)
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


# Having the date argument, Pip can use the Dagster partitioned jobs!
# where each partition is a time window.

@daily_partitioned_config(start_date=datetime(2022, 5, 1))
def my_partitioned_config(start: datetime, _end: datetime):
    return {
        'ops': {
            'download_cereals': {'config': {'date_value': start.strftime('%Y-%m-%d')}},
            'find_highest_whatever': {'inputs': {'metric': {'value': 'calories'}}},
        }
    }


# And provide the io manager as a resource definition to his job
@job(
    resource_defs={'dataframe_io': io_manager_definition},
    config=my_partitioned_config,
)
def job_tutorial_partitioned():
    """
        This is the job to download cereal data and find
        the most caloric cereal.
    """
    find_highest_whatever(
        cereals=download_cereals(),
        )


#  *Show them the UI and see the partition tabs*

# From the terminal:
# dagit -f step_6.py -p 3006

# From another terminal, start the daemon:
# dagit-daemon run

# http://127.0.0.1:3006
