import requests
import csv

from dagster import op, job

# Now Pip wants to get fancy and use an orchestrator
# He chooses dagster!.


# The only thing he needs is to add a decorator:
@op
def download_cereals():
    response = requests.get('https://docs.dagster.io/assets/cereal.csv')
    lines = response.text.split('\n')
    cereals = [row for row in csv.DictReader(lines)]

    return cereals

@op
def find_highest_whatever(cereals, metric):
    sorted_cereals = sorted(cereals, 
                        key=lambda cereal: cereal[metric])
    sorted_cereals = list(sorted_cereals)
    return sorted_cereals[-1]['name']


# And chain his ops in a job
@job
def job_tutorial():
    find_highest_whatever(
        cereals=download_cereals(),
        )


# And for sure, get what he is looking for (using the command line):
if __name__ == '__main__':
    metric = 'calories'
    run_config={
        'ops': {
            'find_highest_whatever': {
                'inputs': {'metric': {'value': metric}},
                },
        },
    }

    result = job_tutorial.\
        execute_in_process(run_config=run_config).\
        output_for_node(node_str='find_highest_whatever')

    print(f'The one with the highest amount of {metric} is', result)


# Pip runs
# python step_2.py

# Almost the same, even with more jargon and verbosity. 
# But now, Pip has access to the dagster UI!

#  *Show them the UI and make a run*

# From the terminal:
# dagit -f step_2.py -p 3002

# http://127.0.0.1:3002
