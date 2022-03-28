import requests
import csv

from dagster import job, op, get_dagster_logger


@op
def download_cereals():
    response = requests.get('https://docs.dagster.io/assets/cereal.csv')
    lines = response.text.split('\n')
    cereals = [row for row in csv.DictReader(lines)]

    return cereals


def finder_factory(metric):

    @op(name=f'find_highest_{metric}_cereal')
    def find_whatever(cereals):
        sorted_cereals = sorted(cereals, 
                            key=lambda cereal: cereal[metric])
        sorted_cereals = list(sorted_cereals)
        return sorted_cereals[-1]['name']

    return find_whatever


@op
def display_results(calories, protein, sugars):
    message_dict = {'caloric': calories,
                    'protein_rich': protein,
                    'sugariest': sugars,
                    }

    logger = get_dagster_logger()
    for key, value in message_dict.items():
        logger.info(f'Most {key} cereal: {value}')


@job
def diamond():
    cereals = download_cereals()
    display_results(**{metric: finder_factory(metric)(cereals) 
                    for metric in ['calories', 'protein', 'sugars']})


def test_find_highest_calorie_cereal():
    cereals = [
        {'name': 'hi_cal cereal', 'calories': 400},
        {'name': 'lo_cal cereal', 'calories': 50},
    ]
    result = finder_factory('calories')(cereals)
    assert result == 'hi_cal cereal'


def test_diamond():
    res = diamond.execute_in_process()
    assert res.success
    assert res.output_for_node('find_highest_protein_cereal') == 'Special K'


if __name__ == '__main__':
    result = diamond.execute_in_process()
