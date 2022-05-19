import requests
import csv


# Pip wants to download some data from an API and
# do some math with it.


# He can just write vanilla python like this:
def download_cereals():
    response = requests.get('https://docs.dagster.io/assets/cereal.csv')
    lines = response.text.split('\n')
    cereals = [row for row in csv.DictReader(lines)]

    return cereals


            # *Show them the data Pip has downloaded*


# And play with the data like he likes
def find_highest_whatever(cereals, metric):
    sorted_cereals = sorted(cereals, 
                        key=lambda cereal: cereal[metric])
    sorted_cereals = list(sorted_cereals)
    return sorted_cereals[-1]['name']


# And for sure, get what he is looking for:
if __name__ == '__main__':
    metric = 'calories'
    print(f'The one with the highest amount of {metric} is', 
        find_highest_whatever(
            cereals=download_cereals(), 
            metric=metric,
            ),
        )
