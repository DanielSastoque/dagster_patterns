from dagster import op, job
from time import sleep

@op
def wait_one():
    sleep(1)


@op
def wait_two(dummy=None):
    sleep(2)


@job
def sleeper():
    wait_two(wait_one())

    wait_one.alias(name='wait_one_parallel')()
    wait_two.alias(name='wait_two_parallel')()
