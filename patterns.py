import random
import time

from dagster import (
    DynamicOut,
    DynamicOutput,
    GraphOut,
    In,
    Nothing,
    Out, 
    Output,
    config_mapping,
    graph, 
    job, 
    op,
    repository,
    get_dagster_logger,
)
from pyparsing import line, nums

#------------------------------------------------------#
# Branching #

@op(out={'branch_1': Out(is_required=False),
         'branch_2': Out(is_required=False),
         })
def branchig_op():
    num = random.randint(0, 1)
    if num == 0:
        yield Output(value=1, output_name='branch_1')
    else:
        yield Output(value=2, output_name='branch_2')


@op
def branch_1_op(_input):
    pass


@op
def branch_2_op(_input):
    pass


@job
def branching():
    branch_1, branch_2 = branchig_op()
    branch_1_op(branch_1)
    branch_2_op(branch_2)

#------------------------------------------------------#
# Multiple input

@op
def return_one():
    time.sleep(1)
    return 1


@op
def add_one(number):
    return number + 1


@op
def adder(a, b):
    return a + b


@job
def inputs_and_outputs():
    value = return_one()
    a, b = add_one(value), add_one(value)
    adder(a, b)

#------------------------------------------------------#
# Fan in

@op
def sum_fan_in(nums):
    return sum(nums)


@job
def fan_in():
    sum_fan_in([return_one() for i in range(10)])


#------------------------------------------------------#
# Linear

@job
def linear():
    df = return_one()

    for operator in [add_one]*10:
        df = operator(df)


#------------------------------------------------------#
# Nothing dependency

@op(ins={'start': In(Nothing)})
def give_me_nothing():
    get_dagster_logger().info('Order dependency')


@job
def nothing_job():
    give_me_nothing(start=return_one())


#------------------------------------------------------#
# Nested graph

@op
def return_fifty():
    return 50

@op(config_schema={'n': float})
def add_n(context, number):
    return number + context.op_config['n']

@op(config_schema={'factor': float})
def multiply_by_factor(context, number):
    return number * context.op_config['factor']

@op
def log_number(number):
    get_dagster_logger().info(f'Number = {number}')


@config_mapping(config_schema={'from_unit': str})
def generate_config(val):
    n = {'celsius': 32, 'kelvin': -459.67}.get(val['from_unit'], 0)

    return {'multiply_by_factor': {'config': {'factor': 1.8}},
            'add_n': {'config': {'n': n}},
            }

@graph(config=generate_config)
def to_fahrenheit(number):
    return add_n(multiply_by_factor(number))

@job
def nested():
    log_number(to_fahrenheit(return_fifty()))


#------------------------------------------------------#
# Multiple outputs

@op
def echo(i):
    print(i)

@op
def one() -> int:
    return 1

@op
def hello() -> str:
    return "hello"

@graph(out={'x': GraphOut(), 'y': GraphOut()})
def graph_multiple_output():
    return {'x': one(), 'y': hello()}

@job
def multiple_out():
    x, y = graph_multiple_output()
    echo(x), echo(y)


#------------------------------------------------------#
# Dynamic output

@op(out=DynamicOut())
def dynamic_out():
    n_branches = random.randint(0, 10)
    get_dagster_logger().info(f'Amount of branches: {n_branches}')

    for idx in range(n_branches):
        yield DynamicOutput(idx, mapping_key=str(idx))

@job
def dynamic_graph():
    outputs = dynamic_out()
    results = outputs.map(add_one)
    sum_fan_in(results.collect())


#------------------------------------------------------#
@repository
def my_patterns():
    return [
        inputs_and_outputs,
        branching,
        fan_in,
        linear,
        nothing_job,
        nested,
        multiple_out,
        dynamic_graph,
    ]
