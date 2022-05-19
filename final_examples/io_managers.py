from dagster import (
    IOManager,
    Out,
    fs_asset_io_manager,
    fs_io_manager,
    io_manager,
    job,
    op,
    repository,
)

import pandas as pd
import numpy as np


#------------------------------------------------------#
# Simple IO manager with the file system

@op
def op_1():
    return 1


@op
def op_2(a):
    return a + 1


@job(resource_defs={'io_manager': fs_io_manager})
def simple_io_manager():
    op_2(op_1())


#------------------------------------------------------#
# Output from pandas to csv


class DataframeTableIOManager(IOManager):
    def handle_output(self, context, obj):
        file_name = context.metadata['file_name']
        obj.to_csv(f'{file_name}.csv', index=False)

    def load_input(self, context):
        file_name = context.upstream_output.metadata['file_name']
        return pd.read_csv(f'{file_name}.csv')


@io_manager
def io_manager_definition(_):
    return DataframeTableIOManager()


@op(out=Out(metadata={'file_name': 'pandas_op'}))
def pandas_op():
    df = pd.DataFrame(np.random.randint(0, 100, size=(100, 4)), columns=list('ABCD'))
    return df


@op(out=Out(metadata={'file_name': 'pandas_sum'}))
def pandas_sum(df):
    df['sum'] = df.sum(axis=1)
    return df[['sum']]


@job(resource_defs={'io_manager': io_manager_definition})
def job_with_custom_io_manager():
    pandas_sum(df=pandas_op())


@repository
def jobs_with_io_manager():
    return [
        simple_io_manager,
        job_with_custom_io_manager,
    ]


if __name__ == '__main__':
    result = job_with_custom_io_manager.execute_in_process()
