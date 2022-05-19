from random import randint
from dagster import (
    AssetMaterialization,
    MetadataValue,
    Output,
    RetryRequested,
    failure_hook,
    get_dagster_logger,
    job,
    op,
    repository,
    In,
    Nothing,
    success_hook,
)


sample_metadata = {
            'text_metadata': 'This is a metadata message',
            'dashboard_url': MetadataValue.url('https://www.google.com/'),
            'raw_count': 0,
            'custom message': 'Here will be a custom message',
            'size (bytes)': 100,
        }


#------------------------------------------------------#
# Output with metadata

@op
def output_with_meta():
    data = {'A': 1, 'B': 2}
    sample_metadata['raw_count'] = len(data)

    yield Output(
        value=data,
        metadata=sample_metadata,
    )


@job
def simple_job_with_meta():
    data_out = output_with_meta()


#------------------------------------------------------#
# Asset materialization


@op(ins={'start': In(Nothing)})
def asset_event(context):
    for metric in ['raw_count', 'size (bytes)']:
        sample_metadata[metric] = randint(0, 100)

    context.log_event(
        AssetMaterialization(
            asset_key='testing.do_nothing',
            description='Event triggered from an op',
            metadata=sample_metadata,
        )
    )


@job
def asset_job():
    init = asset_event()
    for _ in range(20):
        init = asset_event(start=init)


#------------------------------------------------------#
# Robust pipeline with retries

@op
def op_with_retry():
    value = randint(0, 20)
    get_dagster_logger().info(f'value = {value}')
    try:
        if value > 2:
            raise ValueError
    except ValueError as e:
        raise RetryRequested(max_retries=10) from e

    yield Output(value, metadata={'value': value})


@job
def job_with_retry():
    op_with_retry()


#------------------------------------------------------#
# Hooks on failure and on success

@success_hook
def trigger_when_succeed(context):
    message = f'Op {context.op.name} has succeed'
    get_dagster_logger().info(message)


@failure_hook
def trigger_when_fails(context):
    message = f'Op {context.op.name} has failed'
    get_dagster_logger().info(message)


@op
def binary_op():
    n = randint(0, 1)
    if n == 0:
        raise ValueError


@job(hooks={
            trigger_when_fails,
            trigger_when_succeed,
            }
    )
def job_with_hooks():
    binary_op()


@repository
def enriched_repo():
    return [
        simple_job_with_meta,
        asset_job,
        job_with_retry,
        job_with_hooks,
        ]
