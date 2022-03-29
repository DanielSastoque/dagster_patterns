from dagster import (
    get_dagster_logger,
    job,
    op,
    repository,
    resource,
)


class ExternalCerealFetcher:
    def fetch_new_cereals(self, start_ts=None, ent_ts=None):
        return 'Resource body'


@resource
def resource_def(context):
    return ExternalCerealFetcher()


@op(required_resource_keys={'resource_key'})
def op_uses_resource(context):
    resource_out = context.resources.resource_key.fetch_new_cereals()
    get_dagster_logger().info(resource_out)


@job(resource_defs={'resource_key': resource_def})
def job_with_resource():
    op_uses_resource()


@repository
def resources_jobs():
    return [
        job_with_resource,
    ]
