import os
import boto3
import logging

log_level = os.getenv('LOG_LEVEL', logging.ERROR)
logger = logging.getLogger(__name__)
logger.setLevel(log_level)

DEFAULT_DB_INSTANCE_CLASS = 'db.r5.8xlarge'


def describe_db_clusters(action):
    # Get all the instances
    my_rds = boto3.client('rds', os.getenv('AWS_DEPLOYMENT_REGION', 'us-west-2'))
    response = my_rds.describe_db_clusters()
    all_dbs = response['DBClusters']
    if action == "start":
        # Filter on the one that are not running yet
        rds_cluster_identifiers = [x['DBClusterIdentifier'] for x in all_dbs if x['Status'] != 'available']
        return rds_cluster_identifiers
    if action == "stop":
        # Filter on the one that are running

        rds_cluster_identifiers = [x['DBClusterIdentifier'] for x in all_dbs if x['Status'] == 'available']
        return rds_cluster_identifiers


def start_db_cluster(cluster_identifier):
    my_rds = boto3.client('rds', os.getenv('AWS_DEPLOYMENT_REGION', 'us-west-2'))
    my_rds.start_db_cluster(
        DBClusterIdentifier=cluster_identifier
    )
    return True


def stop_db_cluster(cluster_identifier):
    my_rds = boto3.client('rds', os.getenv('AWS_DEPLOYMENT_REGION', 'us-west-2'))
    my_rds.stop_db_cluster(
        DBClusterIdentifier=cluster_identifier
    )
    return True


def stop_observations_db_instance(instance_identifier):
    my_rds = boto3.client('rds', os.getenv('AWS_DEPLOYMENT_REGION', 'us-west-2'))
    my_rds.stop_db_instance(DBInstanceIdentifier=instance_identifier)


def purge_queue(queue_names):
    for queue_name in queue_names:
        sqs = boto3.client('sqs', os.getenv('AWS_DEPLOYMENT_REGION'))
        queue_info = sqs.get_queue_url(QueueName=queue_name)
        sqs.purge_queue(QueueUrl=queue_info['QueueUrl'])


def disable_lambda_trigger(function_names):
    my_lambda = boto3.client('lambda', os.getenv('AWS_DEPLOYMENT_REGION', 'us-west-2'))
    return_value = False
    for function_name in function_names:
        response = my_lambda.list_event_source_mappings(FunctionName=function_name)
        for item in response['EventSourceMappings']:
            response = my_lambda.get_event_source_mapping(UUID=item['UUID'])
            if response['State'] in ('Enabled', 'Enabling', 'Updating', 'Creating'):
                my_lambda.update_event_source_mapping(UUID=item['UUID'], Enabled=False)
                response = my_lambda.get_event_source_mapping(UUID=item['UUID'])
                return_value = True
                logger.info(f"Trigger should be disabled.  function name: {function_name} item: {response}")
    return return_value


def enable_lambda_trigger(function_names):
    my_lambda = boto3.client('lambda', os.getenv('AWS_DEPLOYMENT_REGION', 'us-west-2'))
    return_value = False
    for function_name in function_names:
        response = my_lambda.list_event_source_mappings(FunctionName=function_name)
        for item in response['EventSourceMappings']:
            response = my_lambda.get_event_source_mapping(UUID=item['UUID'])
            print(response)
            if response['State'] in ('Disabled', 'Disabling', 'Updating', 'Creating'):
                my_lambda.update_event_source_mapping(UUID=item['UUID'], Enabled=True)
                response = my_lambda.get_event_source_mapping(UUID=item['UUID'])
                logger.info(f"Trigger should be enabled.  function name: {function_name} item: {response}")
                return_value = True
    return return_value
