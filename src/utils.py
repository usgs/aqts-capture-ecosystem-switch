import logging
import os
import boto3

log = logging.getLogger()
log.setLevel(logging.DEBUG)

TEST_DB = 'nwcapture-test'
QA_SB = 'nwcapture-qa'
TEST_TRIGGER = 'aqts-capture-trigger-TEST-aqtsCaptureTrigger'
QA_TRIGGER = 'aqts-capture-trigger-QA-aqtsCaptureTrigger'


def describe_db_clusters(action):
    my_rds = boto3.client('rds', os.environ['AWS_DEPLOYMENT_REGION'])
    # Get all the instances
    response = my_rds.describe_db_clusters()
    all_dbs = response['DBClusters']
    if action == "start":
        # Filter on the one that are not running yet
        rds_clusters = [x['DBClusterIdentifier'] for x in all_dbs if x['Status'] != 'available']
        return rds_clusters
    if action == "stop":
        # Filter on the one that are running
        rds_clusters = [x['DBClusterIdentifier'] for x in all_dbs if x['Status'] == 'available']
        return rds_clusters


def start_db_cluster(cluster_identifier):
    my_rds = boto3.client('rds', os.environ['AWS_DEPLOYMENT_REGION'])
    print(f"starting cluster here for cluster {cluster_identifier}")
    response = my_rds.start_db_cluster(
        DBClusterIdentifier=cluster_identifier
    )
    print(f"start db cluster response {response}")
    return True


def stop_db_cluster(cluster_identifier):
    my_rds = boto3.client('rds', os.environ['AWS_DEPLOYMENT_REGION'])
    print(f"stopping cluster here for cluster {cluster_identifier}")
    response = my_rds.stop_db_cluster(
        DBClusterIdentifier=cluster_identifier
    )
    print(f"stop db cluster response {response}")
    return True


def disable_trigger(function_name):
    my_lambda = boto3.client('lambda', os.getenv('AWS_DEPLOYMENT_REGION'))
    response = my_lambda.list_event_source_mappings(
        FunctionName=function_name
    )
    for item in response['EventSourceMappings']:
        print(f"disabling trigger here for item {item}")
        mapping = my_lambda.update_event_source_mapping(UUID=item['UUID'], Enabled=False)
        print(f"should be disabled {mapping}")
        return True


def enable_trigger(function_name):
    my_lambda = boto3.client('lambda', os.getenv('AWS_DEPLOYMENT_REGION'))
    response = my_lambda.list_event_source_mappings(
        FunctionName=function_name
    )
    for item in response['EventSourceMappings']:
        print(f"enabling trigger here for item {item}")
        mapping = my_lambda.update_event_source_mapping(UUID=item['UUID'], Enabled=True)
        print(f"should be enabled {mapping}")
    return True

