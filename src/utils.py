import logging
import os

import boto3

TEST_DB = 'nwcapture-test'
QA_SB = 'nwcapture-qa'
TEST_TRIGGER = 'aqts-capture-trigger-TEST-aqtsCaptureTrigger'
QA_TRIGGER = 'aqts-capture-trigger-QA-aqtsCaptureTrigger'


def describe_db_clusters(action):
    my_rds = boto3.client('rds', os.environ['AWS_DEPLOYMENT_REGION'])

    # Get all the instances
    response = my_rds.describe_db_clusters()
    print(f"response from my_rds.describe_db_clusters {response}")
    all_dbs = response['DBClusters']
    if action == "start":
        # Filter on the one that are not running yet
        # Can use 'DBName' and filter that way
        rds_clusters = [x['DBClusterIdentifier'] for x in all_dbs if x['Status'] != 'available']
        return rds_clusters
    if action == "stop":
        # Filter on the one that are running
        rds_clusters = [x['DBClusterIdentifier'] for x in all_dbs if x['Status'] == 'available']
        return rds_clusters


def start_db_cluster(cluster_identifier):
    my_rds = boto3.client('rds', os.environ['AWS_DEPLOYMENT_REGION'])

    try:
        logging.info(f"would start cluster here for cluster {cluster_identifier}")
        # response = my_rds.start_db_cluster(
        #    DBClusterIdentifier=cluster_identifier
        # )
        return True
    except:
        logging.error(f"Cannot start {cluster_identifier}")
        return False


def stop_db_cluster(cluster_identifier):
    my_rds = boto3.client('rds', os.environ['AWS_DEPLOYMENT_REGION'])
    try:
        logging.info(f"would stop cluster here for cluster {cluster_identifier}")
        # response = my_rds.stop_db_cluster(
        #    DBClusterIdentifier=cluster_identifier
        # )
        return True
    except:
        logging.error(f"Cannot stop {cluster_identifier}")
        return False


def disable_trigger(function_name):
    my_lambda = boto3.client('lambda', os.getenv('AWS_DEPLOYMENT_REGION'))

    response = my_lambda.list_event_source_mappings(
        FunctionName=function_name
    )
    updated = False
    for item in response['EventSourceMappings']:
        logging.info(f"would disable trigger here for item {item}")
        # mapping = my_lambda.update_event_source_mapping(UUID=item['UUID'], Enabled=False)
        updated = True
    return updated


def enable_trigger(function_name):
    my_lambda = boto3.client('lambda', os.getenv('AWS_DEPLOYMENT_REGION'))
    response = my_lambda.list_event_source_mappings(
        FunctionName=function_name
    )
    updated = False
    for item in response['EventSourceMappings']:
        logging.info(f"would enable trigger here for item {item}")
        # mapping = my_lambda.update_event_source_mapping(UUID=item['UUID'], Enabled=True)
        updated = True
    return updated
