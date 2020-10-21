import datetime
import os

import boto3
from src.utils import enable_triggers, disable_triggers
import logging

# STAGES = ['TEST', 'QA', 'PROD']
# DB = {
#    "TEST": 'nwcapture-test',
#    "QA": 'nwcapture-qa',
#    "PROD": 'nwcapture-prod-external'
# }

# OBSERVATIONS_DB = {
#    "TEST": 'observations-test',
#    "QA": 'observations-qa',
#    "PROD": 'observations-prod-external'
# }


TRIGGER = {
    "TEST": ['aqts-capture-trigger-TEST-aqtsCaptureTrigger'],
    "QA": ['aqts-capture-trigger-QA-aqtsCaptureTrigger'],
    "PROD": ['aqts-capture-trigger-PROD-EXTERNAL-aqtsCaptureTrigger']
}

STAGE = os.getenv('STAGE', 'TEST')

DEFAULT_DB_CLUSTER_IDENTIFIER = f"nwcapture-{STAGE.lower()}"
DEFAULT_DB_INSTANCE_IDENTIFIER = f"{DEFAULT_DB_CLUSTER_IDENTIFIER}-instance1"
DEFAULT_DB_INSTANCE_CLASS = 'db.r5.8xlarge'
ENGINE = 'aurora-postgresql'
NWCAPTURE_REAL = f"NWCAPTURE-DB-{STAGE}"

log_level = os.getenv('LOG_LEVEL', logging.ERROR)
logger = logging.getLogger(__name__)
logger.setLevel(log_level)

STAGE = os.getenv('STAGE', 'TEST')

cloudwatch_client = boto3.client('cloudwatch', os.getenv('AWS_DEPLOYMENT_REGION', 'us-west-2'))
rds_client = boto3.client('rds', os.getenv('AWS_DEPLOYMENT_REGION', 'us-west-2'))

SMALL_DB_SIZE = 'db.r5.2xlarge'
BIG_DB_SIZE = 'db.r5.8xlarge'

"""
DB resize functions
"""


def disable_trigger_before_grow(event, context):
    response = rds_client.describe_db_instances(DBInstanceIdentifier=DEFAULT_DB_INSTANCE_IDENTIFIER)
    db_instance_class = str(response['DBInstances'][0]['DBInstanceClass'])
    if db_instance_class == BIG_DB_SIZE:
        raise Exception("Database has already grown")
    disable_triggers(TRIGGER[STAGE])


def disable_trigger_before_shrink(event, context):
    response = rds_client.describe_db_instances(DBInstanceIdentifier=DEFAULT_DB_INSTANCE_IDENTIFIER)
    db_instance_class = str(response['DBInstances'][0]['DBInstanceClass'])
    if db_instance_class == SMALL_DB_SIZE:
        raise Exception("Database has already shrunk")
    disable_triggers(TRIGGER[STAGE])


def enable_trigger_after_shrink(event, context):
    response = rds_client.describe_db_instances(DBInstanceIdentifier=DEFAULT_DB_INSTANCE_IDENTIFIER)
    db_instance_class = str(response['DBInstances'][0]['DBInstanceClass'])
    if db_instance_class == BIG_DB_SIZE:
        raise Exception("Database has not shrunk yet")
    if _is_cluster_available(DEFAULT_DB_CLUSTER_IDENTIFIER):
        enable_triggers(TRIGGER[STAGE])


def enable_trigger_after_grow(event, context):
    response = rds_client.describe_db_instances(DBInstanceIdentifier=DEFAULT_DB_INSTANCE_IDENTIFIER)
    db_instance_class = str(response['DBInstances'][0]['DBInstanceClass'])
    if db_instance_class == SMALL_DB_SIZE:
        raise Exception("Database has not grown yet")
    if _is_cluster_available(DEFAULT_DB_CLUSTER_IDENTIFIER):
        enable_triggers(TRIGGER[STAGE])


# TODO run this with 'breaching' and 'ignore' on TEST
def shrink_db(event, context):
    stage = os.environ['STAGE']
    identifier = f"nwcapture-{stage.lower()}-instance1"
    period = 300
    total_time = 900
    cpu_util = _get_cpu_utilization(identifier, period, total_time)
    print(f"cpu_util is {cpu_util}")
    logger.info(f"shrink db cpu_util = {cpu_util}")
    time_to_shrink = True
    values = cpu_util['MetricDataResults'][0]['Values']
    for value in values:
        if value > 25:
            time_to_shrink = False
    if time_to_shrink:
        logger.info(f"It's time to shrink the db {values}")
    else:
        logger.info(f"Not time to shrink the db {values}")
    print(f"time to shrink is {time_to_shrink}")
    response = rds_client.describe_db_instances(DBInstanceIdentifier=identifier)
    db_instance_class = str(response['DBInstances'][0]['DBInstanceClass'])
    print(f"db_instance_class = {db_instance_class}")
    if db_instance_class == SMALL_DB_SIZE:
        return "DB is already shrunk"
    else:
        response = rds_client.modify_db_instance(
            DBInstanceIdentifier=identifier,
            DBInstanceClass=SMALL_DB_SIZE,
            ApplyImmediately=True
        )
        return f"Shrinking DB, please stand by. {response}"


def execute_shrink_machine(event, context):
    arn = os.environ['STATE_MACHINE_ARN']
    payload = ""
    return execute_state_machine(arn, payload)


def execute_state_machine(state_machine_arn, invocation_payload, region='us-west-2'):
    """
    Kicks off execution of a state machine in AWS Step Functions.

    :param str state_machine_arn: Amazon Resource Number (ARN) for the step function to be triggered
    :param str invocation_payload: JSON data to kick off the step function
    :param str region: AWS region where the step function is deployed

    """
    sf = boto3.client('stepfunctions', region_name=region)
    resp = sf.start_execution(
        stateMachineArn=state_machine_arn,
        input=invocation_payload
    )
    return resp


def grow_db(event, context):
    logger.info(event)
    stage = os.environ['STAGE']
    identifier = f"nwcapture-{stage.lower()}-instance1"
    period = 300
    total_time = 300
    cpu_util = _get_cpu_utilization(identifier, period, total_time)
    time_to_grow = True
    values = cpu_util['MetricDataResults'][0]['Values']
    for value in values:
        if value < 70:
            time_to_grow = False
    if time_to_grow:
        logger.info(f"It's time to grow the db {values}")
    else:
        logger.info(f"Not time to grow the db {values}")
    logger.info(f"identifier {identifier} period {period} grow db cpu_util = {cpu_util}")

    response = rds_client.describe_db_instances(DBInstanceIdentifier=identifier)
    db_instance_class = str(response['DBInstances'][0]['DBInstanceClass'])
    if db_instance_class == BIG_DB_SIZE:
        return "DB is already at max size"
    else:
        disable_triggers(TRIGGER[stage])
        response = rds_client.modify_db_instance(
            DBInstanceIdentifier=identifier,
            DBInstanceClass=BIG_DB_SIZE,
            ApplyImmediately=True
        )
        return f"Growing DB, please stand by. {response}"


def _get_cpu_utilization(db_instance_identifier, period_in_seconds, total_time):
    response = cloudwatch_client.get_metric_data(
        MetricDataQueries=[
            {
                'Id': 'loadTestCpuUtilization',
                'MetricStat': {
                    'Metric': {
                        'Namespace': 'AWS/RDS',
                        'MetricName': 'CPUUtilization',
                        'Dimensions': [
                            {
                                "Name": "DBInstanceIdentifier",
                                "Value": db_instance_identifier
                            }]
                    },
                    'Period': period_in_seconds,
                    'Stat': 'Average',
                }
            }
        ],
        StartTime=(datetime.datetime.now() - datetime.timedelta(seconds=total_time)).timestamp(),
        EndTime=datetime.datetime.now().timestamp()
    )
    return response


def _validate():
    if os.environ['STAGE'] != "QA":
        raise Exception("This lambda is currently only supported on the QA tier")


def _is_cluster_available(cluster_id):
    response = rds_client.describe_db_clusters()
    all_dbs = response['DBClusters']
    available_clusters = [x['DBClusterIdentifier'] for x in all_dbs if x['Status'] == 'available']
    if cluster_id in available_clusters:
        return True
    else:
        raise Exception(f"DB {DEFAULT_DB_CLUSTER_IDENTIFIER} is not ready yet")
