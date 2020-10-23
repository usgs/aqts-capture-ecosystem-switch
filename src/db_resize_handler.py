import datetime
import json
import os

import boto3
from src.utils import enable_triggers, disable_triggers
import logging

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

SMALL_DB_SIZE = 'db.r5.2xlarge'
BIG_DB_SIZE = 'db.r5.8xlarge'

cloudwatch_client = boto3.client('cloudwatch', os.getenv('AWS_DEPLOYMENT_REGION', 'us-west-2'))
rds_client = boto3.client('rds', os.getenv('AWS_DEPLOYMENT_REGION', 'us-west-2'))

log_level = os.getenv('LOG_LEVEL', logging.ERROR)
logger = logging.getLogger(__name__)
logger.setLevel(log_level)

"""
DB resize functions
"""


def disable_trigger_before_resize(event, context):
    if event.get("resize_action") is None:
        raise Exception(f"No resize action in event {event}")
    response = rds_client.describe_db_instances(DBInstanceIdentifier=DEFAULT_DB_INSTANCE_IDENTIFIER)
    db_instance_class = str(response['DBInstances'][0]['DBInstanceClass'])
    if event.get("resize_action") == "GROW":
        if db_instance_class == BIG_DB_SIZE:
            raise Exception("Database has already grown")
    elif event.get("resize_action") == "SHRINK":
        if db_instance_class == SMALL_DB_SIZE:
            raise Exception("Database has already shrunk")
    else:
        raise Exception(f"Unrecognized resize action {event}")
    disable_triggers(TRIGGER[STAGE])


def enable_trigger_after_resize(event, context):
    if event.get("resize_action") is None:
        raise Exception(f"Invalid resize action, misconfigured event {event}")
    response = rds_client.describe_db_instances(DBInstanceIdentifier=DEFAULT_DB_INSTANCE_IDENTIFIER)
    db_instance_class = str(response['DBInstances'][0]['DBInstanceClass'])
    if event.get('resize_action') == 'GROW':
        if db_instance_class == SMALL_DB_SIZE:
            raise Exception("Database has not grown yet, keep waiting before enable trigger")
    elif event.get('resize_action') == 'SHRINK':
        if db_instance_class == BIG_DB_SIZE:
            raise Exception("Database has not shrunk yet, keep waiting before enable trigger")
    else:
        raise Exception(f"Unrecognized resize action {event}")
    if _is_cluster_available(DEFAULT_DB_CLUSTER_IDENTIFIER):
        enable_triggers(TRIGGER[STAGE])
        return


def shrink_db(event, context):
    threshold = int(os.environ['SHRINK_THRESHOLD'])
    shrink_eval_time = int(os.environ['SHRINK_EVAL_TIME_IN_SECONDS'])
    period = shrink_eval_time
    total_time = shrink_eval_time
    cpu_util = _get_cpu_utilization(DEFAULT_DB_INSTANCE_IDENTIFIER, period, total_time)
    logger.info(f"shrink db cpu_util = {cpu_util}")
    print(f"cpuutil={cpu_util}")
    time_to_shrink = True
    values = cpu_util['MetricDataResults'][0]['Values']
    for value in values:
        if value > threshold:
            time_to_shrink = False
    if time_to_shrink:
        logger.info(f"It's time to shrink the db {values}")
    else:
        logger.info(f"Not time to shrink the db {values}")
        return False
    response = rds_client.describe_db_instances(DBInstanceIdentifier=DEFAULT_DB_INSTANCE_IDENTIFIER)
    db_instance_class = str(response['DBInstances'][0]['DBInstanceClass'])
    if db_instance_class == SMALL_DB_SIZE:
        logger.info("DB is already shrunk")
        return False
    else:
        response = rds_client.modify_db_instance(
            DBInstanceIdentifier=DEFAULT_DB_INSTANCE_IDENTIFIER,
            DBInstanceClass=SMALL_DB_SIZE,
            ApplyImmediately=True
        )
        logger.info(f"Shrinking DB, please stand by. {response}")
        return True


def grow_db(event, context):
    threshold = int(os.environ['GROW_THRESHOLD'])
    grow_eval_time = int(os.environ['GROW_EVAL_TIME_IN_SECONDS'])
    period = grow_eval_time
    total_time = grow_eval_time
    cpu_util = _get_cpu_utilization(DEFAULT_DB_INSTANCE_IDENTIFIER, period, total_time)
    logger.info(f"grow db cpu_util = {cpu_util}")
    time_to_grow = True
    values = cpu_util['MetricDataResults'][0]['Values']
    for value in values:
        if value < threshold:
            time_to_grow = False
    if time_to_grow:
        logger.info(f"It's time to grow the db {values}")
    else:
        logger.info(f"Not time to grow the db {values}")
        return False
    response = rds_client.describe_db_instances(DBInstanceIdentifier=DEFAULT_DB_INSTANCE_IDENTIFIER)
    db_instance_class = str(response['DBInstances'][0]['DBInstanceClass'])
    if db_instance_class == BIG_DB_SIZE:
        logger.info("DB is already grown")
        return False
    else:
        response = rds_client.modify_db_instance(
            DBInstanceIdentifier=DEFAULT_DB_INSTANCE_IDENTIFIER,
            DBInstanceClass=BIG_DB_SIZE,
            ApplyImmediately=True
        )
        logger.info(f"Growing the DB, please stand by. {response}")
        return True


def execute_shrink_machine(event, context):
    arn = os.environ['SHRINK_STATE_MACHINE_ARN']
    payload = {"resize_action": "SHRINK"}
    return _execute_state_machine(arn, json.dumps(payload))


def execute_grow_machine(event, context):
    arn = os.environ['GROW_STATE_MACHINE_ARN']
    payload = {"resize_action": "GROW"}
    alarm_state = event["detail"]["state"]["value"]
    if alarm_state == "ALARM":
        _execute_state_machine(arn, json.dumps(payload))
        return True
    return False


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


def _execute_state_machine(state_machine_arn, invocation_payload, region='us-west-2'):
    sf = boto3.client('stepfunctions', region_name=region)
    resp = sf.start_execution(
        stateMachineArn=state_machine_arn,
        input=invocation_payload
    )
    return resp
