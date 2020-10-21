import datetime
import json
import os

import boto3
from src.rds import RDS
from src.utils import enable_triggers, describe_db_clusters, start_db_cluster, disable_triggers, stop_db_cluster, \
    purge_queue, stop_observations_db_instance
import logging

STAGES = ['TEST', 'QA', 'PROD']
DB = {
    "TEST": 'nwcapture-test',
    "QA": 'nwcapture-qa',
    "PROD": 'nwcapture-prod-external'
}

OBSERVATIONS_DB = {
    "TEST": 'observations-test',
    "QA": 'observations-qa',
    "PROD": 'observations-prod-external'
}

SQS = {
    "TEST": ['aqts-capture-trigger-queue-TEST', 'aqts-capture-error-queue-TEST'],
    "QA": ['aqts-capture-trigger-queue-QA', 'aqts-capture-error-queue-QA'],
    "PROD": ['aqts-capture-trigger-queue-PROD-EXTERNAL']
}

TRIGGER = {
    "TEST": ['aqts-capture-trigger-TEST-aqtsCaptureTrigger'],
    "QA": ['aqts-capture-trigger-QA-aqtsCaptureTrigger'],
    "PROD": ['aqts-capture-trigger-PROD-EXTERNAL-aqtsCaptureTrigger']
}

STAGE = os.getenv('STAGE', 'TEST')
CAPTURE_TRIGGER_QUEUE = f"aqts-capture-trigger-queue-{STAGE}"
ERROR_QUEUE = f"aqts-capture-error-queue-{STAGE}"

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
secrets_client = boto3.client('secretsmanager', os.getenv('AWS_DEPLOYMENT_REGION', 'us-west-2'))
rds_client = boto3.client('rds', os.getenv('AWS_DEPLOYMENT_REGION', 'us-west-2'))
sqs_client = boto3.client('sqs', os.getenv('AWS_DEPLOYMENT_REGION', 'us-west-2'))

DEFAULT_DB_INSTANCE_IDENTIFIER = f"nwcapture-{STAGE.lower()}-instance1"
DEFAULT_DB_CLUSTER_IDENTIFIER = f"nwcapture-{STAGE.lower()}"
SMALL_DB_SIZE = 'db.r5.2xlarge'
BIG_DB_SIZE = 'db.r5.8xlarge'


def _get_etl_start():
    four_days_ago = datetime.datetime.now() - datetime.timedelta(4)
    my_month = str(four_days_ago.month)
    if len(my_month) == 1:
        my_month = f"0{my_month}"
    my_day = str(four_days_ago.day)
    if len(my_day) == 1:
        my_day = f"0{my_day}"
    my_etl_start = f"{four_days_ago.year}-{my_month}-{my_day}"
    return my_etl_start


etl_start = _get_etl_start()
OBSERVATIONS_ETL_IN_PROGRESS_SQL = \
    "select count(1) from batch_job_execution where status not in ('COMPLETED', 'FAILED') and start_time > %s"

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
    payload = {}
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


"""
DB stop and start functions
"""


def start_capture_db(event, context):
    stage = os.getenv('STAGE')
    if stage in STAGES:
        started = _start_db(DB[stage], TRIGGER[stage], SQS[stage])
    else:
        raise Exception(f"stage not recognized {os.getenv('STAGE')}")
    return {
        'statusCode': 200,
        'message': f"Started the {stage} db: {started}"
    }


def stop_capture_db(event, context):
    stage = os.getenv('STAGE')
    if stage in STAGES:
        stopped = _stop_db(DB[stage], TRIGGER[stage])
    else:
        raise Exception(f"stage not recognized {os.getenv('STAGE')}")
    return {
        'statusCode': 200,
        'message': f"Stopped the {os.getenv('STAGE')} db: {stopped}"
    }


def stop_observations_db(event, context):
    stage = os.getenv('STAGE')
    if stage not in STAGES:
        raise Exception(f"stage not recognized {os.getenv('STAGE')}")
    should_stop = run_etl_query()
    if not should_stop:
        return {
            'statusCode': 200,
            'message': f"Could not stop the {stage} observations db. It was busy."
        }
    stop_observations_db_instance(OBSERVATIONS_DB[stage])
    return {
        'statusCode': 200,
        'message': f"Stopped the {os.getenv('STAGE')} observations db."
    }


def start_observations_db(event, context):
    stage = os.getenv('STAGE')
    if stage not in STAGES:
        raise Exception(f"stage not recognized {os.getenv('STAGE')}")
    rds_client = boto3.client('rds', os.getenv('AWS_DEPLOYMENT_REGION', 'us-west-2'))
    rds_client.start_db_instance(DBInstanceIdentifier=OBSERVATIONS_DB[stage])
    return {
        'statusCode': 200,
        'message': f"Started the {stage} observations db."
    }


def control_db_utilization(event, context):
    """
    Right now we are only listening for the error handler alarm, because it
    is the last alarm to get triggered when we are going into a death spiral.
    :param event:
    :param context:
    :return:
    """
    logger.info(event)
    alarm_state = event["detail"]["state"]["value"]
    stage = os.getenv('STAGE')
    if stage not in STAGES:
        raise Exception(f"stage not recognized {os.getenv('STAGE')}")
    if alarm_state == "ALARM":
        logger.info(f"Disabling trigger {TRIGGER[stage]} because error handler is in alarm")
        disable_triggers(TRIGGER[stage])
    elif alarm_state == "OK":
        """
        We do NOT want to enable the trigger if the db is not up and running.
        """
        active_dbs = describe_db_clusters('stop')
        if DB[stage] in active_dbs:
            logger.info(f"Enabling trigger {TRIGGER[stage]} for {DB[stage]} because error handler is okay")
            enable_triggers(TRIGGER[stage])


def run_etl_query(rds=None):
    """
    If we look in the batch_job_executions table and something is in a state other than COMPLETED or FAILED,
    assume an ETL is in progress and don't shut the db down.

    Also, if someone aborts an ETL job in Jenkins, we can get in a messed up state where a job execution has
    STARTED but never fails or completes, so check the start time and if it's more than 4 days ignore those and
    shut the db down anyway.
    """
    if rds is None:
        # TODO db host, db user, db address, db password ... get from secrets
        rds = RDS()
    result = rds.execute_sql(OBSERVATIONS_ETL_IN_PROGRESS_SQL, (etl_start,))
    if result[0] > 0:
        logger.debug(f"Cannot shutdown down observations db because {result[0]} processes are running")
        return False
    elif result[0] == 0:
        logger.debug("Shutting down observations db because no processes are running")
        return True
    else:
        raise Exception(f"something wrong with db result {result}")


def _start_db(db, triggers, queue_name):
    purge_queue(queue_name)
    cluster_identifiers = describe_db_clusters("start")
    started = False
    for cluster_identifier in cluster_identifiers:
        if cluster_identifier == db:
            start_db_cluster(db)
            started = True
            enable_triggers(triggers)
    return started


def _stop_db(db, triggers):
    cluster_identifiers = describe_db_clusters("stop")
    stopped = False
    disable_triggers(triggers)
    for cluster_identifier in cluster_identifiers:
        if cluster_identifier == db:
            stop_db_cluster(db)
            stopped = True
    return stopped


"""
DB create and delete functions
"""


def modify_postgres_password(event, context):
    _validate()
    logger.info("enter modify postgres password")
    logger.info(event)
    original = secrets_client.get_secret_value(
        SecretId=NWCAPTURE_REAL,
    )
    secret_string = json.loads(original['SecretString'])
    postgres_password = secret_string['POSTGRES_PASSWORD']

    response = rds_client.describe_db_clusters()
    logger.info(f" all clusters {response}")
    rds_client.modify_db_cluster(
        DBClusterIdentifier=DEFAULT_DB_CLUSTER_IDENTIFIER,
        ApplyImmediately=True,
        MasterUserPassword=postgres_password
    )


def delete_capture_db(event, context):
    _validate()
    try:
        rds_client.delete_db_instance(
            DBInstanceIdentifier=DEFAULT_DB_INSTANCE_IDENTIFIER,
            SkipFinalSnapshot=True
        )
    except rds_client.exceptions.DBInstanceNotFoundFault as e:
        """
        We could be in a messed up state where the instance doesn't exist but the cluster does,
        due to vagaries of how long AWS takes to set up a cluster, so proceed
        """

    rds_client.delete_db_cluster(
        DBClusterIdentifier=DEFAULT_DB_CLUSTER_IDENTIFIER,
        SkipFinalSnapshot=True
    )
    disable_triggers(TRIGGER[os.environ['STAGE']])


def create_db_instance(event, context):
    _validate()
    logger.info("enter create db instance")
    logger.info(event)
    stage = os.environ['STAGE'].lower()
    rds_client.create_db_instance(
        DBInstanceIdentifier=DEFAULT_DB_INSTANCE_IDENTIFIER,
        DBInstanceClass=DEFAULT_DB_INSTANCE_CLASS,
        DBClusterIdentifier=DEFAULT_DB_CLUSTER_IDENTIFIER,
        Engine=ENGINE,
        Tags=[
            {'Key': 'Name', 'Value': f"NWISWEB-CAPTURE-RDS-AURORA-{stage.upper()}"},
            {'Key': 'wma:applicationId', 'Value': 'NWISWEB-CAPTURE'},
            {'Key': 'wma:contact', 'Value': 'tbd'},
            {'Key': 'wma:costCenter', 'Value': 'tbd'},
            {'Key': 'wma:criticality', 'Value': 'tbd'},
            {'Key': 'wma:environment', 'Value': stage},
            {'Key': 'wma:operationalHours', 'Value': 'tbd'},
            {'Key': 'wma:organization', 'Value': 'tbd'},
            {'Key': 'wma:role', 'Value': 'database'},
            {'Key': 'wma:system', 'Value': 'NWIS'},
            {'Key': 'wma:subSystem', 'Value': 'NWISWeb-Capture'},
            {'Key': 'taggingVersion', 'Value': '0.0.1'}]
    )


def restore_db_cluster(event, context):
    _validate()
    logger.info("enter restore db cluster")
    logger.info(event)

    original = secrets_client.get_secret_value(
        SecretId=NWCAPTURE_REAL
    )
    secret_string = json.loads(original['SecretString'])
    kms_key = str(secret_string['KMS_KEY_ID'])
    subgroup_name = str(secret_string['DB_SUBGROUP_NAME'])
    vpc_security_group_id = str(secret_string['VPC_SECURITY_GROUP_ID'])
    if not kms_key or not subgroup_name or not vpc_security_group_id:
        raise Exception(f"Missing db configuration data {secret_string}")
    my_snapshot_identifier = get_snapshot_identifier()
    if event is not None:
        if event.get("db_config") is not None and event['db_config'].get('snapshot_identifier') is not None:
            my_snapshot_identifier = event['db_config'].get("snapshot_identifier")

    rds_client.restore_db_cluster_from_snapshot(
        DBClusterIdentifier=DEFAULT_DB_CLUSTER_IDENTIFIER,
        SnapshotIdentifier=my_snapshot_identifier,
        Engine=ENGINE,
        EngineVersion='11.7',
        Port=5432,
        DBSubnetGroupName=subgroup_name,
        DatabaseName=DB[os.environ['STAGE']],
        EnableIAMDatabaseAuthentication=False,
        EngineMode='provisioned',
        DBClusterParameterGroupName='aqts-capture',
        DeletionProtection=False,
        CopyTagsToSnapshot=False,
        KmsKeyId=kms_key,
        VpcSecurityGroupIds=[
            vpc_security_group_id
        ],
        Tags=[
            {
                'Key': 'Name',
                'Value': f"NWISWEB-CAPTURE-RDS-AURORA-{STAGE}"
            },
            {
                'Key': 'wma:applicationId',
                'Value': 'NWISWEB-CAPTURE'
            },
            {
                'Key': 'wma:contact',
                'Value': 'tbd'
            },
            {
                'Key': 'wma:costCenter',
                'Value': 'tbd'
            },
            {
                'Key': 'wma:criticality',
                'Value': 'tbd'
            },
            {
                'Key': 'wma:environment',
                'Value': 'qa'
            },
            {
                'Key': 'wma:operationalHours',
                'Value': 'tbd'
            },
            {
                'Key': 'wma:organization',
                'Value': 'tbd'
            },
            {
                'Key': 'wma:role',
                'Value': 'database'
            },
            {
                'Key': 'wma:system',
                'Value': 'NWIS'
            },
            {
                'Key': 'wma:subSystem',
                'Value': 'NWISWeb-Capture'
            },
            {
                'Key': 'taggingVersion',
                'Value': '0.0.1'
            }
        ]
    )


def modify_schema_owner_password(event, context):
    _validate()
    logger.info(event)
    """
    We don't know the password for 'capture_owner' on the production db,
    but we have already changed the postgres password in the modifyDbCluster step.
    So change the password for 'capture_owner' here.
    :param event:
    :param context:
    :return:
    """
    original = secrets_client.get_secret_value(
        SecretId=NWCAPTURE_REAL,
    )
    secret_string = json.loads(original['SecretString'])
    db_host = secret_string['DATABASE_ADDRESS']
    db_name = secret_string['DATABASE_NAME']
    postgres_password = secret_string['POSTGRES_PASSWORD']
    schema_owner_password = secret_string['SCHEMA_OWNER_PASSWORD']
    logger.info(
        f"db_host {db_host} db_name {db_name} postgres_password {postgres_password} sop {schema_owner_password}")
    rds = RDS(db_host, 'postgres', db_name, postgres_password)
    logger.info("got rds ok")
    sql = "alter user capture_owner with password %s"
    rds.alter_permissions(sql, (schema_owner_password,))

    queue_info = sqs_client.get_queue_url(QueueName=CAPTURE_TRIGGER_QUEUE)
    sqs_client.purge_queue(QueueUrl=queue_info['QueueUrl'])
    queue_info = sqs_client.get_queue_url(QueueName=ERROR_QUEUE)
    sqs_client.purge_queue(QueueUrl=queue_info['QueueUrl'])

    enable_triggers(TRIGGER[os.environ['STAGE']])


def get_snapshot_identifier():
    two_days_ago = datetime.datetime.now() - datetime.timedelta(2)
    month = str(two_days_ago.month)
    if len(month) == 1:
        month = f"0{month}"
    day = str(two_days_ago.day)
    if len(day) == 1:
        day = f"0{day}"
    return f"rds:nwcapture-prod-external-{two_days_ago.year}-{month}-{day}-10-08"


"""
Miscellaneous functions
"""


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
