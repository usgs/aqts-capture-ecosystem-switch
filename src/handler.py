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
secrets_client = boto3.client('secretsmanager', os.getenv('AWS_DEPLOYMENT_REGION', 'us-west-2'))
rds_client = boto3.client('rds', os.getenv('AWS_DEPLOYMENT_REGION', 'us-west-2'))
sqs_client = boto3.client('sqs', os.getenv('AWS_DEPLOYMENT_REGION', 'us-west-2'))


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
        rds = RDS(os.getenv('DB_HOST'), os.getenv('DB_USER'), os.getenv('DB_NAME'), os.getenv('DB_PASSWORD'))
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
    except rds_client.exceptions.DBInstanceNotFoundFault:
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
            {'Key': 'wma:organization', 'Value': 'IOW'},
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
            {'Key': 'Name', 'Value': f"NWISWEB-CAPTURE-RDS-AURORA-{STAGE}"},
            {'Key': 'wma:applicationId', 'Value': 'NWISWEB-CAPTURE'},
            {'Key': 'wma:contact', 'Value': 'tbd'},
            {'Key': 'wma:costCenter', 'Value': 'tbd'},
            {'Key': 'wma:criticality', 'Value': 'tbd'},
            {'Key': 'wma:environment', 'Value': 'qa'},
            {'Key': 'wma:operationalHours', 'Value': 'tbd'},
            {'Key': 'wma:organization', 'Value': 'IOW'},
            {'Key': 'wma:role', 'Value': 'database'},
            {'Key': 'wma:system', 'Value': 'NWIS'},
            {'Key': 'wma:subSystem', 'Value': 'NWISWeb-Capture'},
            {'Key': 'taggingVersion', 'Value': '0.0.1'}
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


# TODO REMOVE


def copy_s3(event, context):
    logger.info(event)
    """
    Copy files from the 'reference' bucket to the trigger bucket to simulate
    a full run.
    :param event:
    :param context:
    :return:
    """
    s3_client = boto3.client('s3', os.getenv('AWS_DEPLOYMENT_REGION'))
    SRC_BUCKET = 'iow-retriever-capture-test'
    DEST_BUCKET = 'iow-retriever-capture-qa'
    resp = s3_client.list_objects_v2(Bucket=SRC_BUCKET)
    keys = []
    for obj in resp['Contents']:
        keys.append(obj['Key'])

    s3_resource = boto3.resource('s3')
    for key in keys:
        copy_source = {
            'Bucket': SRC_BUCKET,
            'Key': key
        }
        logger.info(f"key = {key}")
        bucket = s3_resource.Bucket(DEST_BUCKET)
        bucket.copy(copy_source, key)
