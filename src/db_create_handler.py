import datetime
import json
import os

import boto3
from src.rds import RDS
from src.utils import enable_lambda_trigger, disable_lambda_trigger, \
    DEFAULT_DB_INSTANCE_CLASS, CAPTURE_INSTANCE_TAGS, OBSERVATION_INSTANCE_TAGS
import logging

STAGES = ['TEST', 'QA', 'PROD-EXTERNAL']
DB = {
    "TEST": 'nwcapture-test',
    "QA": 'nwcapture-qa',
    "PROD-EXTERNAL": 'nwcapture-prod-external'
}

OBSERVATIONS_DB = {
    "TEST": 'observations-test',
    "QA": 'observations-qa',
    "PROD-EXTERNAL": 'observations-prod-external'
}

SQS = {
    "TEST": ['aqts-capture-trigger-queue-TEST', 'aqts-capture-error-queue-TEST'],
    "QA": ['aqts-capture-trigger-queue-QA', 'aqts-capture-error-queue-QA'],
    "PROD-EXTERNAL": ['aqts-capture-trigger-queue-PROD-EXTERNAL']
}

TRIGGER = {
    "TEST": ['aqts-capture-trigger-TEST-aqtsCaptureTrigger'],
    "QA": ['aqts-capture-trigger-QA-aqtsCaptureTrigger'],
    "PROD-EXTERNAL": ['aqts-capture-trigger-PROD-EXTERNAL-aqtsCaptureTrigger']
}

STAGE = os.getenv('STAGE', 'TEST')
CAPTURE_TRIGGER_QUEUE = f"aqts-capture-trigger-queue-{STAGE}"
ERROR_QUEUE = f"aqts-capture-error-queue-{STAGE}"

DEFAULT_DB_CLUSTER_IDENTIFIER = f"nwcapture-{STAGE.lower()}"
DEFAULT_DB_INSTANCE_IDENTIFIER = f"{DEFAULT_DB_CLUSTER_IDENTIFIER}-instance1"
ENGINE = 'aurora-postgresql'
NWCAPTURE_REAL = f"NWCAPTURE-DB-{STAGE}"
OBSERVATION_REAL = f"WQP-EXTERNAL-{STAGE}"

log_level = os.getenv('LOG_LEVEL', logging.ERROR)
logger = logging.getLogger(__name__)
logger.setLevel(log_level)

STAGE = os.getenv('STAGE', 'TEST')

cloudwatch_client = boto3.client('cloudwatch', os.getenv('AWS_DEPLOYMENT_REGION', 'us-west-2'))
secrets_client = boto3.client('secretsmanager', os.getenv('AWS_DEPLOYMENT_REGION', 'us-west-2'))
rds_client = boto3.client('rds', os.getenv('AWS_DEPLOYMENT_REGION', 'us-west-2'))
sqs_client = boto3.client('sqs', os.getenv('AWS_DEPLOYMENT_REGION', 'us-west-2'))


def _get_date_string(my_datetime):
    month = str(my_datetime.month)
    if len(month) == 1:
        month = f"0{month}"
    day = str(my_datetime.day)
    if len(day) == 1:
        day = f"0{day}"
    return f"{my_datetime.year}-{month}-{day}"


def _get_etl_start():
    two_days_ago = datetime.datetime.now() - datetime.timedelta(2)
    return _get_date_string(two_days_ago)


etl_start = _get_etl_start()
OBSERVATIONS_ETL_IN_PROGRESS_SQL = \
    "select count(1) from batch_job_execution where status not in ('COMPLETED', 'FAILED') and start_time > %s"

"""
DB create and delete functions
"""


def modify_postgres_password(event, context):
    _validate()
    original = secrets_client.get_secret_value(
        SecretId=NWCAPTURE_REAL,
    )
    secret_string = json.loads(original['SecretString'])
    postgres_password = secret_string['POSTGRES_PASSWORD']

    response = rds_client.describe_db_clusters()
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
    disable_lambda_trigger(TRIGGER[os.environ['STAGE']])


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
        Tags=CAPTURE_INSTANCE_TAGS
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
    my_snapshot_identifier = get_snapshot_identifier()
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
            {'Key': 'wma:project_id', 'Value': 'aqtscapture'},
            {'Key': 'wma:application_id', 'Value': 'NWISWEB-CAPTURE'},
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
    rds = RDS(db_host, 'postgres', db_name, postgres_password)
    sql = "alter user capture_owner with password %s"
    rds.alter_permissions(sql, (schema_owner_password,))

    queue_info = sqs_client.get_queue_url(QueueName=CAPTURE_TRIGGER_QUEUE)
    sqs_client.purge_queue(QueueUrl=queue_info['QueueUrl'])
    queue_info = sqs_client.get_queue_url(QueueName=ERROR_QUEUE)
    sqs_client.purge_queue(QueueUrl=queue_info['QueueUrl'])

    enable_lambda_trigger(TRIGGER[os.environ['STAGE']])


def get_snapshot_identifier():
    two_days_ago = datetime.datetime.now() - datetime.timedelta(2)
    date_str = _get_date_string(two_days_ago)
    return f"rds:nwcapture-prod-external-{date_str}-10-08"


def create_observation_db(event, context):
    _validate()
    logger.info(f"event {event}")

    original = secrets_client.get_secret_value(
        SecretId=OBSERVATION_REAL
    )
    secret_string = json.loads(original['SecretString'])
    subgroup_name = str(secret_string['DB_SUBGROUP_NAME'])
    vpc_security_group_id = str(secret_string['VPC_SECURITY_GROUP_ID'])
    my_snapshot_identifier = _get_observation_snapshot_identifier()
    logger.info(f"my snapshot identified {my_snapshot_identifier}")

    response = rds_client.restore_db_instance_from_db_snapshot(
        DBInstanceIdentifier=f"observations-{STAGE.lower()}",
        DBSnapshotIdentifier=my_snapshot_identifier,
        DBInstanceClass='db.r5.2xlarge',
        Port=5432,
        DBSubnetGroupName=subgroup_name,
        Iops=0,
        MultiAZ=False,
        Engine='postgres',
        VpcSecurityGroupIds=[
            vpc_security_group_id,
        ],
        Tags=OBSERVATION_INSTANCE_TAGS

    )


def delete_observation_db(event, context):
    _validate()
    try:
        rds_client.delete_db_instance(
            DBInstanceIdentifier=f"observations-{STAGE.lower()}",
            SkipFinalSnapshot=True
        )
    except rds_client.exceptions.DBInstanceNotFoundFault:
        logger.info("observations db was already deleted, skipping")


def modify_observation_postgres_password(event, context):
    _validate()
    logger.info(event)
    original = secrets_client.get_secret_value(
        SecretId=OBSERVATION_REAL,
    )
    secret_string = json.loads(original['SecretString'])
    postgres_password = secret_string['POSTGRES_PASSWORD']

    rds_client.modify_db_instance(
        DBInstanceIdentifier=f"observations-{STAGE.lower()}",
        ApplyImmediately=True,
        MasterUserPassword=postgres_password
    )


def modify_observation_passwords(event, context):
    _validate()
    logger.info(event)
    original = secrets_client.get_secret_value(
        SecretId=OBSERVATION_REAL,
    )
    secret_string = json.loads(original['SecretString'])
    db_host = secret_string['DATABASE_ADDRESS']
    db_name = secret_string['DATABASE_NAME']
    postgres_password = secret_string['POSTGRES_PASSWORD']

    rds = RDS(db_host, 'postgres', db_name, postgres_password)
    logger.info("got rds ok")
    pwd = secret_string['DB_OWNER_PASSWORD']
    sql = "alter user wqp_core with password %s"
    rds.alter_permissions(sql, (pwd,))
    logger.info("changed wqp_core password")

    pwd = secret_string['WQP_READ_ONLY_PASSWORD']
    sql = "alter user wqp_user with password %s"
    rds.alter_permissions(sql, (pwd,))
    logger.info("changed wqp_user password")

    pwd = secret_string['ARS_SCHEMA_OWNER_PASSWORD']
    sql = "alter user ars_owner with password %s"
    rds.alter_permissions(sql, (pwd,))
    logger.info("changed ars_owner password")

    pwd = secret_string['NWIS_SCHEMA_OWNER_PASSWORD']
    sql = "alter user nwis_ws_star_owner with password %s"
    rds.alter_permissions(sql, (pwd,))
    logger.info("changed nwis_ws_star_owner password")

    pwd = secret_string['EPA_SCHEMA_OWNER_PASSWORD']
    sql = "alter user epa_owner with password %s"
    rds.alter_permissions(sql, (pwd,))
    logger.info("changed epa_owner password")

    pwd = secret_string['WDFN_DB_READ_ONLY_PASSWORD']
    sql = "alter user wdfn_user with password %s"
    rds.alter_permissions(sql, (pwd,))
    logger.info("changed wdfn_user password")
    return True


def _get_observation_snapshot_identifier():
    # In the dev account we don't have a list of automatic backups
    # See README
    if os.getenv('LAST_OB_DB_SNAPSHOT') is not None:
        return os.getenv('LAST_OB_DB_SNAPSHOT')
    two_days_ago = datetime.datetime.now() - datetime.timedelta(2)
    date_str = _get_date_string(two_days_ago)
    response = rds_client.describe_db_snapshots(
        DBInstanceIdentifier='observations-db-legacy-production-external',
        SnapshotType='automated')
    for snapshot in response['DBSnapshots']:
        if date_str in snapshot['DBSnapshotIdentifier'] \
                and "observations-db-legacy-production-external" in snapshot['DBSnapshotIdentifier']:
            return snapshot['DBSnapshotIdentifier']
    raise Exception(f"DB Snapshot not found for date_str {date_str} {response['DBSnapshots']}")


def _validate():
    if os.getenv('CAN_DELETE_DB') is None or os.getenv('CAN_DELETE_DB') == 'false':
        raise Exception("Cannot create or delete the db on this tier")
