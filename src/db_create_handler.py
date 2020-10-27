import datetime
import json
import os

import boto3
from src.rds import RDS
from src.utils import enable_lambda_trigger, describe_db_clusters, start_db_cluster, disable_lambda_trigger, \
    stop_db_cluster, \
    purge_queue, stop_observations_db_instance, DEFAULT_DB_INSTANCE_CLASS
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
DB create and delete functions
"""


def modify_postgres_password(event, context):
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

    enable_lambda_trigger(TRIGGER[os.environ['STAGE']])


def get_snapshot_identifier():
    two_days_ago = datetime.datetime.now() - datetime.timedelta(2)
    month = str(two_days_ago.month)
    if len(month) == 1:
        month = f"0{month}"
    day = str(two_days_ago.day)
    if len(day) == 1:
        day = f"0{day}"
    return f"rds:nwcapture-prod-external-{two_days_ago.year}-{month}-{day}-10-08"


def create_observation_db(event, context):
    logger.info(event)

    original = secrets_client.get_secret_value(
        SecretId='WQP-EXTERNAL-QA'
    )
    secret_string = json.loads(original['SecretString'])
    kms_key = str(secret_string['KMS_KEY_ID'])
    subgroup_name = str(secret_string['DB_SUBGROUP_NAME'])
    vpc_security_group_id = str(secret_string['VPC_SECURITY_GROUP_ID'])
    if not kms_key or not subgroup_name or not vpc_security_group_id:
        raise Exception(f"Missing db configuration data {secret_string}")
    my_snapshot_identifier = _get_observation_snapshot_identifier()
    if event is not None:
        if event.get("db_config") is not None and event['db_config'].get('snapshot_identifier') is not None:
            my_snapshot_identifier = event['db_config'].get("snapshot_identifier")

    """
    We need to use the copied snapshot, not the original, because the copied snapshot
    has the correct kms key.
    """
    response = rds_client.restore_db_instance_from_db_snapshot(
        DBInstanceIdentifier='observations-qa-exp',
        DBSnapshotIdentifier=f"observationSnapshot{STAGE}Temp",
        DBInstanceClass='db.r5.2xlarge',
        Port=5432,
        DBSubnetGroupName=subgroup_name,
        MultiAZ=False,
        Engine='postgres',
        VpcSecurityGroupIds=[
            vpc_security_group_id,
        ],
        Tags=[
            {'Key': 'Name', 'Value': f"OBSERVATIONS-RDS-{STAGE}-EXP"},
            {'Key': 'wma:applicationId', 'Value': 'OBSERVATIONS'},
            {'Key': 'wma:contact', 'Value': 'tbd'},
            {'Key': 'wma:costCenter', 'Value': 'tbd'},
            {'Key': 'wma:criticality', 'Value': 'tbd'},
            {'Key': 'wma:environment', 'Value': f"{STAGE.lower()}"},
            {'Key': 'wma:operationalHours', 'Value': 'tbd'},
            {'Key': 'wma:organization', 'Value': 'IOW'},
            {'Key': 'wma:role', 'Value': 'database'},
            {'Key': 'taggingVersion', 'Value': '0.0.1'}
        ]

    )
    logger.info(response)


def copy_observation_db_snapshot(event, context):
    logger.info(event)

    original = secrets_client.get_secret_value(
        SecretId='WQP-EXTERNAL-QA'
    )
    secret_string = json.loads(original['SecretString'])
    kms_key = str(secret_string['KMS_KEY_ID'])
    subgroup_name = str(secret_string['DB_SUBGROUP_NAME'])
    vpc_security_group_id = str(secret_string['VPC_SECURITY_GROUP_ID'])
    if not kms_key or not subgroup_name or not vpc_security_group_id:
        raise Exception(f"Missing db configuration data {secret_string}")
    my_snapshot_identifier = _get_observation_snapshot_identifier()
    if event is not None:
        if event.get("db_config") is not None and event['db_config'].get('snapshot_identifier') is not None:
            my_snapshot_identifier = event['db_config'].get("snapshot_identifier")
    response = rds_client.copy_db_snapshot(
        SourceDBSnapshotIdentifier=my_snapshot_identifier,
        TargetDBSnapshotIdentifier=f"observationSnapshot{STAGE}Temp",
        KmsKeyId=kms_key
    )
    logger.info(response)


def delete_observation_db(event, context):

    try:
        rds_client.delete_db_instance(
            DBInstanceIdentifier='observations-qa-exp'
        )
    except rds_client.exceptions.DBInstanceNotFoundFault:
        logger.info("observations db was already deleted, skipping")

    rds_client.delete_db_snapshot(
        DBSnapshotIdentifier=f"observationSnapshot{STAGE}Temp"
    )


def modify_observation_postgres_password(event, context):
    logger.info("enter modify postgres password")
    logger.info(event)
    original = secrets_client.get_secret_value(
        SecretId=OBSERVATION_REAL,
    )
    secret_string = json.loads(original['SecretString'])
    postgres_password = secret_string['POSTGRES_PASSWORD']

    rds_client.modify_db_instance(
        DBInstanceIdentifier='observations-qa-exp',
        ApplyImmediately=True,
        MasterUserPassword=postgres_password
    )


def modify_observation_passwords(event, context):
    logger.info(event)
    original = secrets_client.get_secret_value(
        SecretId=OBSERVATION_REAL,
    )
    secret_string = json.loads(original['SecretString'])
    db_host = secret_string['DATABASE_ADDRESS']
    db_name = secret_string['DATABASE_NAME']
    postgres_password = secret_string['POSTGRES_PASSWORD']

    # TODO remove this when using real qa db
    db_host = db_host.replace("observations-qa", "observations-qa-exp")

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
    return "rds:observations-prod-external-2-2020-10-26-07-01"
