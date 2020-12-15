import datetime
import json
import os

import boto3

from src.db_resize_handler import disable_trigger, enable_trigger
from src.rds import RDS
from src.utils import enable_lambda_trigger, describe_db_clusters, start_db_cluster, disable_lambda_trigger, \
    stop_db_cluster, \
    purge_queue, stop_observations_db_instance, DEFAULT_DB_INSTANCE_CLASS
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

log_level = os.getenv('LOG_LEVEL', logging.ERROR)
logger = logging.getLogger(__name__)
logger.setLevel(log_level)

STAGE = os.getenv('STAGE', 'TEST')

cloudwatch_client = boto3.client('cloudwatch', os.getenv('AWS_DEPLOYMENT_REGION', 'us-west-2'))
secrets_client = boto3.client('secretsmanager', os.getenv('AWS_DEPLOYMENT_REGION', 'us-west-2'))
rds_client = boto3.client('rds', os.getenv('AWS_DEPLOYMENT_REGION', 'us-west-2'))
sqs_client = boto3.client('sqs', os.getenv('AWS_DEPLOYMENT_REGION', 'us-west-2'))


def _get_etl_start():
    yesterday = datetime.datetime.now() - datetime.timedelta(1)
    my_month = str(yesterday.month)
    if len(my_month) == 1:
        my_month = f"0{my_month}"
    my_day = str(yesterday.day)
    if len(my_day) == 1:
        my_day = f"0{my_day}"
    my_etl_start = f"{yesterday.year}-{my_month}-{my_day}"
    return my_etl_start


etl_start = _get_etl_start()
OBSERVATIONS_ETL_IN_PROGRESS_SQL = \
    "select count(1) from batch_job_execution where status not in ('COMPLETED', 'FAILED') and last_updated > %s"

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
        logger.info(f"ALARM!")
        if get_flow_rate() == 25:
            logger.info(f"Adjusting flow rate to 15")
            adjust_flow_rate(15)
        elif get_flow_rate() == 15:
            logger.info(f"Adjusting flow rate to 0")
            adjust_flow_rate(0)
    else:
        """
        If we are not in a state of alarm (i.e. OK or INSUFFICIENT_DATA) then it is okay
        to enable the trigger if the db is up and running (status == available)
        
        However, we know there is a backlog to work through so we need to force the db to maximum size
        by issuing a fake high-cpu alarm.
        """
        logger.info(f"The circuit breaker has calmed down.")
        if get_flow_rate() == 0:
            logger.info(f"Adjusting flow rate up to 15.")
            adjust_flow_rate(15)
        elif get_flow_rate() == 15:
            logger.info(f"Adjusting flow rate up to 25.")
            adjust_flow_rate(25)


def adjust_flow_rate(new_flow_rate):
    client = boto3.client('lambda', os.getenv('AWS_DEPLOYMENT_REGION'))
    client.put_function_concurrency(
        FunctionName=TRIGGER[STAGE][0],
        ReservedConcurrentExecutions=new_flow_rate
    )


def get_flow_rate():
    client = boto3.client('lambda', os.getenv('AWS_DEPLOYMENT_REGION'))
    response = client.get_function_concurrency(
        FunctionName=TRIGGER[STAGE][0]
    )
    flow_rate = response['ReservedConcurrentExecutions']
    return flow_rate


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
    """
    Purging the queue was originally done for expense control on the test and QA tiers in the early days, but now that
    development is further along, we'd like to see these tiers coping with a more production-like backlog.
    """
    # purge_queue(queue_name)
    cluster_identifiers = describe_db_clusters("start")
    started = False
    for cluster_identifier in cluster_identifiers:
        if cluster_identifier == db:
            start_db_cluster(db)
            started = True
            enable_lambda_trigger(triggers)
    return started


def _stop_db(db, triggers):
    cluster_identifiers = describe_db_clusters("stop")
    stopped = False
    disable_lambda_trigger(triggers)
    for cluster_identifier in cluster_identifiers:
        if cluster_identifier == db:
            stop_db_cluster(db)
            stopped = True
    return stopped


def troubleshoot(event, context):
    if event['action'].lower() == 'start_capture_db':
        cluster_identifiers = describe_db_clusters("start")
        for cluster_identifier in cluster_identifiers:
            if cluster_identifier == DB[STAGE]:
                start_db_cluster(DB[STAGE])
    elif event['action'].lower() == 'stop_capture_db':
        cluster_identifiers = describe_db_clusters("stop")
        for cluster_identifier in cluster_identifiers:
            if cluster_identifier == DB[STAGE]:
                stop_db_cluster(DB[STAGE])
    elif event['action'].lower() == 'make_kms_key':
        _make_kms_key(event)
    elif event['action'].lower() == 'change_secret_kms_key':
        _change_secret_kms_key(event)
    elif event['action'].lower() == 'change_kms_key_policy':
        _change_kms_key_policy(event)
    # TODO remove
    elif event['action'].lower() == 'delete_stack':
        stack = event['stack']
        client = boto3.client('cloudformation', "us-west-2")
        response = client.delete_stack(
            StackName=stack,
        )
    elif event['action'].lower() == 'purge_queues':
        purge_queue([CAPTURE_TRIGGER_QUEUE, ERROR_QUEUE])
    elif event['action'].lower() == 'create_access_point':
        _make_efs_access_point(event)
    elif event['action'].lower() == 'create_fargate_security_group':
        _make_fargate_security_group(event)
    elif event['action'].lower() == 'delete_fargate_security_group':
        # When you need to delete a security group, modify the code
        # here and specify the group id.  Don't check into master
        client = boto3.client('ec2', os.getenv('AWS_DEPLOYMENT_REGION'))
        client.delete_security_group(GroupId='sg-xxxxxxxxxxxxxxxxx')
    elif event['action'].lower() == 'delete_access_point':
        # When you need to delete an efs access point, modify the code
        # here and specify the access point id.  Don't check into master
        client = boto3.client('efs', os.getenv('AWS_DEPLOYMENT_REGION'))
        client.delete_access_point(AccessPointId='fsap-xxxxxxxxxxxxxxxxx')
    else:
        raise Exception(f"invalid action")


def _validate():
    if os.environ['STAGE'] != "QA":
        raise Exception("This lambda is currently only supported on the QA tier")


"""
Occasional use functions.  These are used rarely to set up new long-lived resources.
"""


def _change_kms_key_policy(event):
    key_id = event['key_id']
    account_id = os.environ['ACCOUNT_ID']
    policy = {
        "Version": "2012-10-17",
        "Id": "key-consolepolicy-3",
        "Statement": [
            {
                "Sid": "Enable IAM User Permissions",
                "Effect": "Allow",
                "Principal": {
                    "AWS": f"arn:aws:iam::{account_id}:root"
                },
                "Action": "kms:*",
                "Resource": "*"
            },
            {
                "Sid": "Allow access for Key Administrators",
                "Effect": "Allow",
                "Principal": {
                    "AWS": f"arn:aws:iam::{account_id}:role/Ec2-Role"
                },
                "Action": [
                    "kms:Create*",
                    "kms:Describe*",
                    "kms:Enable*",
                    "kms:List*",
                    "kms:Put*",
                    "kms:Update*",
                    "kms:Revoke*",
                    "kms:Disable*",
                    "kms:Get*",
                    "kms:Delete*",
                    "kms:TagResource",
                    "kms:UntagResource",
                    "kms:ScheduleKeyDeletion",
                    "kms:CancelKeyDeletion"
                ],
                "Resource": "*"
            },
            {
                "Sid": "Allow use of the key",
                "Effect": "Allow",
                "Principal": {
                    "AWS": [
                        f"arn:aws:iam::{account_id}:role/adfs-developers",
                        f"arn:aws:iam::{account_id}:role/Ec2-Role"
                    ]
                },
                "Action": [
                    "kms:Encrypt",
                    "kms:Decrypt",
                    "kms:ReEncrypt*",
                    "kms:GenerateDataKey*",
                    "kms:DescribeKey"
                ],
                "Resource": "*"
            },
            {
                "Sid": "Allow attachment of persistent resources",
                "Effect": "Allow",
                "Principal": {
                    "AWS": f"arn:aws:iam::{account_id}:role/Ec2-Role"
                },
                "Action": [
                    "kms:CreateGrant",
                    "kms:ListGrants",
                    "kms:RevokeGrant"
                ],
                "Resource": "*",
                "Condition": {
                    "Bool": {
                        "kms:GrantIsForAWSResource": "true"
                    }
                }
            }
        ]
    }
    policy = json.dumps(policy)
    client = boto3.client('kms', os.getenv('AWS_DEPLOYMENT_REGION'))
    response = client.put_key_policy(
        KeyId=key_id,
        PolicyName='default',
        Policy=policy
    )
    logger.info(response)


def _change_secret_kms_key(event):
    new_kms_key = event['new_kms_key']
    secret_id = event['secret_id']
    response = secrets_client.update_secret(
        SecretId=secret_id,
        KmsKeyId=new_kms_key
    )


def _make_efs_access_point(event):
    client = boto3.client('efs', os.getenv('AWS_DEPLOYMENT_REGION'))
    file_system_id = event['file_system_id']
    response = client.create_access_point(
        ClientToken='iow-geoserver-test',
        Tags=[
            {
                'Key': 'wma:organization',
                'Value': 'IOW'
            },
            {
                'Key': 'Name',
                'Value': 'iow-geoserver-test'
            }

        ],
        FileSystemId=file_system_id,
        PosixUser={
            'Uid': 1001,
            'Gid': 1001,
            'SecondaryGids': []
        },
        RootDirectory={
            'Path': '/data',
            'CreationInfo': {
                'OwnerUid': 1001,
                'OwnerGid': 1001,
                'Permissions': '0777'
            }
        }
    )
    logger.info(f"Access point created: {response}")


def _make_fargate_security_group(event):
    client = boto3.client('ec2', os.getenv('AWS_DEPLOYMENT_REGION'))
    description = event['description']
    group_name = event['group_name']
    vpc_id = event['vpc_id']
    response = client.create_security_group(
        Description=description,
        GroupName=group_name,
        VpcId=vpc_id,
    )
    security_group_id = response['GroupId']
    ec2 = boto3.resource('ec2', os.getenv('AWS_DEPLOYMENT_REGION'))
    security_group = ec2.SecurityGroup(security_group_id)
    security_group.authorize_ingress(IpProtocol="tcp", CidrIp="0.0.0.0/0", FromPort=2049, ToPort=2049)
    security_group.authorize_egress(
        IpPermissions=[
            {
                'IpProtocol': 'tcp',
                'FromPort': 0,
                'ToPort': 65535,
                'IpRanges': [
                    {'CidrIp': '0.0.0.0/0'}
                ]
            }
        ]
    )
    logger.info(f"security_group {security_group_id} created")


def _make_kms_key(event):
    key_project = event['key_project'].upper()
    key_stage = event['key_stage'].upper()
    client = boto3.client('kms', os.getenv('AWS_DEPLOYMENT_REGION'))
    try:
        response = client.create_key(
            Description=f'IOW {key_project} {key_stage} key',
            KeyUsage='ENCRYPT_DECRYPT',
            Origin='AWS_KMS',
            Tags=[
                {
                    'TagKey': 'wma:organization',
                    'TagValue': 'IOW'
                },
            ]
        )
    except client.exceptions.ClientError:
        logger.error(f"Couldn't create KMS key, probably already exists")

    alias = f"alias/IOW-{key_project}-{key_stage}"
    client.create_alias(
        AliasName=alias,
        TargetKeyId=response['KeyMetadata']['KeyId']
    )

