import json
import logging
import os

import boto3

STAGES = ['TEST', 'QA', 'PROD-EXTERNAL']

STAGE = os.getenv('STAGE', 'TEST')

log_level = os.getenv('LOG_LEVEL', logging.ERROR)
logger = logging.getLogger(__name__)
logger.setLevel(log_level)

secrets_client = boto3.client('secretsmanager', os.getenv('AWS_DEPLOYMENT_REGION', 'us-west-2'))


def manage_subscriptions(event, context):
    """
    Obtain a list of subscribers for terminal errors and a list of subscribers for warnings from the secrets
    manager.  Process these lists to synchronize them with the subscriptions AWS knows about.
    """
    original = secrets_client.get_secret_value(
        SecretId="AQTS-CAPTURE-MAILING-LISTS",
    )
    client = boto3.client('sns', os.getenv('AWS_DEPLOYMENT_REGION', 'us-west-2'))

    secret_string = json.loads(original['SecretString'])

    topic_arn_errors = os.environ['TERMINAL_ERRORS_TOPIC_ARN']
    response = client.list_subscriptions_by_topic(
        TopicArn=topic_arn_errors
    )
    logger.info(f"terminal error subscriptions: {response}")
    subscribe_emails = secret_string['TERMINAL_ERRORS_LIST'].split(",")
    logger.info(f"\nterminal error subscrbe_emails {subscribe_emails}")
    _process_subscriptions(response, subscribe_emails, topic_arn_errors)

    topic_arn_warnings = os.environ['WARNINGS_TOPIC_ARN']
    response = client.list_subscriptions_by_topic(
        TopicArn=topic_arn_warnings
    )
    logger.info(f"warning subscriptions: {response}")
    subscribe_emails = secret_string['WARNINGS_LIST'].split(",")
    logger.info(f"\nwarning subscribe emails: {subscribe_emails}")
    _process_subscriptions(response, subscribe_emails, topic_arn_warnings)


def _process_subscriptions(response, subscribe_emails, topic_arn):
    """
    If there is an email in the emails list obtained from the secrets manager that does not have a subscription,
    subscribe that email.  If there is a subscription in the aws list of subscriptions that does not have an
    associated email in the list of emails in the secrets manager, unsubscribe that email.
    """
    subscriptions_str = json.dumps(response['Subscriptions'])
    for subscribe_email in subscribe_emails:
        if subscribe_email in subscriptions_str:
            logger.info(f"\nthe user {subscribe_email} is already subscribed")
        else:
            logger.info(f"\nnew subscription for {subscribe_email}")
            _subscribe_sns({'topic_arn': topic_arn, 'endpoint': subscribe_email})
    subscriptions = response['Subscriptions']
    for subscription in subscriptions:
        if subscription['Endpoint'] not in subscribe_emails and subscription['SubscriptionArn'] != "PendingConfirmation":
            logger.info(f"\nunsubscribe {subscription['Endpoint']} because no longer on subscribe list")
            logger.info(f"\nusing the SubscriptionArn {subscription}")
            _unsubscribe_sns(subscription['SubscriptionArn'])


def _subscribe_sns(event):
    client = boto3.client('sns', os.getenv('AWS_DEPLOYMENT_REGION', 'us-west-2'))
    topic_arn = event['topic_arn']
    endpoint = event['endpoint']
    response = client.subscribe(
        TopicArn=topic_arn,
        Protocol='email',
        Endpoint=endpoint,
        ReturnSubscriptionArn=True
    )


def _unsubscribe_sns(subscription_arn):
    client = boto3.client('sns', os.getenv('AWS_DEPLOYMENT_REGION', 'us-west-2'))
    response = client.unsubscribe(
        SubscriptionArn=subscription_arn
    )
