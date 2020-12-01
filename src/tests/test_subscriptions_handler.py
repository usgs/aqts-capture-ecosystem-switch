import json
import os
from unittest import TestCase, mock

from src import subscriptions_handler

subscriptions_response = {
    "Subscriptions": [
        {'SubscriptionArn': 'PendingConfirmation',
         'Owner': '123456789', 'Protocol': 'email', 'Endpoint': ' ssssss@contractor.usgs.gov',
         'TopicArn': 'my_test_topic'},
        {'SubscriptionArn': 'my_test_topic:082',
         'Owner': '123456789', 'Protocol': 'email', 'Endpoint': 'kkkkkk@contractor.usgs.gov',
         'TopicArn': 'my_test_topic'},
        {'SubscriptionArn': 'PendingConfirmation',
         'Owner': '123456789', 'Protocol': 'email', 'Endpoint': 'abc@abc.xyz.com',
         'TopicArn': 'my_test_topic'},
        {'SubscriptionArn': 'PendingConfirmation', 'Owner': '123456789', 'Protocol': 'email',
         'Endpoint': 'abc@abc.xyz.com',
         'TopicArn': 'my_test_topic'}
    ],
    'ResponseMetadata': {'RequestId': 'aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa',
                         'HTTPStatusCode': 200,
                         'HTTPHeaders': {'x-amzn-requestid': '',
                                         'content-type': 'text/xml', 'content-length': '1778',
                                         'date': 'Mon, 30 Nov 2020 23:16:00 GMT'}, 'RetryAttempts': 0}
}


class TestSubscriptionsHandler(TestCase):

    def setUp(self):
        pass

    @mock.patch('src.subscriptions_handler.secrets_client', autospec=True)
    @mock.patch('src.subscriptions_handler._process_subscriptions')
    @mock.patch('src.utils.boto3.client', autospec=True)
    def test_manage_subscriptions(self, mock_boto, mock_process, mock_secrets):
        mock_client = mock.Mock()
        mock_client.list_subscriptions_by_topic.return_value = subscriptions_response
        mock_boto.return_value = mock_client

        secret_string = json.dumps({
            "TERMINAL_ERRORS_LIST": "aaaaaa@usgs.gov",
            "WARNINGS_LIST": "bbbbbb@usgs.gov"
        })
        mock_secrets.get_secret_value.return_value = {
            "SecretString": secret_string
        }
        os.environ['TERMINAL_ERRORS_TOPIC_ARN'] = 'terminal_errors_arn'
        os.environ['WARNINGS_TOPIC_ARN'] = 'warnings_arn'
        mock_process.return_value = None
        subscriptions_handler.manage_subscriptions({}, {})
        self.assertEqual(mock_process.call_count, 2)

    @mock.patch('src.subscriptions_handler._unsubscribe_sns')
    @mock.patch('src.subscriptions_handler._subscribe_sns')
    def test_process_subscriptions(self, mock_subscribe, mock_unsubscribe):
        mock_subscribe.return_value = None
        mock_unsubscribe.return_value = None
        response = subscriptions_response

        subscribe_emails = ["aaaaaa@usgs.gov"]
        topic_arn = "my_test_topic"
        subscriptions_handler._process_subscriptions(response, subscribe_emails, topic_arn)
        mock_subscribe.assert_called_once_with({'topic_arn': 'my_test_topic', 'endpoint': 'aaaaaa@usgs.gov'})
        # should be 4 but three are 'PendingConfirmation' and AWS needs to skip them because no real ARN
        self.assertEqual(mock_unsubscribe.call_count, 1)

    @mock.patch('src.utils.boto3.client', autospec=True)
    def test_subscribe_sns(self, mock_boto):
        mock_client = mock.Mock()
        os.environ['ACCOUNT_ID'] = 'my_account_id'
        mock_client.subscribe.return_value = None
        mock_boto.return_value = mock_client

        subscriptions_handler._subscribe_sns(
            {
                "topic_arn": "my_topic_arn",
                "endpoint": "my_endpoint"
            }
        )
        mock_client.subscribe.assert_called_once_with(
            TopicArn='my_topic_arn', Protocol='email', Endpoint='my_endpoint', ReturnSubscriptionArn=True)

    @mock.patch('src.utils.boto3.client', autospec=True)
    def test_unsubscribe_sns(self, mock_boto):
        mock_client = mock.Mock()
        os.environ['ACCOUNT_ID'] = 'my_account_id'
        mock_client.unsubscribe.return_value = None
        mock_boto.return_value = mock_client

        subscriptions_handler._unsubscribe_sns('foo')
        mock_client.unsubscribe.assert_called_once_with(SubscriptionArn='foo')
