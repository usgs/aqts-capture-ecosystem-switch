import os
from unittest import TestCase, mock

from src import handler
from src.handler import TRIGGER, STAGES, DB
from src.utils import enable_triggers, disable_triggers, purge_queue, stop_db_cluster, start_db_cluster, \
    describe_db_clusters


class TestUtils(TestCase):
    queue_url = 'https://sqs.us-south-10.amazonaws.com/887501/some-queue-name'
    sns_arn = 'arn:aws:sns:us-south-23:5746521541:fake-notification'
    region = 'us-south-10'
    max_retries = 6
    mock_env_vars = {
        'AWS_DEPLOYMENT_REGION': region,
        'MAX_RETRIES': str(max_retries)
    }
    mock_db_cluster_identifiers = {'nwcapture-test', 'nwcapture-qa'}
    mock_db_clusters = {
        'Marker': 'string',
        'DBClusters': [
            {
                'DBClusterIdentifier': 'string',
                'Status': 'string',
            },
        ]
    }

    mock_event_source_mapping = {
        'NextMarker': 'string',
        'EventSourceMappings': [
            {
                'UUID': 'string',
            },
        ]
    }

    def setUp(self):
        self.initial_execution_arn = 'arn:aws:states:us-south-10:98877654311:blah:a17h83j-p84321'
        self.state_machine_start_input = {
            'Record': {'eventVersion': '2.1', 'eventSource': 'aws:s3'}
        }
        self.initial_event = {'executionArn': self.initial_execution_arn, 'startInput': self.state_machine_start_input}
        self.context = {'element': 'lithium'}

    @mock.patch.dict('src.utils.os.environ', mock_env_vars)
    @mock.patch('src.utils.boto3', autospec=True)
    def test_start_capture_db_nothing_to_start(self, mock_boto):
        for stage in STAGES:
            os.environ['STAGE'] = stage
            result = handler.start_capture_db(self.initial_event, self.context)
            assert result['statusCode'] == 200
            assert result['message'] == f"Started the {stage} db: False"

        os.environ['STAGE'] = 'UNKNOWN'
        with self.assertRaises(Exception) as context:
            handler.start_capture_db(self.initial_event, self.context)

    @mock.patch('src.utils.boto3.client', autospec=True)
    def test_enable_triggers(self, mock_boto):
        client = mock.Mock()
        mock_boto.return_value = client
        client.list_event_source_mappings.return_value = self.mock_event_source_mapping
        client.get_event_source_mapping.return_value = {"State": "Disabled"}
        result = enable_triggers(["my_function_name"])
        assert result is True
        mock_boto.assert_called_with("lambda", "us-west-2")
        client.list_event_source_mappings.assert_called_with(FunctionName='my_function_name')
        client.update_event_source_mapping.assert_called_with(UUID='string', Enabled=True)

    @mock.patch('src.utils.boto3.client', autospec=True)
    def test_enable_triggers_already_enabled(self, mock_boto):
        client = mock.Mock()
        mock_boto.return_value = client
        client.list_event_source_mappings.return_value = self.mock_event_source_mapping
        client.get_event_source_mapping.return_value = {"State": "Enabled"}
        result = enable_triggers(["my_function_name"])
        assert result is False
        mock_boto.assert_called_with("lambda", "us-west-2")
        client.list_event_source_mappings.assert_called_with(FunctionName='my_function_name')
        client.update_event_source_mapping.assert_not_called()

    @mock.patch('src.utils.boto3.client', autospec=True)
    def test_disable_triggers(self, mock_boto):
        client = mock.Mock()
        mock_boto.return_value = client
        client.list_event_source_mappings.return_value = self.mock_event_source_mapping
        client.get_event_source_mapping.return_value = {"State": "Enabled"}
        result = disable_triggers(["my_function_name"])
        assert result is True
        mock_boto.assert_called_with("lambda", "us-west-2")
        client.list_event_source_mappings.assert_called_with(FunctionName='my_function_name')
        client.update_event_source_mapping.assert_called_with(UUID='string', Enabled=False)

    @mock.patch('src.utils.boto3.client', autospec=True)
    def test_disable_triggers_already_disabled(self, mock_boto):
        client = mock.Mock()
        mock_boto.return_value = client
        client.list_event_source_mappings.return_value = self.mock_event_source_mapping
        client.get_event_source_mapping.return_value = {"State": "Disabled"}
        result = disable_triggers(["my_function_name"])
        assert result is False
        mock_boto.assert_called_with("lambda", "us-west-2")
        client.list_event_source_mappings.assert_called_with(FunctionName='my_function_name')
        client.update_event_source_mapping.assert_not_called()

    @mock.patch('src.utils.boto3.client', autospec=True)
    def test_purge_queue(self, mock_boto):
        client = mock.Mock()
        mock_boto.return_value = client
        client.get_queue_url.return_value = {"QueueUrl": "my_queue_url"}
        purge_queue("foo")
        client.purge_queue.assert_called_with(QueueUrl="my_queue_url")

    @mock.patch('src.utils.boto3.client', autospec=True)
    def test_stop_db_cluster(self, mock_boto):
        client = mock.Mock()
        mock_boto.return_value = client
        stop_db_cluster("foo")
        client.stop_db_cluster.assert_called_with(DBClusterIdentifier="foo")

    @mock.patch('src.utils.boto3.client', autospec=True)
    def test_start_db_cluster(self, mock_boto):
        client = mock.Mock()
        mock_boto.return_value = client
        start_db_cluster("foo")
        client.start_db_cluster.assert_called_with(DBClusterIdentifier="foo")

    @mock.patch('src.utils.boto3.client', autospec=True)
    def test_describe_db_clusters(self, mock_boto):
        client = mock.Mock()
        mock_boto.return_value = client
        client.describe_db_clusters.return_value = {
            'DBClusters': [
                {
                    'DBClusterIdentifier': 'foo',
                    'Status': 'available'
                }
            ]
        }
        describe_db_clusters("stop")
        client.describe_db_clusters.assert_called_once_with()
