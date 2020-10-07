import os
from unittest import TestCase, mock

from src import handler


class TestHandler(TestCase):
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
    def test_start_test_db_nothing_to_start(self, mock_boto):
        os.environ['STAGE'] = 'TEST'
        result = handler.start_capture_db(self.initial_event, self.context)
        assert result['statusCode'] == 200
        assert result['message'] == 'Started the TEST db: False'

    @mock.patch.dict('src.utils.os.environ', mock_env_vars)
    @mock.patch('src.handler.disable_triggers', autospec=True)
    @mock.patch('src.utils.boto3.client', autospec=True)
    def test_stop_test_db_nothing_to_stop(self, mock_boto, mock_disable_triggers):
        mock_disable_triggers.return_value = True
        os.environ['STAGE'] = 'TEST'
        result = handler.stop_capture_db(self.initial_event, self.context)
        assert result['statusCode'] == 200
        assert result['message'] == 'Stopped the TEST db: False'

    @mock.patch.dict('src.utils.os.environ', mock_env_vars)
    @mock.patch('src.utils.boto3', autospec=True)
    def test_start_qa_db_nothing_to_start(self, mock_boto):
        os.environ['STAGE'] = 'QA'
        result = handler.start_capture_db(self.initial_event, self.context)
        assert result['statusCode'] == 200
        assert result['message'] == 'Started the QA db: False'

    @mock.patch.dict('src.utils.os.environ', mock_env_vars)
    @mock.patch('src.handler.disable_triggers', autospec=True)
    @mock.patch('src.utils.boto3.client', autospec=True)
    def test_stop_qa_db_nothing_to_stop(self, mock_boto, mock_disable_triggers):
        mock_disable_triggers.return_value = True
        os.environ['STAGE'] = 'QA'
        result = handler.stop_capture_db(self.initial_event, self.context)
        assert result['statusCode'] == 200
        assert result['message'] == 'Stopped the QA db: False'

    @mock.patch.dict('src.utils.os.environ', mock_env_vars)
    @mock.patch('src.handler._run_query')
    @mock.patch('src.utils.boto3.client', autospec=True)
    def test_stop_observations_qa_db_dont_stop_busy(self, mock_boto, mock_rds):
        mock_client = mock.Mock()
        mock_boto.return_value = mock_client
        mock_rds.return_value = False
        os.environ['STAGE'] = 'QA'
        result = handler.stop_observations_db(self.initial_event, self.context)
        assert result['statusCode'] == 200
        assert result['message'] == "Could not stop the QA observations db. It was busy."

    @mock.patch.dict('src.utils.os.environ', mock_env_vars)
    @mock.patch('src.handler._run_query')
    @mock.patch('src.utils.boto3.client', autospec=True)
    def test_stop_observations_test_db_dont_stop_busy(self, mock_boto, mock_rds):
        mock_client = mock.Mock()
        mock_boto.return_value = mock_client
        mock_rds.return_value = False
        os.environ['STAGE'] = 'TEST'
        result = handler.stop_observations_db(self.initial_event, self.context)
        assert result['statusCode'] == 200
        assert result['message'] == "Could not stop the TEST observations db. It was busy."

    @mock.patch.dict('src.utils.os.environ', mock_env_vars)
    @mock.patch('src.handler._run_query')
    @mock.patch('src.utils.boto3.client', autospec=True)
    def test_stop_observations_db_unknown_stage(self, mock_boto, mock_rds):
        mock_client = mock.Mock()
        mock_boto.return_value = mock_client
        mock_rds.return_value = False
        with self.assertRaises(Exception) as context:
            handler.stop_observations_db(self.initial_event, self.context)

    @mock.patch.dict('src.utils.os.environ', mock_env_vars)
    @mock.patch('src.handler.disable_triggers', autospec=True)
    @mock.patch('src.handler._run_query')
    @mock.patch('src.utils.boto3.client', autospec=True)
    def test_stop_observations_qa_db_stop_quiet(self, mock_boto, mock_rds, mock_disable_triggers):
        mock_disable_triggers.return_value = True
        mock_client = mock.Mock()
        mock_boto.return_value = mock_client
        mock_rds.return_value = True
        os.environ['STAGE'] = 'QA'
        result = handler.stop_observations_db(self.initial_event, self.context)
        assert result['statusCode'] == 200
        assert result['message'] == 'Stopped the QA db.'

    @mock.patch.dict('src.utils.os.environ', mock_env_vars)
    @mock.patch('src.handler.disable_triggers', autospec=True)
    @mock.patch('src.handler._run_query')
    @mock.patch('src.utils.boto3.client', autospec=True)
    def test_stop_observations_test_db_stop_quiet(self, mock_boto, mock_rds, mock_disable_triggers):
        mock_disable_triggers.return_value = True
        mock_client = mock.Mock()
        mock_boto.return_value = mock_client
        mock_rds.return_value = True
        os.environ['STAGE'] = 'TEST'
        result = handler.stop_observations_db(self.initial_event, self.context)
        assert result['statusCode'] == 200
        assert result['message'] == 'Stopped the TEST db.'

    @mock.patch.dict('src.utils.os.environ', mock_env_vars)
    @mock.patch('src.handler.enable_triggers', autospec=True)
    @mock.patch('src.handler.cloudwatch_client')
    def test_control_db_utilization_enable(self, mock_cloudwatch, mock_enable_triggers):
        mock_enable_triggers.return_value = True
        os.environ['STAGE'] = 'TEST'
        my_alarm = {
            "detail": {
                "state": {
                    "value": "OK",
                }
            }
        }
        handler.control_db_utilization(my_alarm, self.context)
        mock_enable_triggers.assert_called_once()

    @mock.patch.dict('src.utils.os.environ', mock_env_vars)
    @mock.patch('src.handler.disable_triggers', autospec=True)
    @mock.patch('src.handler.cloudwatch_client')
    def test_control_db_utilization_disable(self, mock_cloudwatch, mock_disable_triggers):
        mock_disable_triggers.return_value = True
        os.environ['STAGE'] = 'TEST'
        my_alarm = {
            "detail": {
                "state": {
                    "value": "ALARM",
                }
            }
        }
        handler.control_db_utilization(my_alarm, self.context)
        mock_disable_triggers.assert_called_once()

    @mock.patch.dict('src.utils.os.environ', mock_env_vars)
    @mock.patch('src.handler.enable_triggers', autospec=True)
    @mock.patch('src.utils.boto3.client', autospec=True)
    def test_start_test_db_something_to_start(self, mock_boto, mock_enable_triggers):
        mock_enable_triggers.return_value = True
        mock_client = mock.Mock()
        mock_boto.return_value = mock_client
        my_mock_db_clusters = self.mock_db_clusters
        my_mock_db_clusters['DBClusters'][0]['DBClusterIdentifier'] = 'nwcapture-test'
        mock_client.describe_db_clusters.return_value = my_mock_db_clusters
        mock_client.start_db_cluster.return_value = {'nwcapture-test'}
        mock_client.get_queue_url.return_value = {'QueueUrl': 'queue'}
        mock_client.list_event_source_mappings.return_value = self.mock_event_source_mapping
        os.environ['STAGE'] = 'TEST'
        result = handler.start_capture_db(self.initial_event, self.context)

        assert result['statusCode'] == 200
        assert result['message'] == 'Started the TEST db: True'

    @mock.patch.dict('src.utils.os.environ', mock_env_vars)
    @mock.patch('src.handler.enable_triggers', autospec=True)
    @mock.patch('src.utils.boto3.client', autospec=True)
    def test_start_qa_db_something_to_start(self, mock_boto, mock_enable_triggers):
        mock_enable_triggers.return_value = True
        mock_client = mock.Mock()
        mock_boto.return_value = mock_client
        my_mock_db_clusters = self.mock_db_clusters
        my_mock_db_clusters['DBClusters'][0]['DBClusterIdentifier'] = 'nwcapture-qa'
        mock_client.describe_db_clusters.return_value = my_mock_db_clusters
        mock_client.get_queue_url.return_value = {'QueueUrl': 'queue'}
        mock_client.list_event_source_mappings.return_value = self.mock_event_source_mapping
        os.environ['STAGE'] = 'QA'
        result = handler.start_capture_db(self.initial_event, self.context)
        assert result['statusCode'] == 200
        assert result['message'] == 'Started the QA db: True'

    @mock.patch.dict('src.utils.os.environ', mock_env_vars)
    @mock.patch('src.handler.disable_triggers', autospec=True)
    @mock.patch('src.utils.boto3.client', autospec=True)
    def test_stop_test_db_something_to_stop(self, mock_boto, mock_disable_triggers):
        mock_disable_triggers.return_value = True
        mock_client = mock.Mock()
        mock_boto.return_value = mock_client
        my_mock_db_clusters = self.mock_db_clusters
        my_mock_db_clusters['DBClusters'][0]['DBClusterIdentifier'] = 'nwcapture-test'
        my_mock_db_clusters['DBClusters'][0]['Status'] = 'available'
        mock_client.describe_db_clusters.return_value = my_mock_db_clusters
        mock_client.list_event_source_mappings.return_value = self.mock_event_source_mapping
        os.environ['STAGE'] = 'TEST'
        result = handler.stop_capture_db(self.initial_event, self.context)
        assert result['statusCode'] == 200
        assert result['message'] == 'Stopped the TEST db: True'

    @mock.patch.dict('src.utils.os.environ', mock_env_vars)
    @mock.patch('src.handler.disable_triggers', autospec=True)
    @mock.patch('src.utils.boto3.client', autospec=True)
    def test_stop_qa_db_something_to_stop(self, mock_boto, mock_disable_triggers):
        mock_disable_triggers.return_value = True
        mock_client = mock.Mock()
        mock_boto.return_value = mock_client
        my_mock_db_clusters = self.mock_db_clusters
        my_mock_db_clusters['DBClusters'][0]['DBClusterIdentifier'] = 'nwcapture-qa'
        my_mock_db_clusters['DBClusters'][0]['Status'] = 'available'
        mock_client.describe_db_clusters.return_value = my_mock_db_clusters
        mock_client.list_event_source_mappings.return_value = self.mock_event_source_mapping
        os.environ['STAGE'] = 'QA'
        result = handler.stop_capture_db(self.initial_event, self.context)
        assert result['statusCode'] == 200
        assert result['message'] == 'Stopped the QA db: True'
