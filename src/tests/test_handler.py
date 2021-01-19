import json
import os
from unittest import TestCase, mock

from src import handler
from src.db_resize_handler import BIG_DB_SIZE
from src.handler import TRIGGER, STAGES, DB, run_etl_query, DEFAULT_DB_INSTANCE_IDENTIFIER, \
    DEFAULT_DB_CLUSTER_IDENTIFIER


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
    def test_start_capture_db_nothing_to_start(self, mock_boto):
        for stage in STAGES:
            os.environ['STAGE'] = stage
            result = handler.start_capture_db(self.initial_event, self.context)
            assert result['statusCode'] == 200
            assert result['message'] == f"Started the {stage} db: False"

        os.environ['STAGE'] = 'UNKNOWN'
        with self.assertRaises(Exception) as context:
            handler.start_capture_db(self.initial_event, self.context)

    @mock.patch.dict('src.utils.os.environ', mock_env_vars)
    @mock.patch('src.handler.disable_lambda_trigger', autospec=True)
    @mock.patch('src.utils.boto3.client', autospec=True)
    def test_stop_capture_db_nothing_to_stop(self, mock_boto, mock_disable_lambda_trigger):
        mock_disable_lambda_trigger.return_value = True

        for stage in STAGES:
            os.environ['STAGE'] = stage
            result = handler.stop_capture_db(self.initial_event, self.context)
            assert result['statusCode'] == 200
            assert result['message'] == f"Stopped the {stage} db: False"

        os.environ['STAGE'] = 'UNKNOWN'
        with self.assertRaises(Exception) as context:
            handler.stop_capture_db(self.initial_event, self.context)

    @mock.patch.dict('src.utils.os.environ', mock_env_vars)
    @mock.patch('src.handler.run_etl_query')
    def test_stop_observations_db_dont_stop_busy(self, mock_rds):
        mock_rds.return_value = False

        for stage in STAGES:
            os.environ['STAGE'] = stage
            result = handler.stop_observations_db(self.initial_event, self.context)
            assert result['statusCode'] == 200
            assert result['message'] == f"Could not stop the {stage} observations db. It was busy."

        os.environ['STAGE'] = 'UNKNOWN'
        with self.assertRaises(Exception) as context:
            handler.stop_observations_db(self.initial_event, self.context)

    @mock.patch.dict('src.utils.os.environ', mock_env_vars)
    @mock.patch('src.handler.rds_client')
    @mock.patch('src.handler.run_etl_query')
    @mock.patch('src.handler.boto3.client', autospec=True)
    def test_start_observations_db(self, mock_boto, mock_etl, mock_rds):
        client = mock.Mock()
        mock_boto.return_value = client
        mock_etl.return_value = False
        client.start_db_instance.return_value = True
        for stage in STAGES:
            os.environ['STAGE'] = stage
            result = handler.start_observations_db(self.initial_event, self.context)
            assert result['statusCode'] == 200
            assert result['message'] == f"Started the {stage} observations db."

        os.environ['STAGE'] = 'UNKNOWN'
        with self.assertRaises(Exception) as context:
            handler.start_observations_db(self.initial_event, self.context)

    @mock.patch.dict('src.utils.os.environ', mock_env_vars)
    @mock.patch('src.handler.stop_observations_db_instance')
    @mock.patch('src.handler.disable_lambda_trigger', autospec=True)
    @mock.patch('src.handler.run_etl_query')
    @mock.patch('src.utils.boto3.client', autospec=True)
    def test_stop_observations_db_stop_quiet(self, mock_boto, mock_rds, mock_disable_lambda_trigger,
                                             mock_utils_stop_ob):
        mock_disable_lambda_trigger.return_value = True
        mock_utils_stop_ob.return_value = True
        mock_client = mock.Mock()
        mock_boto.return_value = mock_client

        for stage in STAGES:
            os.environ['STAGE'] = stage
            result = handler.stop_observations_db(self.initial_event, self.context)
            assert result['statusCode'] == 200
            assert result['message'] == f"Stopped the {stage} observations db."

        os.environ['STAGE'] = 'UNKNOWN'
        with self.assertRaises(Exception) as context:
            handler.stop_observations_db(self.initial_event, self.context)

    @mock.patch.dict('src.utils.os.environ', mock_env_vars)
    @mock.patch('src.handler.describe_db_clusters')
    @mock.patch('src.handler.get_flow_rate')
    @mock.patch('src.handler.adjust_flow_rate')
    def test_circuit_breaker_ramp_10_to_5(
            self, mock_adjust, mock_get_flow, mock_describe_db_clusters):

        # Test where we ramp down from 10
        mock_get_flow.return_value = 10
        my_alarm = {
            "detail": {
                "state": {
                    "value": "ALARM",
                }
            }
        }
        os.environ['STAGE'] = 'TEST'
        mock_describe_db_clusters.return_value = DB['TEST']
        handler.circuit_breaker(my_alarm, self.context)
        mock_get_flow.assert_called_once()
        mock_adjust.assert_called_once_with(5)

    @mock.patch.dict('src.utils.os.environ', mock_env_vars)
    @mock.patch('src.handler.describe_db_clusters')
    @mock.patch('src.handler.get_flow_rate')
    @mock.patch('src.handler.adjust_flow_rate')
    def test_circuit_breaker_ramp_5_to_0(
            self, mock_adjust, mock_get_flow, mock_describe_db_clusters):

        # Test where we ramp down from 5
        mock_get_flow.return_value = 5
        my_alarm = {
            "detail": {
                "state": {
                    "value": "ALARM",
                }
            }
        }
        os.environ['STAGE'] = 'TEST'
        mock_describe_db_clusters.return_value = DB['TEST']
        handler.circuit_breaker(my_alarm, self.context)
        mock_get_flow.assert_called_once()
        mock_adjust.assert_called_once_with(0)

    @mock.patch.dict('src.utils.os.environ', mock_env_vars)
    @mock.patch('src.handler.describe_db_clusters')
    @mock.patch('src.handler.get_flow_rate')
    @mock.patch('src.handler.adjust_flow_rate')
    def test_circuit_breaker_ramp_0_to_0(
            self, mock_adjust, mock_get_flow, mock_describe_db_clusters):

        # Test where we ramp down from 0
        mock_get_flow.return_value = 0
        my_alarm = {
            "detail": {
                "state": {
                    "value": "ALARM",
                }
            }
        }
        os.environ['STAGE'] = 'TEST'
        mock_describe_db_clusters.return_value = DB['TEST']
        handler.circuit_breaker(my_alarm, self.context)
        mock_get_flow.assert_called_once()
        mock_adjust.assert_not_called()

    @mock.patch.dict('src.utils.os.environ', mock_env_vars)
    @mock.patch('src.handler.describe_db_clusters')
    @mock.patch('src.handler.get_flow_rate')
    @mock.patch('src.handler.adjust_flow_rate')
    def test_circuit_breaker_ramp_0_to_5(
            self, mock_adjust, mock_get_flow, mock_describe_db_clusters):

        # Test where we ramp from zero
        mock_get_flow.return_value = 0
        my_alarm = {
            "detail": {
                "state": {
                    "value": "OK",
                }
            }
        }
        os.environ['STAGE'] = 'TEST'
        mock_describe_db_clusters.return_value = DB['TEST']
        handler.circuit_breaker(my_alarm, self.context)
        mock_get_flow.assert_called_once()
        mock_adjust.assert_called_once_with(5)

    @mock.patch.dict('src.utils.os.environ', mock_env_vars)
    @mock.patch('src.handler.describe_db_clusters')
    @mock.patch('src.handler.get_flow_rate')
    @mock.patch('src.handler.adjust_flow_rate')
    def test_circuit_breaker_ramp_5_to_10(
            self, mock_adjust, mock_get_flow, mock_describe_db_clusters):

        # Test where we ramp from zero
        mock_get_flow.return_value = 5
        my_alarm = {
            "detail": {
                "state": {
                    "value": "OK",
                }
            }
        }
        os.environ['STAGE'] = 'TEST'
        mock_describe_db_clusters.return_value = DB['TEST']
        handler.circuit_breaker(my_alarm, self.context)
        mock_get_flow.assert_called_once()
        mock_adjust.assert_called_once_with(10)

    @mock.patch.dict('src.utils.os.environ', mock_env_vars)
    @mock.patch('src.handler.describe_db_clusters')
    @mock.patch('src.handler.get_flow_rate')
    @mock.patch('src.handler.adjust_flow_rate')
    def test_circuit_breaker_ramp_10_to_10(
            self, mock_adjust, mock_get_flow, mock_describe_db_clusters):

        # Test where we ramp from zero
        mock_get_flow.return_value = 10
        my_alarm = {
            "detail": {
                "state": {
                    "value": "OK",
                }
            }
        }
        os.environ['STAGE'] = 'TEST'
        mock_describe_db_clusters.return_value = DB['TEST']
        handler.circuit_breaker(my_alarm, self.context)
        mock_get_flow.assert_called_once()
        mock_adjust.assert_not_called()

    @mock.patch.dict('src.utils.os.environ', mock_env_vars)
    @mock.patch('src.handler.describe_db_clusters')
    @mock.patch('src.handler.get_flow_rate')
    @mock.patch('src.handler.adjust_flow_rate')
    def test_circuit_breaker_ramp_bogus_flow_rate(
            self, mock_adjust, mock_get_flow, mock_describe_db_clusters):
        my_alarm = {
            "detail": {
                "state": {
                    "value": "OK",
                }
            }
        }

        os.environ['STAGE'] = 'TEST'
        mock_get_flow.return_value = 9000
        with self.assertRaises(Exception) as context:
            handler.circuit_breaker(my_alarm, self.context)

    @mock.patch.dict('src.utils.os.environ', mock_env_vars)
    @mock.patch('src.handler.describe_db_clusters')
    @mock.patch('src.handler.get_flow_rate')
    @mock.patch('src.handler.adjust_flow_rate')
    def test_circuit_breaker_bogus_stage(
            self, mock_adjust, mock_get_flow, mock_describe_db_clusters):

        os.environ['STAGE'] = 'UNKNOWN'
        with self.assertRaises(Exception) as context:
            handler.circuit_breaker(self.initial_event, self.context)

    @mock.patch('src.handler.disable_lambda_trigger', autospec=True)
    @mock.patch('src.handler.run_etl_query')
    @mock.patch('src.utils.boto3.client', autospec=True)
    def test_stop_observations_test_db_stop_quiet(self, mock_boto, mock_rds, mock_disable_lambda_trigger):
        mock_disable_lambda_trigger.return_value = True
        mock_client = mock.Mock()
        mock_boto.return_value = mock_client
        mock_rds.return_value = True
        os.environ['STAGE'] = 'TEST'
        result = handler.stop_observations_db(self.initial_event, self.context)
        assert result['statusCode'] == 200
        assert result['message'] == 'Stopped the TEST observations db.'

    @mock.patch.dict('src.utils.os.environ', mock_env_vars)
    @mock.patch('src.handler.enable_lambda_trigger', autospec=True)
    @mock.patch('src.utils.boto3.client', autospec=True)
    def test_start_capture_db_something_to_start(self, mock_boto, mock_enable_lambda_trigger):
        mock_enable_lambda_trigger.return_value = True
        mock_client = mock.Mock()
        mock_boto.return_value = mock_client
        my_mock_db_clusters = self.mock_db_clusters

        for stage in STAGES:
            my_mock_db_clusters['DBClusters'][0]['DBClusterIdentifier'] = DB[stage]
            mock_client.describe_db_clusters.return_value = my_mock_db_clusters
            mock_client.start_db_cluster.return_value = {DB[stage]}
            mock_client.get_queue_url.return_value = {'QueueUrl': 'queue'}
            mock_client.list_event_source_mappings.return_value = self.mock_event_source_mapping
            os.environ['STAGE'] = stage
            result = handler.start_capture_db(self.initial_event, self.context)
            assert result['statusCode'] == 200
            assert result['message'] == f"Started the {stage} db: True"

        os.environ['STAGE'] = 'UNKNOWN'
        with self.assertRaises(Exception) as context:
            handler.start_capture_db(self.initial_event, self.context)

    @mock.patch('src.rds.RDS')
    def test_run_etl_query(self, mock_rds):
        """
        So there are 4 ETLs running
        """
        mock_rds.execute_sql.return_value = [4]
        result = run_etl_query(mock_rds)
        assert result is False

        """
        No ETLs running so we can turn off the db
        """
        mock_rds.execute_sql.return_value = [0]
        result = run_etl_query(mock_rds)
        assert result is True

    @mock.patch.dict('src.utils.os.environ', mock_env_vars)
    @mock.patch('src.handler.disable_lambda_trigger', autospec=True)
    @mock.patch('src.utils.boto3.client', autospec=True)
    def test_stop_capture_db_something_to_stop(self, mock_boto, mock_disable_lambda_trigger):
        mock_disable_lambda_trigger.return_value = True
        mock_client = mock.Mock()
        my_mock_db_clusters = self.mock_db_clusters
        mock_client.describe_db_clusters.return_value = my_mock_db_clusters
        mock_client.list_event_source_mappings.return_value = self.mock_event_source_mapping
        mock_boto.return_value = mock_client

        for stage in STAGES:
            my_mock_db_clusters['DBClusters'][0]['DBClusterIdentifier'] = DB[stage]
            my_mock_db_clusters['DBClusters'][0]['Status'] = 'available'
            mock_client.describe_db_clusters.return_value = my_mock_db_clusters
            mock_client.start_db_cluster.return_value = {DB[stage]}
            mock_client.get_queue_url.return_value = {'QueueUrl': 'queue'}
            os.environ['STAGE'] = stage
            result = handler.stop_capture_db(self.initial_event, self.context)
            assert result['statusCode'] == 200
            assert result['message'] == f"Stopped the {stage} db: True"

        os.environ['STAGE'] = 'UNKNOWN'
        with self.assertRaises(Exception) as context:
            handler.stop_capture_db(self.initial_event, self.context)

    @mock.patch('src.utils.boto3.client', autospec=True)
    def test_troubleshoot_stop(self, mock_boto):
        mock_client = mock.Mock()
        my_mock_db_clusters = self.mock_db_clusters
        mock_client.describe_db_clusters.return_value = my_mock_db_clusters
        mock_boto.return_value = mock_client
        stage = 'TEST'
        my_mock_db_clusters['DBClusters'][0]['DBClusterIdentifier'] = DB[stage]
        my_mock_db_clusters['DBClusters'][0]['Status'] = 'available'
        mock_client.describe_db_clusters.return_value = my_mock_db_clusters
        mock_client.start_db_cluster.return_value = {DB[stage]}
        os.environ['STAGE'] = stage
        handler.troubleshoot({"action": "stop_capture_db"}, self.context)
        mock_client.describe_db_clusters.assert_called_once()
        mock_client.stop_db_cluster.assert_called_once_with(DBClusterIdentifier='nwcapture-test')

    @mock.patch('src.utils.boto3.client', autospec=True)
    def test_troubleshoot_start(self, mock_boto):
        mock_client = mock.Mock()
        my_mock_db_clusters = self.mock_db_clusters
        mock_client.describe_db_clusters.return_value = my_mock_db_clusters
        mock_boto.return_value = mock_client
        stage = 'TEST'
        my_mock_db_clusters['DBClusters'][0]['DBClusterIdentifier'] = DB[stage]
        my_mock_db_clusters['DBClusters'][0]['Status'] = 'stopped'
        mock_client.describe_db_clusters.return_value = my_mock_db_clusters
        mock_client.start_db_cluster.return_value = {DB[stage]}
        os.environ['STAGE'] = stage
        handler.troubleshoot({"action": "start_capture_db"}, self.context)
        mock_client.describe_db_clusters.assert_called_once()
        mock_client.start_db_cluster.assert_called_once_with(DBClusterIdentifier='nwcapture-test')

    @mock.patch('src.handler.rds_client')
    def test_troubleshoot_start_bad_actions(self, mock_rds):
        with self.assertRaises(Exception) as context:
            handler.troubleshoot({}, self.context)
        with self.assertRaises(Exception) as context:
            handler.troubleshoot({"action": "unknown"}, self.context)

    @mock.patch('src.utils.boto3.client', autospec=True)
    def test_make_kms_key(self, mock_boto):
        mock_client = mock.Mock()
        mock_client.create_key.return_value = {
            'KeyMetadata': {
                'KeyId': '12345'
            }
        }
        mock_boto.return_value = mock_client

        os.environ['ARN_PREFIX'] = "arn:aws:iam::None"
        handler.troubleshoot(
            {"action": "make_kms_key", "key_project": "WQP-EXTERNAL", "key_stage": "TEST"},
            self.context)
        mock_client.create_key.assert_called_once_with(
            Description='IOW WQP-EXTERNAL TEST key',
            KeyUsage='ENCRYPT_DECRYPT',
            Origin='AWS_KMS',
            Tags=[{'TagKey': 'wma:organization', 'TagValue': 'IOW'}])
        mock_client.create_alias.assert_called_once_with(AliasName='alias/IOW-WQP-EXTERNAL-TEST', TargetKeyId='12345')

    @mock.patch('src.handler.secrets_client', autospec=True)
    def test_change_secret_kms_key(self, mock_boto):
        handler.troubleshoot(
            {"action": "change_secret_kms_key", "new_kms_key": "my_kms_key_id", "secret_id": "my_secret_id"},
            self.context)
        mock_boto.update_secret.assert_called_once_with(SecretId='my_secret_id', KmsKeyId='my_kms_key_id')

    @mock.patch('src.utils.boto3.client', autospec=True)
    def test_change_kms_key_policy(self, mock_boto):
        mock_client = mock.Mock()
        os.environ['ACCOUNT_ID'] = 'my_account_id'
        mock_client.put_key_policy.return_value = {
            'KeyMetadata': {
                'KeyId': '12345'
            }
        }
        mock_boto.return_value = mock_client

        handler.troubleshoot(
            {"action": "change_kms_key_policy", "key_id": "key123"},
            self.context)
        mock_client.put_key_policy.assert_called_once_with(
            KeyId='key123', PolicyName='default',
            Policy='{"Version": "2012-10-17", "Id": "key-consolepolicy-3", "Statement": [{"Sid": "Enable IAM User Permissions", "Effect": "Allow", "Principal": {"AWS": "arn:aws:iam::my_account_id:root"}, "Action": "kms:*", "Resource": "*"}, {"Sid": "Allow access for Key Administrators", "Effect": "Allow", "Principal": {"AWS": "arn:aws:iam::my_account_id:role/Ec2-Role"}, "Action": ["kms:Create*", "kms:Describe*", "kms:Enable*", "kms:List*", "kms:Put*", "kms:Update*", "kms:Revoke*", "kms:Disable*", "kms:Get*", "kms:Delete*", "kms:TagResource", "kms:UntagResource", "kms:ScheduleKeyDeletion", "kms:CancelKeyDeletion"], "Resource": "*"}, {"Sid": "Allow use of the key", "Effect": "Allow", "Principal": {"AWS": ["arn:aws:iam::my_account_id:role/adfs-developers", "arn:aws:iam::my_account_id:role/adfs-app-operations", "arn:aws:iam::my_account_id:role/Ec2-Role"]}, "Action": ["kms:Encrypt", "kms:Decrypt", "kms:ReEncrypt*", "kms:GenerateDataKey*", "kms:DescribeKey"], "Resource": "*"}, {"Sid": "Allow attachment of persistent resources", "Effect": "Allow", "Principal": {"AWS": "arn:aws:iam::my_account_id:role/Ec2-Role"}, "Action": ["kms:CreateGrant", "kms:ListGrants", "kms:RevokeGrant"], "Resource": "*", "Condition": {"Bool": {"kms:GrantIsForAWSResource": "true"}}}]}'
        )

    @mock.patch('src.utils.boto3.client', autospec=True)
    def test_create_efs_access_point(self, mock_boto):
        mock_client = mock.Mock()
        mock_client.create_access_point.return_value = \
            {
                'AccessPointId': 'mynewaccesspointid',
            }

        mock_boto.return_value = mock_client
        handler.troubleshoot(
            {"action": "create_access_point", "file_system_id": "my_file_system_id"},
            self.context
        )
        mock_client.create_access_point.assert_called_once_with(
            ClientToken='iow-geoserver-test',
            Tags=[{'Key': 'wma:organization', 'Value': 'IOW'}, {'Key': 'Name', 'Value': 'iow-geoserver-test'}],
            FileSystemId='my_file_system_id', PosixUser={'Uid': 1001, 'Gid': 1001, 'SecondaryGids': []},
            RootDirectory={'Path': '/data', 'CreationInfo': {'OwnerUid': 1001, 'OwnerGid': 1001, 'Permissions': '0777'}}
        )

    @mock.patch('src.handler.boto3.resource', autospec=True)
    @mock.patch('src.utils.boto3.client', autospec=True)
    def test_create_create_security_group(self, mock_boto, mock_resource):
        os.environ['AWS_DEPLOYMENT_REGION'] = 'us-west-2'
        mock_client = mock.Mock()
        mock_client.create_security_group.return_value = {
            'GroupId': 'f12345'
        }
        mock_boto.return_value = mock_client
        handler.troubleshoot(
            {
                "action": "create_fargate_security_group",
                "description": "test security group",
                "group_name": "my group",
                "vpc_id": "fsa12345"
            },
            self.context
        )
        mock_client.create_security_group.assert_called_once_with(
            Description='test security group', GroupName='my group', VpcId='fsa12345'
        )

    @mock.patch('src.utils.boto3.client', autospec=True)
    def test_purge_queue(self, mock_boto):
        os.environ['AWS_DEPLOYMENT_REGION'] = 'us-west-2'
        mock_client = mock.MagicMock()
        mock_client.purge_queue.return_value = None
        mock_boto.return_value = mock_client

        handler.troubleshoot(
            {"action": "purge_queues"},
            self.context
        )

        self.assertEqual(mock_client.purge_queue.call_count, 2)

    @mock.patch('src.utils.boto3.client', autospec=True)
    def test_adjust_flow_rate(self, mock_boto):
        os.environ['AWS_DEPLOYMENT_REGION'] = 'us-west-2'
        mock_client = mock.MagicMock()
        mock_client.put_function_concurrency.return_value = None
        mock_boto.return_value = mock_client
        handler.adjust_flow_rate(10)
        mock_client.put_function_concurrency.assert_called_once_with(
            FunctionName='aqts-capture-trigger-TEST-aqtsCaptureTrigger', ReservedConcurrentExecutions=10)

    @mock.patch('src.utils.boto3.client', autospec=True)
    def test_get_flow_rate(self, mock_boto):
        os.environ['AWS_DEPLOYMENT_REGION'] = 'us-west-2'
        mock_client = mock.MagicMock()
        mock_client.get_function_concurrency.return_value = {'ReservedConcurrentExecutions': 10}
        mock_boto.return_value = mock_client
        handler.get_flow_rate()
        mock_client.get_function_concurrency.assert_called_once_with(
            FunctionName='aqts-capture-trigger-TEST-aqtsCaptureTrigger')
