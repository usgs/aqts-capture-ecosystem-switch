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
    @mock.patch('src.handler.disable_triggers', autospec=True)
    @mock.patch('src.utils.boto3.client', autospec=True)
    def test_stop_capture_db_nothing_to_stop(self, mock_boto, mock_disable_triggers):
        mock_disable_triggers.return_value = True

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
    @mock.patch('src.handler.run_etl_query')
    @mock.patch('src.handler.boto3.client', autospec=True)
    def test_start_observations_db(self, mock_boto, mock_rds):
        client = mock.Mock()
        mock_boto.return_value = client
        mock_rds.return_value = False
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
    @mock.patch('src.handler.disable_triggers', autospec=True)
    @mock.patch('src.handler.run_etl_query')
    @mock.patch('src.utils.boto3.client', autospec=True)
    def test_stop_observations_db_stop_quiet(self, mock_boto, mock_rds, mock_disable_triggers, mock_utils_stop_ob):
        mock_disable_triggers.return_value = True
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
    @mock.patch('src.handler.enable_triggers', autospec=True)
    def test_control_db_utilization_enable_triggers_when_db_on(self, mock_enable_triggers, mock_describe_db_clusters):
        mock_enable_triggers.return_value = True
        my_alarm = {
            "detail": {
                "state": {
                    "value": "OK",
                }
            }
        }
        for stage in STAGES:
            os.environ['STAGE'] = stage
            mock_describe_db_clusters.return_value = DB[stage]
            handler.control_db_utilization(my_alarm, self.context)
            mock_enable_triggers.assert_called_with(TRIGGER[stage])

        os.environ['STAGE'] = 'UNKNOWN'
        with self.assertRaises(Exception) as context:
            handler.control_db_utilization(self.initial_event, self.context)

    @mock.patch.dict('src.utils.os.environ', mock_env_vars)
    @mock.patch('src.handler.describe_db_clusters')
    @mock.patch('src.handler.enable_triggers', autospec=True)
    def test_control_db_utilization_dont_enable_triggers_when_db_off(self, mock_enable_triggers,
                                                                     mock_describe_db_clusters):
        mock_enable_triggers.return_value = True
        my_alarm = {
            "detail": {
                "state": {
                    "value": "OK",
                }
            }
        }
        for stage in STAGES:
            os.environ['STAGE'] = stage
            mock_describe_db_clusters.return_value = "SomebodyElsesDb"
            handler.control_db_utilization(my_alarm, self.context)
            mock_enable_triggers.assert_not_called()

        os.environ['STAGE'] = 'UNKNOWN'
        with self.assertRaises(Exception) as context:
            handler.control_db_utilization(self.initial_event, self.context)

    @mock.patch.dict('src.utils.os.environ', mock_env_vars)
    @mock.patch('src.handler.disable_triggers', autospec=True)
    def test_control_db_utilization_disable(self, mock_disable_triggers):
        mock_disable_triggers.return_value = True
        my_alarm = {
            "detail": {
                "state": {
                    "value": "ALARM",
                }
            }
        }
        for stage in STAGES:
            os.environ['STAGE'] = stage
            handler.control_db_utilization(my_alarm, self.context)
            mock_disable_triggers.assert_called_with(TRIGGER[stage])

        os.environ['STAGE'] = 'UNKNOWN'
        with self.assertRaises(Exception) as context:
            handler.control_db_utilization(self.initial_event, self.context)

    @mock.patch('src.handler.disable_triggers', autospec=True)
    @mock.patch('src.handler.run_etl_query')
    @mock.patch('src.utils.boto3.client', autospec=True)
    def test_stop_observations_test_db_stop_quiet(self, mock_boto, mock_rds, mock_disable_triggers):
        mock_disable_triggers.return_value = True
        mock_client = mock.Mock()
        mock_boto.return_value = mock_client
        mock_rds.return_value = True
        os.environ['STAGE'] = 'TEST'
        result = handler.stop_observations_db(self.initial_event, self.context)
        assert result['statusCode'] == 200
        assert result['message'] == 'Stopped the TEST observations db.'

    @mock.patch.dict('src.utils.os.environ', mock_env_vars)
    @mock.patch('src.handler.enable_triggers', autospec=True)
    @mock.patch('src.utils.boto3.client', autospec=True)
    def test_start_capture_db_something_to_start(self, mock_boto, mock_enable_triggers):
        mock_enable_triggers.return_value = True
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

    @mock.patch('src.handler.disable_triggers', autospec=True)
    @mock.patch('src.handler.rds_client')
    def test_delete_capture_db(self, mock_rds, mock_triggers):
        os.environ['STAGE'] = 'QA'
        handler.delete_capture_db({}, {})
        mock_triggers.return_value = True
        mock_rds.delete_db_instance.assert_called_once_with(
            DBInstanceIdentifier=DEFAULT_DB_INSTANCE_IDENTIFIER,
            SkipFinalSnapshot=True)
        mock_rds.delete_db_cluster.assert_called_once_with(
            DBClusterIdentifier=DEFAULT_DB_CLUSTER_IDENTIFIER,
            SkipFinalSnapshot=True)

    @mock.patch('src.handler.rds_client')
    def test_create_db_instance_default(self, mock_rds):
        os.environ['STAGE'] = 'QA'
        handler.create_db_instance({}, {})

        mock_rds.create_db_instance.assert_called_once_with(
            DBInstanceIdentifier=DEFAULT_DB_INSTANCE_IDENTIFIER,
            DBInstanceClass=BIG_DB_SIZE,
            DBClusterIdentifier=DEFAULT_DB_CLUSTER_IDENTIFIER,
            Engine='aurora-postgresql',
            Tags=[
                {'Key': 'Name', 'Value': 'NWISWEB-CAPTURE-RDS-AURORA-QA'},
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

    @mock.patch('src.handler.secrets_client')
    @mock.patch('src.handler.rds_client')
    def test_modify_postgres_password(self, mock_rds, mock_secrets_client):
        os.environ['STAGE'] = 'QA'
        my_secret_string = json.dumps(
            {
                "POSTGRES_PASSWORD": "Password123"
            }
        )
        mock_secret_payload = {
            "SecretString": my_secret_string
        }
        mock_secrets_client.get_secret_value.return_value = mock_secret_payload

        handler.modify_postgres_password({}, {})
        mock_rds.modify_db_cluster.assert_called_once_with(
            DBClusterIdentifier=DEFAULT_DB_CLUSTER_IDENTIFIER,
            ApplyImmediately=True,
            MasterUserPassword='Password123')

    @mock.patch('src.handler.secrets_client')
    @mock.patch('src.handler.rds_client')
    def test_restore_db_cluster(self, mock_rds, mock_secrets_client):
        os.environ['STAGE'] = 'QA'
        my_secret_string = json.dumps(
            {
                "KMS_KEY_ID": "kms",
                "DB_SUBGROUP_NAME": "subgroup",
                "VPC_SECURITY_GROUP_ID": "vpc_id"
            }
        )
        mock_secret_payload = {
            "SecretString": my_secret_string
        }
        mock_secrets_client.get_secret_value.return_value = mock_secret_payload

        handler.restore_db_cluster({}, {})
        mock_rds.restore_db_cluster_from_snapshot.assert_called_once_with(
            DBClusterIdentifier=DEFAULT_DB_CLUSTER_IDENTIFIER,
            SnapshotIdentifier=handler.get_snapshot_identifier(),
            Engine='aurora-postgresql',
            EngineVersion='11.7',
            Port=5432,
            DBSubnetGroupName='subgroup',
            DatabaseName='nwcapture-qa',
            EnableIAMDatabaseAuthentication=False,
            EngineMode='provisioned',
            DBClusterParameterGroupName='aqts-capture',
            DeletionProtection=False,
            CopyTagsToSnapshot=False,
            KmsKeyId='kms',
            VpcSecurityGroupIds=['vpc_id'],

            Tags=[{'Key': 'Name', 'Value': 'NWISWEB-CAPTURE-RDS-AURORA-TEST'},
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
                  {'Key': 'taggingVersion', 'Value': '0.0.1'}]

        )

    @mock.patch('src.handler.enable_triggers', autospec=True)
    @mock.patch('src.handler.RDS', autospec=True)
    @mock.patch('src.handler.sqs_client')
    @mock.patch('src.handler.secrets_client')
    @mock.patch('src.handler.rds_client')
    def test_modify_schema_owner_password(self, mock_rds, mock_secrets_client, mock_sqs_client,
                                          mock_db, mock_triggers):
        os.environ['STAGE'] = 'QA'
        mock_triggers.return_value = True
        my_secret_string = json.dumps(
            {
                "DATABASE_ADDRESS": "address",
                "DATABASE_NAME": "name",
                "VPC_SECURITY_GROUP_ID": "vpc_id",
                "POSTGRES_PASSWORD": "Password123",
                "SCHEMA_OWNER_PASSWORD": "Password123"
            }
        )
        mock_secret_payload = {
            "SecretString": my_secret_string
        }
        mock_secrets_client.get_secret_value.return_value = mock_secret_payload
        handler.modify_schema_owner_password({}, {})
        self.assertEqual(mock_sqs_client.purge_queue.call_count, 2)

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
    @mock.patch('src.handler.disable_triggers', autospec=True)
    @mock.patch('src.utils.boto3.client', autospec=True)
    def test_stop_capture_db_something_to_stop(self, mock_boto, mock_disable_triggers):
        mock_disable_triggers.return_value = True
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
