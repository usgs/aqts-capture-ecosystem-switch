import json
import os
from unittest import TestCase, mock

from src import handler, db_create_handler
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

    @mock.patch('src.db_create_handler.disable_lambda_trigger', autospec=True)
    @mock.patch('src.db_create_handler.rds_client')
    def test_delete_capture_db(self, mock_rds, mock_triggers):
        os.environ['STAGE'] = 'QA'
        db_create_handler.delete_capture_db({}, {})
        mock_triggers.return_value = True
        mock_rds.delete_db_instance.assert_called_once_with(
            DBInstanceIdentifier=DEFAULT_DB_INSTANCE_IDENTIFIER,
            SkipFinalSnapshot=True)
        mock_rds.delete_db_cluster.assert_called_once_with(
            DBClusterIdentifier=DEFAULT_DB_CLUSTER_IDENTIFIER,
            SkipFinalSnapshot=True)

    @mock.patch('src.db_create_handler.rds_client')
    def test_create_db_instance_default(self, mock_rds):
        os.environ['STAGE'] = 'QA'
        db_create_handler.create_db_instance({}, {})

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

    @mock.patch('src.db_create_handler.secrets_client')
    @mock.patch('src.db_create_handler.rds_client')
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

        db_create_handler.modify_postgres_password({}, {})
        mock_rds.modify_db_cluster.assert_called_once_with(
            DBClusterIdentifier=DEFAULT_DB_CLUSTER_IDENTIFIER,
            ApplyImmediately=True,
            MasterUserPassword='Password123')

    @mock.patch('src.db_create_handler.secrets_client')
    @mock.patch('src.db_create_handler.rds_client')
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

        db_create_handler.restore_db_cluster({}, {})
        mock_rds.restore_db_cluster_from_snapshot.assert_called_once_with(
            DBClusterIdentifier=DEFAULT_DB_CLUSTER_IDENTIFIER,
            SnapshotIdentifier=db_create_handler.get_snapshot_identifier(),
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

    @mock.patch('src.db_create_handler.enable_lambda_trigger', autospec=True)
    @mock.patch('src.db_create_handler.RDS', autospec=True)
    @mock.patch('src.db_create_handler.sqs_client')
    @mock.patch('src.db_create_handler.secrets_client')
    @mock.patch('src.db_create_handler.rds_client')
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
        db_create_handler.modify_schema_owner_password({}, {})
        self.assertEqual(mock_sqs_client.purge_queue.call_count, 2)