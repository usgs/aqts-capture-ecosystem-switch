import json
import os
import datetime
from unittest import TestCase, mock

from src import db_create_handler
from src.db_create_handler import _get_observation_snapshot_identifier, _get_date_string
from src.db_resize_handler import BIG_DB_SIZE
from src.handler import DEFAULT_DB_INSTANCE_IDENTIFIER, \
    DEFAULT_DB_CLUSTER_IDENTIFIER
from src.utils import CAPTURE_INSTANCE_TAGS, OBSERVATION_INSTANCE_TAGS


class TestDbCreateHandler(TestCase):
    queue_url = 'https://sqs.us-south-10.amazonaws.com/887501/some-queue-name'
    sns_arn = 'arn:aws:sns:us-south-23:5746521541:fake-notification'
    region = 'us-south-10'
    mock_env_vars = {
        'AWS_DEPLOYMENT_REGION': region
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
        os.environ['CAN_DELETE_DB'] = 'true'
        db_create_handler.delete_capture_db({}, {})
        mock_triggers.return_value = True
        mock_rds.delete_db_instance.assert_called_once_with(
            DBInstanceIdentifier=DEFAULT_DB_INSTANCE_IDENTIFIER,
            SkipFinalSnapshot=True)
        mock_rds.delete_db_cluster.assert_called_once_with(
            DBClusterIdentifier=DEFAULT_DB_CLUSTER_IDENTIFIER,
            SkipFinalSnapshot=True)

    @mock.patch('src.db_create_handler.disable_lambda_trigger', autospec=True)
    @mock.patch('src.db_create_handler.rds_client')
    def test_delete_capture_db_invalid_tier(self, mock_rds, mock_triggers):
        os.environ['STAGE'] = 'QA'
        os.environ['CAN_DELETE_DB'] = 'false'
        with self.assertRaises(Exception) as context:
            db_create_handler.delete_capture_db({}, {})
        mock_triggers.return_value = True
        mock_rds.delete_db_instance.assert_not_called()
        mock_rds.delete_db_cluster.assert_not_called()

    @mock.patch('src.db_create_handler.rds_client')
    def test_create_db_instance_default(self, mock_rds):
        os.environ['STAGE'] = 'TEST'
        os.environ['CAN_DELETE_DB'] = 'true'
        db_create_handler.create_db_instance({}, {})

        mock_rds.create_db_instance.assert_called_once_with(
            DBInstanceIdentifier=DEFAULT_DB_INSTANCE_IDENTIFIER,
            DBInstanceClass=BIG_DB_SIZE,
            DBClusterIdentifier=DEFAULT_DB_CLUSTER_IDENTIFIER,
            Engine='aurora-postgresql',
            Tags=CAPTURE_INSTANCE_TAGS
        )

    @mock.patch('src.db_create_handler.rds_client')
    def test_create_db_instance_default_invalid_tier(self, mock_rds):
        os.environ['STAGE'] = 'QA'
        os.environ['CAN_DELETE_DB'] = 'false'
        with self.assertRaises(Exception) as context:
            db_create_handler.create_db_instance({}, {})
        mock_rds.create_db_instance.assert_not_called()

    @mock.patch('src.db_create_handler.secrets_client')
    @mock.patch('src.db_create_handler.rds_client')
    def test_modify_postgres_password(self, mock_rds, mock_secrets_client):
        os.environ['STAGE'] = 'QA'
        os.environ['CAN_DELETE_DB'] = 'true'
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
    def test_modify_postgres_password_invalid_tier(self, mock_rds, mock_secrets_client):
        os.environ['STAGE'] = 'QA'
        os.environ['CAN_DELETE_DB'] = 'false'
        my_secret_string = json.dumps({"POSTGRES_PASSWORD": "Password123"})
        mock_secret_payload = {
            "SecretString": my_secret_string
        }
        mock_secrets_client.get_secret_value.return_value = mock_secret_payload
        with self.assertRaises(Exception) as context:
            db_create_handler.modify_postgres_password({}, {})
        mock_rds.modify_db_cluster.assert_not_called()

    @mock.patch('src.db_create_handler.secrets_client')
    @mock.patch('src.db_create_handler.rds_client')
    def test_restore_db_cluster(self, mock_rds, mock_secrets_client):
        os.environ['STAGE'] = 'QA'
        os.environ['CAN_DELETE_DB'] = 'true'
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
        two_days_ago = datetime.datetime.now() - datetime.timedelta(2)
        date_str = _get_date_string(two_days_ago)
        mock_rds.describe_db_snapshots.return_value = {
            'DBSnapshots': [
                {
                    "DBInstanceIdentifier": 'aqts-capture-db-legacy-production-external',
                    "DBSnapshotIdentifier": f"rds:aqts-capture-db-legacy-production-external-{date_str}"

                }
            ]
        }

        db_create_handler.restore_db_cluster({}, {})
        mock_rds.restore_db_cluster_from_snapshot.assert_called_once()

    @mock.patch('src.db_create_handler.secrets_client')
    @mock.patch('src.db_create_handler.rds_client')
    def test_restore_db_cluster_invalid_tier(self, mock_rds, mock_secrets_client):
        os.environ['STAGE'] = 'QA'
        os.environ['CAN_DELETE_DB'] = 'false'
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
        with self.assertRaises(Exception) as context:
            db_create_handler.restore_db_cluster({}, {})
        mock_rds.restore_db_cluster_from_snapshot.assert_not_called()

    @mock.patch('src.db_create_handler.secrets_client')
    @mock.patch('src.db_create_handler.rds_client')
    def test_restore_db_cluster_invalid_secrets(self, mock_rds, mock_secrets_client):
        os.environ['STAGE'] = 'QA'
        os.environ['CAN_DELETE_DB'] = 'false'
        my_secret_string = json.dumps(
            {
            }
        )
        mock_secret_payload = {
            "SecretString": my_secret_string
        }
        mock_secrets_client.get_secret_value.return_value = mock_secret_payload
        with self.assertRaises(Exception) as context:
            db_create_handler.restore_db_cluster({}, {})
        mock_rds.restore_db_cluster_from_snapshot.assert_not_called()

    @mock.patch('src.db_create_handler.enable_lambda_trigger', autospec=True)
    @mock.patch('src.db_create_handler.RDS', autospec=True)
    @mock.patch('src.db_create_handler.sqs_client')
    @mock.patch('src.db_create_handler.secrets_client')
    @mock.patch('src.db_create_handler.rds_client')
    def test_modify_schema_owner_password(self, mock_rds, mock_secrets_client, mock_sqs_client,
                                          mock_db, mock_triggers):
        os.environ['CAN_DELETE_DB'] = 'true'
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

    @mock.patch('src.db_create_handler.enable_lambda_trigger', autospec=True)
    @mock.patch('src.db_create_handler.RDS', autospec=True)
    @mock.patch('src.db_create_handler.sqs_client')
    @mock.patch('src.db_create_handler.secrets_client')
    @mock.patch('src.db_create_handler.rds_client')
    def test_modify_schema_owner_password_invalid_tier(self, mock_rds, mock_secrets_client, mock_sqs_client,
                                                       mock_db, mock_triggers):
        os.environ['CAN_DELETE_DB'] = 'false'
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
        with self.assertRaises(Exception) as context:
            db_create_handler.modify_schema_owner_password({}, {})
        self.assertEqual(mock_sqs_client.purge_queue.call_count, 0)

    @mock.patch('src.db_create_handler.secrets_client')
    @mock.patch('src.db_create_handler.rds_client')
    def test_create_observation_db(self, mock_rds, mock_secrets_client):
        os.environ['STAGE'] = 'TEST'
        os.environ['CAN_DELETE_DB'] = 'true'
        my_secret_string = json.dumps(
            {
                "KMS_KEY_ID": "kms",
                "DB_SUBGROUP_NAME": "subgroup",
                "VPC_SECURITY_GROUP_ID": "vpc_id",
                "DATABASE_NAME": "dbname"
            }
        )
        mock_secret_payload = {
            "SecretString": my_secret_string
        }
        mock_secrets_client.get_secret_value.return_value = mock_secret_payload
        two_days_ago = datetime.datetime.now() - datetime.timedelta(2)
        date_str = _get_date_string(two_days_ago)
        mock_rds.describe_db_snapshots.return_value = {
            'DBSnapshots': [
                {
                    "DBInstanceIdentifier": 'observations-db-legacy-production-external',
                    "DBSnapshotIdentifier": f"rds:observations-db-legacy-production-external-{date_str}"

                }
            ]
        }

        db_create_handler.create_observation_db({}, {})
        mock_rds.restore_db_instance_from_db_snapshot.assert_called_once()

    @mock.patch('src.db_create_handler.secrets_client')
    @mock.patch('src.db_create_handler.rds_client')
    def test_create_observation_db_invalid_tier(self, mock_rds, mock_secrets_client):
        os.environ['STAGE'] = 'TEST'
        os.environ['CAN_DELETE_DB'] = 'false'
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
        two_days_ago = datetime.datetime.now() - datetime.timedelta(2)
        date_str = _get_date_string(two_days_ago)
        mock_rds.describe_db_snapshots.return_value = {
            'DBSnapshots': [
                {
                    "DBInstanceIdentifier": 'observations-db-legacy-production-external2',
                    "DBSnapshotIdentifier": f"rds:observations-db-legacy-production-external2-{date_str}"

                }
            ]
        }
        with self.assertRaises(Exception) as context:
            db_create_handler.create_observation_db({}, {})
        mock_rds.restore_db_instance_from_db_snapshot.assert_not_called()

    @mock.patch('src.db_create_handler.secrets_client')
    @mock.patch('src.db_create_handler.rds_client')
    def test_delete_observation_db(self, mock_rds, mock_secrets_client):
        os.environ['STAGE'] = 'QA'
        os.environ['CAN_DELETE_DB'] = 'true'
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
        db_create_handler.delete_observation_db({}, {})

        mock_rds.delete_db_instance.assert_called_once_with(
            DBInstanceIdentifier='observations-test',
            SkipFinalSnapshot=True
        )


    @mock.patch('src.db_create_handler.secrets_client')
    @mock.patch('src.db_create_handler.rds_client')
    def test_delete_observation_db_invalid_tier(self, mock_rds, mock_secrets_client):
        os.environ['STAGE'] = 'QA'
        os.environ['CAN_DELETE_DB'] = 'false'
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
        with self.assertRaises(Exception) as context:
            db_create_handler.delete_observation_db({}, {})
        mock_rds.delete_db_instance.assert_not_called()
        mock_rds.delete_db_snapshot.assert_not_called()

    @mock.patch('src.db_create_handler.secrets_client')
    @mock.patch('src.db_create_handler.rds_client')
    def test_modify_observation_postgres_password(self, mock_rds, mock_secrets_client):
        os.environ['STAGE'] = 'TEST'
        os.environ['CAN_DELETE_DB'] = 'true'
        my_secret_string = json.dumps(
            {
                "POSTGRES_PASSWORD": "Password123"
            }
        )
        mock_secret_payload = {
            "SecretString": my_secret_string
        }
        mock_secrets_client.get_secret_value.return_value = mock_secret_payload

        db_create_handler.modify_observation_postgres_password({}, {})
        mock_rds.modify_db_instance.assert_called_once_with(
            DBInstanceIdentifier='observations-test', ApplyImmediately=True, MasterUserPassword='Password123'
        )

    @mock.patch('src.db_create_handler.secrets_client')
    @mock.patch('src.db_create_handler.rds_client')
    def test_modify_observation_postgres_password_invalid_tier(self, mock_rds, mock_secrets_client):
        os.environ['STAGE'] = 'TEST'
        os.environ['CAN_DELETE_DB'] = 'false'
        my_secret_string = json.dumps(
            {
                "POSTGRES_PASSWORD": "Password123"
            }
        )
        mock_secret_payload = {
            "SecretString": my_secret_string
        }
        mock_secrets_client.get_secret_value.return_value = mock_secret_payload
        with self.assertRaises(Exception) as context:
            db_create_handler.modify_observation_postgres_password({}, {})
        mock_rds.modify_db_instance.assert_not_called()

    @mock.patch('src.db_create_handler.RDS', autospec=True)
    @mock.patch('src.db_create_handler.secrets_client')
    @mock.patch('src.db_create_handler.rds_client')
    def test_modify_observation_passwords(self, mock_rds, mock_secrets_client,
                                          mock_db):
        os.environ['STAGE'] = 'TEST'
        os.environ['CAN_DELETE_DB'] = 'true'
        my_secret_string = json.dumps(
            {
                "WQP_SCHEMA_OWNER_USERNAME": "name",
                "WQP_SCHEMA_OWNER_PASSWORD": "Password123",
                "WQP_READ_ONLY_USERNAME": "name",
                "WQP_READ_ONLY_PASSWORD": "Password123",
                "ARS_SCHEMA_OWNER_USERNAME": "name",
                "ARS_SCHEMA_OWNER_PASSWORD": "Password123",
                "EPA_SCHEMA_OWNER_USERNAME": "name",
                "EPA_SCHEMA_OWNER_PASSWORD": "Password123",
                "NWIS_SCHEMA_OWNER_USERNAME": "name",
                "NWIS_SCHEMA_OWNER_PASSWORD": "Password123",
                "DB_OWNER_USERNAME": "name",
                "DB_OWNER_PASSWORD": "Password123",
                "WDFN_DB_READ_ONLY_USERNAME": "name",
                "WDFN_DB_READ_ONLY_PASSWORD": "Password123",
                "DATABASE_ADDRESS": "blah",
                "DATABASE_NAME": "blah",
                "POSTGRES_PASSWORD": "blah"
            }
        )
        mock_secret_payload = {
            "SecretString": my_secret_string
        }
        mock_secrets_client.get_secret_value.return_value = mock_secret_payload
        result = db_create_handler.modify_observation_passwords({}, {})
        assert result is True

    @mock.patch('src.db_create_handler.RDS', autospec=True)
    @mock.patch('src.db_create_handler.secrets_client')
    @mock.patch('src.db_create_handler.rds_client')
    def test_modify_observation_passwords_invalid_tier(self, mock_rds, mock_secrets_client,
                                                       mock_db):
        os.environ['STAGE'] = 'TEST'
        os.environ['CAN_DELETE_DB'] = 'false'
        my_secret_string = json.dumps(
            {
                "WQP_SCHEMA_OWNER_USERNAME": "name",
                "WQP_SCHEMA_OWNER_PASSWORD": "Password123",
                "WQP_READ_ONLY_USERNAME": "name",
                "WQP_READ_ONLY_PASSWORD": "Password123",
                "ARS_SCHEMA_OWNER_USERNAME": "name",
                "ARS_SCHEMA_OWNER_PASSWORD": "Password123",
                "EPA_SCHEMA_OWNER_USERNAME": "name",
                "EPA_SCHEMA_OWNER_PASSWORD": "Password123",
                "NWIS_SCHEMA_OWNER_USERNAME": "name",
                "NWIS_SCHEMA_OWNER_PASSWORD": "Password123",
                "DB_OWNER_USERNAME": "name",
                "DB_OWNER_PASSWORD": "Password123",
                "WDFN_DB_READ_ONLY_USERNAME": "name",
                "WDFN_DB_READ_ONLY_PASSWORD": "Password123",
                "DATABASE_ADDRESS": "blah",
                "DATABASE_NAME": "blah",
                "POSTGRES_PASSWORD": "blah"
            }
        )
        mock_secret_payload = {
            "SecretString": my_secret_string
        }
        mock_secrets_client.get_secret_value.return_value = mock_secret_payload
        with self.assertRaises(Exception) as context:
            db_create_handler.modify_observation_passwords({}, {})

    def test_get_date_string(self):
        jan_1 = datetime.datetime(2020, 1, 1)
        date_str = db_create_handler._get_date_string(jan_1)
        assert date_str == "2020-01-01"
