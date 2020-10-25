import os
from unittest import TestCase, mock

from src import db_resize_handler
from src.db_resize_handler import SMALL_DB_SIZE, BIG_DB_SIZE, DEFAULT_DB_CLUSTER_IDENTIFIER
from src.handler import DEFAULT_DB_INSTANCE_IDENTIFIER


class TestDbResizeHandler(TestCase):

    def setUp(self):
        pass

    @mock.patch('src.db_resize_handler.disable_lambda_trigger')
    def test_disable_trigger(self, mock_trigger):
        db_resize_handler.disable_trigger({}, {})
        mock_trigger.assert_called_once()

    @mock.patch('src.db_resize_handler.rds_client')
    @mock.patch('src.db_resize_handler.enable_lambda_trigger')
    def test_enable_trigger(self, mock_trigger, mock_rds):
        mock_rds.describe_db_clusters.return_value = {
            'DBClusters': [
                {
                    'DBClusterIdentifier': DEFAULT_DB_CLUSTER_IDENTIFIER,
                    'Status': 'available'
                }
            ]
        }
        mock_trigger.return_value = True
        db_resize_handler.enable_trigger({}, {})
        mock_trigger.assert_called_once()

    @mock.patch('src.db_resize_handler.rds_client')
    @mock.patch('src.db_resize_handler.enable_lambda_trigger')
    def test_enable_trigger_not_ready(self, mock_trigger, mock_rds):
        mock_rds.describe_db_clusters.return_value = {
            'DBClusters': [
                {
                    'DBClusterIdentifier': DEFAULT_DB_CLUSTER_IDENTIFIER,
                    'Status': 'starting'
                }
            ]
        }
        mock_trigger.return_value = False
        with self.assertRaises(Exception) as context:
            db_resize_handler.enable_trigger({}, {})
        mock_trigger.assert_not_called()

    @mock.patch('src.db_resize_handler._get_cpu_utilization')
    @mock.patch('src.db_resize_handler.rds_client')
    @mock.patch('src.db_resize_handler.disable_lambda_trigger')
    def test_shrink_db_okay(self, mock_utils, mock_rds, mock_cpu_util):
        os.environ['STAGE'] = 'TEST'
        os.environ['SHRINK_THRESHOLD'] = '10'
        os.environ['SHRINK_EVAL_TIME_IN_SECONDS'] = '3600'
        mock_utils.return_value = True
        mock_rds.describe_db_clusters.return_value = {'DBClusters': [
            {
                'DBClusterIdentifier': DEFAULT_DB_CLUSTER_IDENTIFIER,
                'Status': 'available'
            }]}
        mock_rds.describe_db_instances.return_value = {"DBInstances": [{"DBInstanceClass": BIG_DB_SIZE}]}
        mock_cpu_util.return_value = {'MetricDataResults': [{'Values': [0.0]}]}
        db_resize_handler.shrink_db({}, {})
        mock_rds.modify_db_instance.assert_called_once_with(
            DBInstanceIdentifier=DEFAULT_DB_INSTANCE_IDENTIFIER,
            DBInstanceClass=SMALL_DB_SIZE,
            ApplyImmediately=True)

    @mock.patch('src.db_resize_handler._get_cpu_utilization')
    @mock.patch('src.db_resize_handler.rds_client')
    @mock.patch('src.db_resize_handler.disable_lambda_trigger')
    def test_shrink_db_not_available(self, mock_trigger, mock_rds, mock_cpu_util):
        os.environ['STAGE'] = 'TEST'
        os.environ['SHRINK_THRESHOLD'] = '10'
        os.environ['SHRINK_EVAL_TIME_IN_SECONDS'] = '3600'
        mock_trigger.return_value = True
        mock_cpu_util.return_value = {'MetricDataResults': [{'Values': [0.0]}]}
        mock_rds.describe_db_instances.return_value = {"DBInstances": [{"DBInstanceClass": BIG_DB_SIZE}]}
        mock_rds.describe_db_clusters.return_value = {
            'DBClusters': [
                {
                    'DBClusterIdentifier': DEFAULT_DB_CLUSTER_IDENTIFIER,
                    'Status': 'starting'
                }
            ]
        }
        with self.assertRaises(Exception) as context:
            db_resize_handler.shrink_db({}, {})
        mock_rds.modify_db_instance.assert_not_called()

    @mock.patch('src.db_resize_handler._get_cpu_utilization')
    @mock.patch('src.db_resize_handler.rds_client')
    @mock.patch('src.db_resize_handler.disable_lambda_trigger')
    def test_shrink_db_too_busy(self, mock_utils, mock_rds, mock_cpu_util):
        os.environ['STAGE'] = 'TEST'
        os.environ['SHRINK_THRESHOLD'] = '10'
        os.environ['SHRINK_EVAL_TIME_IN_SECONDS'] = '3600'
        mock_utils.return_value = True
        mock_rds.describe_db_instances.return_value = {"DBInstances": [{"DBInstanceClass": BIG_DB_SIZE}]}
        mock_cpu_util.return_value = {'MetricDataResults': [{'Values': [90.0]}]}
        with self.assertRaises(Exception) as context:
            db_resize_handler.shrink_db({}, {})
        mock_rds.modify_db_instance.assert_not_called()

    @mock.patch('src.db_resize_handler._get_cpu_utilization')
    @mock.patch('src.db_resize_handler.rds_client')
    @mock.patch('src.db_resize_handler.disable_lambda_trigger')
    def test_grow_db_okay(self, mock_utils, mock_rds, mock_cpu_util):
        os.environ['STAGE'] = 'TEST'
        os.environ['GROW_THRESHOLD'] = '75'
        os.environ['GROW_EVAL_TIME_IN_SECONDS'] = '3600'
        mock_utils.return_value = True
        mock_rds.describe_db_clusters.return_value = {'DBClusters': [
            {
                'DBClusterIdentifier': DEFAULT_DB_CLUSTER_IDENTIFIER,
                'Status': 'available'
            }]}
        mock_rds.describe_db_instances.return_value = {"DBInstances": [{"DBInstanceClass": SMALL_DB_SIZE}]}
        mock_cpu_util.return_value = {'MetricDataResults': [{'Values': [80.0]}]}
        db_resize_handler.grow_db({}, {})
        mock_rds.modify_db_instance.assert_called_once_with(
            DBInstanceIdentifier=DEFAULT_DB_INSTANCE_IDENTIFIER,
            DBInstanceClass=BIG_DB_SIZE,
            ApplyImmediately=True)

    @mock.patch('src.db_resize_handler._get_cpu_utilization')
    @mock.patch('src.db_resize_handler.rds_client')
    @mock.patch('src.handler.disable_lambda_trigger')
    def test_grow_db_too_busy(self, mock_utils, mock_rds, mock_cpu_util):
        os.environ['STAGE'] = 'TEST'
        os.environ['GROW_THRESHOLD'] = '75'
        os.environ['GROW_EVAL_TIME_IN_SECONDS'] = '3600'
        mock_utils.return_value = True
        mock_rds.describe_db_instances.return_value = {"DBInstances": [{"DBInstanceClass": SMALL_DB_SIZE}]}
        mock_cpu_util.return_value = {'MetricDataResults': [{'Values': [70.0]}]}
        with self.assertRaises(Exception) as context:
            db_resize_handler.grow_db({}, {})
        mock_rds.modify_db_instance.assert_not_called()

    @mock.patch('src.db_resize_handler._get_cpu_utilization')
    @mock.patch('src.db_resize_handler.rds_client')
    @mock.patch('src.db_resize_handler.disable_lambda_trigger')
    def test_grow_db_not_available(self, mock_trigger, mock_rds, mock_cpu_util):
        os.environ['STAGE'] = 'TEST'
        os.environ['GROW_THRESHOLD'] = '10'
        os.environ['GROW_EVAL_TIME_IN_SECONDS'] = '3600'
        mock_trigger.return_value = True
        mock_cpu_util.return_value = {'MetricDataResults': [{'Values': [0.0]}]}
        mock_rds.describe_db_instances.return_value = {"DBInstances": [{"DBInstanceClass": SMALL_DB_SIZE}]}
        mock_rds.describe_db_clusters.return_value = {
            'DBClusters': [
                {
                    'DBClusterIdentifier': DEFAULT_DB_CLUSTER_IDENTIFIER,
                    'Status': 'starting'
                }
            ]
        }
        with self.assertRaises(Exception) as context:
            db_resize_handler.grow_db({}, {})
        mock_rds.modify_db_instance.assert_not_called()

    def test_validate_okay(self):
        os.environ['STAGE'] = 'QA'
        db_resize_handler._validate()
        # If no exception is thrown, we passed

    def test_validate_not_okay(self):
        os.environ['STAGE'] = 'TEST'
        with self.assertRaises(Exception) as context:
            db_resize_handler._validate()

    def test_execute_grow_machine_no_arn(self):
        with self.assertRaises(KeyError) as context:
            db_resize_handler.execute_grow_machine({}, {})

    @mock.patch('src.db_resize_handler._get_cpu_utilization')
    @mock.patch('src.db_resize_handler.boto3', autospec=True)
    def test_execute_grow_machine_alarm_needs_to_grow(self, mock_boto3, mock_cpu_util):
        os.environ['GROW_STATE_MACHINE_ARN'] = 'arn'
        os.environ['GROW_THRESHOLD'] = '75'
        os.environ['GROW_EVAL_TIME_IN_SECONDS'] = '300'
        mock_cpu_util.return_value = {'MetricDataResults': [{'Values': [80.0]}]}
        alarm_event = {
            "detail": {
                "state": {
                    "value": "ALARM"
                }
            }
        }
        result = db_resize_handler.execute_grow_machine(alarm_event, {})
        assert result is True

    @mock.patch('src.db_resize_handler.boto3', autospec=True)
    def test_execute_grow_machine_not_alarm(self, mock_boto3):
        os.environ['GROW_STATE_MACHINE_ARN'] = 'arn'
        alarm_event = {
            "detail": {
                "state": {
                    "value": "INSUFFICIENT DATA"
                }
            }
        }
        result = db_resize_handler.execute_grow_machine(alarm_event, {})
        assert result is False

    def test_execute_shrink_machine_no_arn(self):
        with self.assertRaises(KeyError) as context:
            db_resize_handler.execute_shrink_machine({}, {})

    @mock.patch('src.db_resize_handler.rds_client')
    def test_is_cluster_available_no(self, mock_rds):
        mock_rds.describe_db_clusters.return_value = {
            'DBClusters': [
                {
                    'DBClusterIdentifier': DEFAULT_DB_CLUSTER_IDENTIFIER,
                    'Status': 'starting'
                }
            ]
        }
        with self.assertRaises(Exception) as context:
            db_resize_handler._is_cluster_available(DEFAULT_DB_CLUSTER_IDENTIFIER)

    @mock.patch('src.db_resize_handler.rds_client')
    def test_is_cluster_available_yes(self, mock_rds):
        mock_rds.describe_db_clusters.return_value = {
            'DBClusters': [
                {
                    'DBClusterIdentifier': DEFAULT_DB_CLUSTER_IDENTIFIER,
                    'Status': 'available'
                }
            ]
        }
        result = db_resize_handler._is_cluster_available(DEFAULT_DB_CLUSTER_IDENTIFIER)
        assert result is True
