import os
from unittest import TestCase, mock

from src import db_resize_handler
from src.db_resize_handler import SMALL_DB_SIZE, BIG_DB_SIZE
from src.handler import TRIGGER, DEFAULT_DB_INSTANCE_IDENTIFIER


class TestDbResizeHandler(TestCase):

    def setUp(self):
        pass

    @mock.patch('src.db_resize_handler.rds_client')
    @mock.patch('src.handler.disable_triggers')
    def test_disable_trigger_before_resize_exception_no_resize_action(self, mock_utils, mock_rds):
        os.environ['STAGE'] = 'TEST'
        mock_utils.return_value = True
        mock_rds.describe_db_instances.return_value = {
            "DBInstances": [{
                "DBInstanceClass": SMALL_DB_SIZE
            }]
        }
        with self.assertRaises(Exception) as context:
            db_resize_handler.disable_trigger_before_resize({}, {})
        mock_utils.assert_not_called()

    @mock.patch('src.db_resize_handler.rds_client')
    @mock.patch('src.db_resize_handler.disable_triggers')
    def test_disable_trigger_before_resize_ok_shrink(self, mock_utils, mock_rds):
        os.environ['STAGE'] = 'TEST'
        mock_utils.return_value = True
        mock_rds.describe_db_instances.return_value = {
            "DBInstances": [{
                "DBInstanceClass": BIG_DB_SIZE
            }]
        }
        event = {"resize_action": "SHRINK"}
        db_resize_handler.disable_trigger_before_resize(event, {})
        mock_rds.describe_db_instances.assert_called_once_with(
            DBInstanceIdentifier=DEFAULT_DB_INSTANCE_IDENTIFIER)
        mock_utils.assert_called_once_with(TRIGGER['TEST'])

    @mock.patch('src.db_resize_handler.rds_client')
    @mock.patch('src.db_resize_handler.disable_triggers')
    def test_disable_trigger_before_resize_ok_grow(self, mock_utils, mock_rds):
        os.environ['STAGE'] = 'TEST'
        mock_utils.return_value = True
        mock_rds.describe_db_instances.return_value = {
            "DBInstances": [{
                "DBInstanceClass": SMALL_DB_SIZE
            }]
        }
        event = {"resize_action": "GROW"}
        db_resize_handler.disable_trigger_before_resize(event, {})
        mock_rds.describe_db_instances.assert_called_once_with(
            DBInstanceIdentifier=DEFAULT_DB_INSTANCE_IDENTIFIER)
        mock_utils.assert_called_once_with(TRIGGER['TEST'])

    @mock.patch('src.db_resize_handler.rds_client')
    @mock.patch('src.handler.disable_triggers')
    def test_disable_trigger_before_resize_exception_already_grown(self, mock_utils, mock_rds):
        os.environ['STAGE'] = 'TEST'
        mock_utils.return_value = True
        mock_rds.describe_db_instances.return_value = {
            "DBInstances": [{
                "DBInstanceClass": BIG_DB_SIZE
            }]
        }
        with self.assertRaises(Exception) as context:
            event = {"resize_action": "GROW"}
            db_resize_handler.disable_trigger_before_resize(event, {})
        mock_rds.describe_db_instances.assert_called_once_with(
            DBInstanceIdentifier=DEFAULT_DB_INSTANCE_IDENTIFIER)
        mock_utils.assert_not_called()

    @mock.patch('src.db_resize_handler.rds_client')
    @mock.patch('src.handler.disable_triggers')
    def test_disable_trigger_before_resize_exception_already_shrunk(self, mock_utils, mock_rds):
        os.environ['STAGE'] = 'TEST'
        mock_utils.return_value = True
        mock_rds.describe_db_instances.return_value = {
            "DBInstances": [{
                "DBInstanceClass": SMALL_DB_SIZE
            }]
        }
        with self.assertRaises(Exception) as context:
            event = {"resize_action": "SHRINK"}
            db_resize_handler.disable_trigger_before_resize(event, {})
        mock_rds.describe_db_instances.assert_called_once_with(
            DBInstanceIdentifier=DEFAULT_DB_INSTANCE_IDENTIFIER)
        mock_utils.assert_not_called()

    @mock.patch('src.db_resize_handler.rds_client')
    @mock.patch('src.handler.disable_triggers')
    def test_disable_trigger_before_resize_exception_unrecognized_resize_action(self, mock_utils, mock_rds):
        os.environ['STAGE'] = 'TEST'
        mock_utils.return_value = True
        mock_rds.describe_db_instances.return_value = {
            "DBInstances": [{
                "DBInstanceClass": SMALL_DB_SIZE
            }]
        }
        with self.assertRaises(Exception) as context:
            event = {"resize_action": "INVALID"}
            db_resize_handler.disable_trigger_before_resize(event, {})
        mock_rds.describe_db_instances.assert_called_once_with(
            DBInstanceIdentifier=DEFAULT_DB_INSTANCE_IDENTIFIER)
        mock_utils.assert_not_called()



    @mock.patch('src.db_resize_handler._is_cluster_available')
    @mock.patch('src.db_resize_handler.rds_client')
    @mock.patch('src.db_resize_handler.enable_triggers')
    def test_enable_trigger_after_resize_ok_shrink(self, mock_enable_trigger, mock_rds, mock_cluster):
        os.environ['STAGE'] = 'TEST'
        mock_enable_trigger.return_value = True
        mock_cluster.return_value = True
        mock_rds.describe_db_instances.return_value = {
            "DBInstances": [{
                "DBInstanceClass": SMALL_DB_SIZE
            }]
        }
        event = {"resize_action": "SHRINK"}
        db_resize_handler.enable_trigger_after_resize(event, {})
        mock_rds.describe_db_instances.assert_called_once_with(
            DBInstanceIdentifier=DEFAULT_DB_INSTANCE_IDENTIFIER)
        mock_enable_trigger.assert_called_once_with(['aqts-capture-trigger-TEST-aqtsCaptureTrigger'])

    @mock.patch('src.db_resize_handler._is_cluster_available')
    @mock.patch('src.db_resize_handler.rds_client')
    @mock.patch('src.db_resize_handler.enable_triggers')
    def test_enable_trigger_after_resize_ok_grow(self, mock_enable_trigger, mock_rds, mock_cluster):
        os.environ['STAGE'] = 'TEST'
        mock_enable_trigger.return_value = True
        mock_cluster.return_value = True
        mock_rds.describe_db_instances.return_value = {
            "DBInstances": [{
                "DBInstanceClass": BIG_DB_SIZE
            }]
        }
        event = {"resize_action": "GROW"}
        db_resize_handler.enable_trigger_after_resize(event, {})
        mock_rds.describe_db_instances.assert_called_once_with(
            DBInstanceIdentifier=DEFAULT_DB_INSTANCE_IDENTIFIER)
        mock_enable_trigger.assert_called_once_with(['aqts-capture-trigger-TEST-aqtsCaptureTrigger'])

    @mock.patch('src.db_resize_handler._is_cluster_available')
    @mock.patch('src.db_resize_handler.rds_client')
    @mock.patch('src.db_resize_handler.enable_triggers')
    def test_enable_trigger_after_resize_exception_grow_not_finished(self, mock_enable_trigger, mock_rds, mock_cluster):
        os.environ['STAGE'] = 'TEST'
        mock_enable_trigger.return_value = True
        mock_cluster.return_value = True
        mock_rds.describe_db_instances.return_value = {
            "DBInstances": [{
                "DBInstanceClass": SMALL_DB_SIZE
            }]
        }
        event = {"resize_action": "GROW"}
        with self.assertRaises(Exception) as context:
            db_resize_handler.enable_trigger_after_resize(event, {})
        mock_rds.describe_db_instances.assert_called_once_with(DBInstanceIdentifier='nwcapture-test-instance1')
        mock_enable_trigger.assert_not_called()

    @mock.patch('src.db_resize_handler._is_cluster_available')
    @mock.patch('src.db_resize_handler.rds_client')
    @mock.patch('src.db_resize_handler.enable_triggers')
    def test_enable_trigger_after_resize_exception_shrink_not_finished(self, mock_enable_trigger, mock_rds, mock_cluster):
        os.environ['STAGE'] = 'TEST'
        mock_enable_trigger.return_value = True
        mock_cluster.return_value = True
        mock_rds.describe_db_instances.return_value = {
            "DBInstances": [{
                "DBInstanceClass": BIG_DB_SIZE
            }]
        }
        event = {"resize_action": "SHRINK"}
        with self.assertRaises(Exception) as context:
            db_resize_handler.enable_trigger_after_resize(event, {})
        mock_rds.describe_db_instances.assert_called_once_with(DBInstanceIdentifier='nwcapture-test-instance1')
        mock_enable_trigger.assert_not_called()

    @mock.patch('src.db_resize_handler._is_cluster_available')
    @mock.patch('src.db_resize_handler.rds_client')
    @mock.patch('src.db_resize_handler.enable_triggers')
    def test_enable_trigger_after_resize_exception_cluster_not_ready(self, mock_enable_trigger, mock_rds,
                                                                       mock_cluster):
        os.environ['STAGE'] = 'TEST'
        mock_enable_trigger.return_value = True
        mock_cluster.side_effect = Exception("cluster not ready")
        mock_rds.describe_db_instances.return_value = {
            "DBInstances": [{
                "DBInstanceClass": BIG_DB_SIZE
            }]
        }
        event = {"resize_action": "GROW"}
        with self.assertRaises(Exception) as context:
            db_resize_handler.enable_trigger_after_resize(event, {})
        mock_rds.describe_db_instances.assert_called_once_with(DBInstanceIdentifier='nwcapture-test-instance1')
        mock_enable_trigger.assert_not_called()

    @mock.patch('src.db_resize_handler._is_cluster_available')
    @mock.patch('src.db_resize_handler.rds_client')
    @mock.patch('src.db_resize_handler.enable_triggers')
    def test_enable_trigger_after_resize_exception_resize_none(self, mock_enable_trigger, mock_rds, mock_cluster):
        os.environ['STAGE'] = 'TEST'
        mock_enable_trigger.return_value = True
        mock_cluster.return_value = True
        mock_rds.describe_db_instances.return_value = {
            "DBInstances": [{
                "DBInstanceClass": SMALL_DB_SIZE
            }]
        }
        event = {"resize_action": None}
        with self.assertRaises(Exception) as context:
            db_resize_handler.enable_trigger_after_resize(event, {})
        mock_rds.describe_db_instances.assert_not_called()
        mock_enable_trigger.assert_not_called()

    @mock.patch('src.db_resize_handler.rds_client')
    @mock.patch('src.handler.enable_triggers')
    def test_enable_trigger_after_resize_invalid(self, mock_utils, mock_rds):
        os.environ['STAGE'] = 'TEST'
        mock_utils.return_value = True
        mock_rds.describe_db_instances.return_value = {
            "DBInstances": [{
                "DBInstanceClass": BIG_DB_SIZE
            }]
        }
        with self.assertRaises(Exception) as context:
            event = {"resize_action": "INVALID"}
            db_resize_handler.enable_trigger_after_resize(event, {})
        mock_rds.describe_db_instances.assert_called_once_with(
            DBInstanceIdentifier=DEFAULT_DB_INSTANCE_IDENTIFIER)
        mock_utils.assert_not_called()


    @mock.patch('src.db_resize_handler.cloudwatch_client')
    @mock.patch('src.db_resize_handler.rds_client')
    @mock.patch('src.handler.disable_triggers')
    def test_shrink_db(self, mock_utils, mock_rds, mock_cloudwatch):
        os.environ['STAGE'] = 'TEST'
        os.environ['SHRINK_THRESHOLD'] = '10'
        os.environ['SHRINK_EVAL_TIME_IN_SECONDS'] = '3600'
        mock_utils.return_value = True
        mock_rds.describe_db_instances.return_value = {
            "DBInstances": [{
                "DBInstanceClass": BIG_DB_SIZE
            }]
        }
        db_resize_handler.shrink_db({}, {})
        mock_rds.modify_db_instance.assert_called_once_with(
            DBInstanceIdentifier=DEFAULT_DB_INSTANCE_IDENTIFIER,
            DBInstanceClass=SMALL_DB_SIZE,
            ApplyImmediately=True)

    @mock.patch('src.db_resize_handler.cloudwatch_client')
    @mock.patch('src.db_resize_handler.rds_client')
    @mock.patch('src.db_resize_handler.disable_triggers')
    def test_grow_db(self, mock_utils, mock_rds, mock_cloudwatch):
        os.environ['STAGE'] = 'TEST'
        mock_utils.return_value = True
        mock_rds.describe_db_instances.return_value = {
            "DBInstances": [{
                "DBInstanceClass": SMALL_DB_SIZE
            }]
        }
        db_resize_handler.grow_db({}, {})
        mock_rds.modify_db_instance.assert_called_once_with(
            DBInstanceIdentifier=DEFAULT_DB_INSTANCE_IDENTIFIER,
            DBInstanceClass=BIG_DB_SIZE,
            ApplyImmediately=True)

    def test_validate_okay(self):
        os.environ['STAGE'] = 'QA'
        db_resize_handler._validate()
        # If no exception is thrown, we passed

    def test_validate_not_okay(self):
        os.environ['STAGE'] = 'TEST'
        with self.assertRaises(Exception) as context:
            db_resize_handler._validate()
        