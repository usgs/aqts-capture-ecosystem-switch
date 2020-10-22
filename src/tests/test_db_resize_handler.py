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
