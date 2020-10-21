import os
from unittest import TestCase, mock

from src import db_resizer
from src.db_resizer import SMALL_DB_SIZE, BIG_DB_SIZE
from src.handler import TRIGGER, DEFAULT_DB_INSTANCE_IDENTIFIER


class TestDbResizer(TestCase):

    def setUp(self):
        pass

    @mock.patch('src.db_resizer.rds_client')
    @mock.patch('src.handler.disable_triggers')
    def test_disable_trigger_before_shrink_exception_already_shrunk(self, mock_utils, mock_rds):
        os.environ['STAGE'] = 'TEST'
        mock_utils.return_value = True
        mock_rds.describe_db_instances.return_value = {
            "DBInstances": [{
                "DBInstanceClass": SMALL_DB_SIZE
            }]
        }
        with self.assertRaises(Exception) as context:
            db_resizer.disable_trigger_before_shrink({}, {})
        mock_rds.describe_db_instances.assert_called_once_with(
            DBInstanceIdentifier=DEFAULT_DB_INSTANCE_IDENTIFIER)
        mock_utils.assert_not_called()

    @mock.patch('src.db_resizer.rds_client')
    @mock.patch('src.db_resizer.disable_triggers')
    def test_disable_trigger_before_shrink_exception_ok(self, mock_utils, mock_rds):
        os.environ['STAGE'] = 'TEST'
        mock_utils.return_value = True
        mock_rds.describe_db_instances.return_value = {
            "DBInstances": [{
                "DBInstanceClass": BIG_DB_SIZE
            }]
        }
        db_resizer.disable_trigger_before_shrink({}, {})
        mock_rds.describe_db_instances.assert_called_once_with(
            DBInstanceIdentifier=DEFAULT_DB_INSTANCE_IDENTIFIER)
        mock_utils.assert_called_once_with(TRIGGER['TEST'])

    @mock.patch('src.db_resizer.rds_client')
    @mock.patch('src.handler.disable_triggers')
    def test_disable_trigger_before_grow_exception_already_grown(self, mock_utils, mock_rds):
        os.environ['STAGE'] = 'TEST'
        mock_utils.return_value = True
        mock_rds.describe_db_instances.return_value = {
            "DBInstances": [{
                "DBInstanceClass": BIG_DB_SIZE
            }]
        }
        with self.assertRaises(Exception) as context:
            db_resizer.disable_trigger_before_grow({}, {})
        mock_rds.describe_db_instances.assert_called_once_with(
            DBInstanceIdentifier=DEFAULT_DB_INSTANCE_IDENTIFIER)
        mock_utils.assert_not_called()

    @mock.patch('src.db_resizer.rds_client')
    @mock.patch('src.db_resizer.disable_triggers')
    def test_disable_trigger_before_grow_exception_ok(self, mock_utils, mock_rds):
        os.environ['STAGE'] = 'TEST'
        mock_utils.return_value = True
        mock_rds.describe_db_instances.return_value = {
            "DBInstances": [{
                "DBInstanceClass": SMALL_DB_SIZE
            }]
        }
        db_resizer.disable_trigger_before_grow({}, {})
        mock_rds.describe_db_instances.assert_called_once_with(
            DBInstanceIdentifier=DEFAULT_DB_INSTANCE_IDENTIFIER)
        mock_utils.assert_called_once_with(TRIGGER['TEST'])

    @mock.patch('src.db_resizer.cloudwatch_client')
    @mock.patch('src.db_resizer.rds_client')
    @mock.patch('src.handler.disable_triggers')
    def test_shrink_db(self, mock_utils, mock_rds, mock_cloudwatch):
        os.environ['STAGE'] = 'TEST'
        mock_utils.return_value = True
        mock_rds.describe_db_instances.return_value = {
            "DBInstances": [{
                "DBInstanceClass": BIG_DB_SIZE
            }]
        }
        db_resizer.shrink_db({}, {})
        mock_rds.modify_db_instance.assert_called_once_with(
            DBInstanceIdentifier=DEFAULT_DB_INSTANCE_IDENTIFIER,
            DBInstanceClass=SMALL_DB_SIZE,
            ApplyImmediately=True)

    @mock.patch('src.db_resizer.cloudwatch_client')
    @mock.patch('src.db_resizer.rds_client')
    @mock.patch('src.db_resizer.disable_triggers')
    def test_grow_db(self, mock_utils, mock_rds, mock_cloudwatch):
        os.environ['STAGE'] = 'TEST'
        mock_utils.return_value = True
        mock_rds.describe_db_instances.return_value = {
            "DBInstances": [{
                "DBInstanceClass": SMALL_DB_SIZE
            }]
        }
        db_resizer.grow_db({}, {})
        mock_rds.modify_db_instance.assert_called_once_with(
            DBInstanceIdentifier=DEFAULT_DB_INSTANCE_IDENTIFIER,
            DBInstanceClass=BIG_DB_SIZE,
            ApplyImmediately=True)
