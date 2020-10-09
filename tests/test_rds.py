import unittest
from unittest import TestCase, mock

from src.rds import RDS


class TestRDS(TestCase):

    def setUp(self):
        self.host = 'some-host'
        self.database = 'some-database'
        self.user = 'some-user'
        self.password = 'some-password'
        self.config = {
            'rds': {
                'host': self.host,
                'database': self.database,
                'user': self.user,
                'password': self.password
            }
        }

    @mock.patch('src.rds.connect')
    def test_db_connect(self, mock_connection):
        mock_connection.return_value.cursor.return_value = mock.Mock()
        with mock.patch.dict('src.config.CONFIG', self.config):
            RDS()
            mock_connection.assert_called_with(
                host='some-host',
                database='some-database',
                user='some-user',
                password='some-password',
                connect_timeout=65
            )


if __name__ == '__main__':
    unittest.main()
