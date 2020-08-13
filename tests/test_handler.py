from unittest import TestCase, mock
from src import handler


class TestHandlerHandler(TestCase):
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
                'AllocatedStorage': 123,
                'AvailabilityZones': [
                    'string',
                ],
                'BackupRetentionPeriod': 123,
                'CharacterSetName': 'string',
                'DatabaseName': 'string',
                'DBClusterIdentifier': 'string',
                'DBClusterParameterGroup': 'string',
                'DBSubnetGroup': 'string',
                'Status': 'string',
                'PercentProgress': 'string',
                'EarliestRestorableTime': '',
                'Endpoint': 'string',
                'ReaderEndpoint': 'string',
                'CustomEndpoints': [
                    'string',
                ],
                'MultiAZ': True,
                'Engine': 'string',
                'EngineVersion': 'string',
                'LatestRestorableTime': '',
                'Port': 123,
                'MasterUsername': 'string',
                'DBClusterOptionGroupMemberships': [
                    {
                        'DBClusterOptionGroupName': 'string',
                        'Status': 'string'
                    },
                ],
                'PreferredBackupWindow': 'string',
                'PreferredMaintenanceWindow': 'string',
                'ReplicationSourceIdentifier': 'string',
                'ReadReplicaIdentifiers': [
                    'string',
                ],
                'DBClusterMembers': [
                    {
                        'DBInstanceIdentifier': 'string',
                        'IsClusterWriter': True,
                        'DBClusterParameterGroupStatus': 'string',
                        'PromotionTier': 123
                    },
                ],
                'VpcSecurityGroups': [
                    {
                        'VpcSecurityGroupId': 'string',
                        'Status': 'string'
                    },
                ],
                'HostedZoneId': 'string',
                'StorageEncrypted': True,
                'KmsKeyId': 'string',
                'DbClusterResourceId': 'string',
                'DBClusterArn': 'string',
                'AssociatedRoles': [
                    {
                        'RoleArn': 'string',
                        'Status': 'string',
                        'FeatureName': 'string'
                    },
                ],
                'IAMDatabaseAuthenticationEnabled': True,
                'CloneGroupId': 'string',
                'ClusterCreateTime': '',
                'EarliestBacktrackTime': '',
                'BacktrackWindow': 123,
                'BacktrackConsumedChangeRecords': 123,
                'EnabledCloudwatchLogsExports': [
                    'string',
                ],
                'Capacity': 123,
                'EngineMode': 'string',
                'ScalingConfigurationInfo': {
                    'MinCapacity': 123,
                    'MaxCapacity': 123,
                    'AutoPause': True,
                    'SecondsUntilAutoPause': 123,
                    'TimeoutAction': 'string'
                },
                'DeletionProtection': True,
                'HttpEndpointEnabled': True,
                'ActivityStreamMode': 'sync',
                'ActivityStreamStatus': 'stopped',
                'ActivityStreamKmsKeyId': 'string',
                'ActivityStreamKinesisStreamName': 'string',
                'CopyTagsToSnapshot': True,
                'CrossAccountClone': True,
                'DomainMemberships': [
                    {
                        'Domain': 'string',
                        'Status': 'string',
                        'FQDN': 'string',
                        'IAMRoleName': 'string'
                    },
                ],
                'GlobalWriteForwardingStatus': 'enabled',
                'GlobalWriteForwardingRequested': True
            },
        ]
    }

    mock_event_source_mapping = {
        'NextMarker': 'string',
        'EventSourceMappings': [
            {
                'UUID': 'string',
                'BatchSize': 123,
                'MaximumBatchingWindowInSeconds': 123,
                'ParallelizationFactor': 123,
                'EventSourceArn': 'string',
                'FunctionArn': 'string',
                'LastModified': '',
                'LastProcessingResult': 'string',
                'State': 'string',
                'StateTransitionReason': 'string',
                'DestinationConfig': {
                    'OnSuccess': {
                        'Destination': 'string'
                    },
                    'OnFailure': {
                        'Destination': 'string'
                    }
                },
                'Topics': [
                    'string',
                ],
                'MaximumRecordAgeInSeconds': 123,
                'BisectBatchOnFunctionError': True,
                'MaximumRetryAttempts': 123
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
    @mock.patch('src.handler.enable_trigger')
    @mock.patch('src.handler.describe_db_clusters')
    def test_start_test_db_nothing_to_start(self, mock_et, mock_ddc):
        mock_et.return_value = self.mock_db_clusters
        mock_et.return_value['DBClusters'][0]['DBClusterIdentifier'] = 'string'
        mock_ddc.return_value = False
        result = handler.start_test_db(self.initial_event, self.context)
        assert result['statusCode'] == 200
        assert result['message'] == 'Started the test db: False'

    @mock.patch.dict('src.utils.os.environ', mock_env_vars)
    @mock.patch('src.handler.describe_db_clusters')
    @mock.patch('src.handler.enable_trigger')
    @mock.patch('src.handler.start_db_cluster')
    def test_start_test_db_something_to_start(self, mock_ddc, mock_et, mock_sdc):
        mock_ddc.return_value = True
        mock_et.return_value = True
        mock_sdc.return_value = self.mock_db_cluster_identifiers

        result = handler.start_test_db(self.initial_event, self.context)
        mock_et.assert_called_with(
            'aqts-capture-trigger-TEST-aqtsCaptureTrigger'
        )
        assert result['statusCode'] == 200
        assert result['message'] == 'Started the test db: True'

    @mock.patch.dict('src.utils.os.environ', mock_env_vars)
    @mock.patch('src.handler.disable_trigger')
    @mock.patch('src.handler.describe_db_clusters')
    def test_stop_test_db_nothing_to_stop(self, mock_dt, mock_ddc):
        mock_dt.return_value = {}
        mock_ddc.return_value = False
        result = handler.stop_test_db(self.initial_event, self.context)
        assert result['statusCode'] == 200
        assert result['message'] == 'Stopped the test db: False'

    @mock.patch.dict('src.utils.os.environ', mock_env_vars)
    @mock.patch('src.handler.describe_db_clusters')
    @mock.patch('src.handler.disable_trigger')
    @mock.patch('src.handler.stop_db_cluster')
    def test_stop_test_db_something_to_stop(self, mock_ddc, mock_dt, mock_sdc):
        mock_ddc.return_value = True
        mock_dt.return_value = True
        mock_sdc.return_value = self.mock_db_cluster_identifiers
        result = handler.stop_test_db(self.initial_event, self.context)
        mock_dt.assert_called_with(
            'aqts-capture-trigger-TEST-aqtsCaptureTrigger'
        )
        assert result['statusCode'] == 200
        assert result['message'] == 'Stopped the test db: True'
