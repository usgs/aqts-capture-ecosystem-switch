import logging

from src.utils import enable_trigger, describe_db_clusters, start_db_cluster, disable_trigger, stop_db_cluster

TEST_DB = 'nwcapture-test'
QA_DB = 'nwcapture-qa'
TEST_TRIGGER = 'aqts-capture-trigger-TEST-aqtsCaptureTrigger'
QA_TRIGGER = 'aqts-capture-trigger-QA-aqtsCaptureTrigger'

log = logging.getLogger()
log.setLevel(logging.DEBUG)


def start_test_db(event, context):
    print("enter start_test_db")
    cluster_identifiers = describe_db_clusters("start")
    print(f"ran describe_db_clusters {cluster_identifiers}")
    started = False
    for cluster_identifier in cluster_identifiers:
        if cluster_identifier == TEST_DB:
            log.debug(f"going to start cluster {TEST_DB}")
            start_db_cluster(TEST_DB)
            started = True
            enable_trigger(TEST_TRIGGER)

    return {
        'statusCode': 200,
        'message': f"Started the test db: {started}"
    }


def stop_test_db(event, context):
    print("enter stop_test_db")
    cluster_identifiers = describe_db_clusters("stop")
    print(f"ran describe_db_clusters {cluster_identifiers}")

    stopped = False
    for cluster_identifier in cluster_identifiers:
        if cluster_identifier == TEST_DB:

            result = disable_trigger(TEST_TRIGGER)
            print(f"ran disable_trigger {result}")
            print(f"going to stop cluster {TEST_DB}")
            stop_db_cluster(TEST_DB)
            stopped = True
    return {
        'statusCode': 200,
        'message': f"Stopped the test db: {stopped}"
    }


def start_qa_db(event, context):
    enable_trigger(QA_TRIGGER)
    cluster_identifiers = describe_db_clusters("start")
    for cluster_identifier in cluster_identifiers:
        if cluster_identifier == QA_DB:
            pass
            # start_db_cluster(clusters[i])


def stop_qa_db(event, context):
    disable_trigger(QA_TRIGGER)
    cluster_identifiers = describe_db_clusters("stop")
    for cluster_identifier in cluster_identifiers:
        if cluster_identifier == QA_DB:
            pass
            # stop_db_cluster(clusters[i])
