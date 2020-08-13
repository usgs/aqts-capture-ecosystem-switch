import logging

from src.utils import enable_trigger, describe_db_clusters, start_db_cluster, disable_trigger, stop_db_cluster

TEST_DB = 'nwcapture-test'
QA_SB = 'nwcapture-qa'
TEST_TRIGGER = 'aqts-capture-trigger-TEST-aqtsCaptureTrigger'
QA_TRIGGER = 'aqts-capture-trigger-QA-aqtsCaptureTrigger'


def start_test_db(event, context):
    logging.info("enter start_test_db")
    enable_trigger(TEST_TRIGGER)
    clusters = describe_db_clusters("start")
    started = False
    for cluster in clusters['DBClusters']:
        if cluster['DBClusterIdentifier'] == TEST_DB:
            logging.info(f"going to start cluster {TEST_DB}")
            start_db_cluster(TEST_DB)
            started = True
    return {
        'statusCode': 200,
        'message': f"Started the test db: {started}"
    }


def stop_test_db(event, context):
    logging.info("enter stop_test_db")
    result = disable_trigger(TEST_TRIGGER)
    logging.info(f"ran disable_trigger {result}")
    clusters = describe_db_clusters("stop")
    logging.info(f"ran describe_db_clusters {len(clusters['DBClusters'])}")

    stopped = False
    for cluster in clusters['DBClusters']:
        if cluster['DBClusterIdentifier'] == TEST_DB:
            logging.info(f"going to stop cluster {TEST_DB}")
            stop_db_cluster(TEST_DB)
            stopped = True
    return {
        'statusCode': 200,
        'message': f"Stopped the test db: {stopped}"
    }


def start_qa_db(event, context):
    enable_trigger(QA_TRIGGER)
    clusters = describe_db_clusters("start")
    for i in range(0, len(clusters)):
        if clusters[i] == TEST_DB:
            start_db_cluster(clusters[i])


def stop_qa_db(event, context):
    disable_trigger(QA_TRIGGER)
    clusters = describe_db_clusters("stop")
    for i in range(0, len(clusters)):
        if clusters[i] == TEST_DB:
            stop_db_cluster(clusters[i])
