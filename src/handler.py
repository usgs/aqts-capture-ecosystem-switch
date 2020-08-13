import logging

from src.utils import enable_trigger, describe_db_clusters, start_db_cluster, disable_trigger, stop_db_cluster

TEST_DB = 'nwcapture-test'
QA_DB = 'nwcapture-qa'
TEST_TRIGGER = 'aqts-capture-trigger-TEST-aqtsCaptureTrigger'
QA_TRIGGER = 'aqts-capture-trigger-QA-aqtsCaptureTrigger'

log = logging.getLogger()
log.setLevel(logging.DEBUG)


def start_test_db(event, context):
    started = start_db(TEST_DB, TEST_TRIGGER)
    return {
        'statusCode': 200,
        'message': f"Started the test db: {started}"
    }


def start_qa_db(event, context):
    started = start_db(QA_DB, QA_TRIGGER)
    return {
        'statusCode': 200,
        'message': f"Started the qa db: {started}"
    }


def stop_test_db(event, context):
    stopped = stop_db(TEST_DB, TEST_TRIGGER)
    return {
        'statusCode': 200,
        'message': f"Stopped the test db: {stopped}"
    }


def stop_qa_db(event, context):
    stopped = stop_db(QA_DB, QA_TRIGGER)
    return {
        'statusCode': 200,
        'message': f"Stopped the qa db: {stopped}"
    }


def start_db(db, trigger):
    print(f"enter start_db {db} {trigger}")
    cluster_identifiers = describe_db_clusters("start")
    print(f"ran describe_db_clusters {cluster_identifiers}")
    started = False
    for cluster_identifier in cluster_identifiers:
        if cluster_identifier == db:
            print(f"going to start cluster {db}")
            start_db_cluster(db)
            started = True
            print(f"going to enable trigger {trigger}")
            enable_trigger(trigger)
    return started


def stop_db(db, trigger):
    print("enter stop_db")
    cluster_identifiers = describe_db_clusters("stop")
    print(f"ran describe_db_clusters {cluster_identifiers}")

    stopped = False
    for cluster_identifier in cluster_identifiers:
        if cluster_identifier == db:
            print(f"going to disable trigger {trigger}")
            result = disable_trigger(trigger)
            print(f"going to stop cluster {db}")
            stop_db_cluster(db)
            stopped = True
    return stopped
