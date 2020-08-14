from src.utils import enable_trigger, describe_db_clusters, start_db_cluster, disable_trigger, stop_db_cluster

TEST_DB = 'nwcapture-test'
QA_DB = 'nwcapture-qa'
TEST_TRIGGER = 'aqts-capture-trigger-TEST-aqtsCaptureTrigger'
QA_TRIGGER = 'aqts-capture-trigger-QA-aqtsCaptureTrigger'


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
    cluster_identifiers = describe_db_clusters("start")
    started = False
    for cluster_identifier in cluster_identifiers:
        if cluster_identifier == db:
            start_db_cluster(db)
            started = True
            enable_trigger(trigger)
    return started


def stop_db(db, trigger):
    cluster_identifiers = describe_db_clusters("stop")
    stopped = False
    for cluster_identifier in cluster_identifiers:
        if cluster_identifier == db:
            result = disable_trigger(trigger)
            stop_db_cluster(db)
            stopped = True
    return stopped
