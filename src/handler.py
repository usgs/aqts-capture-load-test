import json
import os
import boto3
import datetime
import logging

from src.rds import RDS

"""
As of right now, the plan is to always deploy and run on QA.  However,
if in the future that changes, use the 'stage' variable to update everything
so it will automatically work on other stages.
"""
stage = os.getenv('STAGE', 'QA')

log_level = os.getenv('LOG_LEVEL', logging.ERROR)
logger = logging.getLogger()
logger.setLevel(log_level)

DB = {
    "TEST": 'nwcapture-test',
    "QA": 'nwcapture-qa',
    "LOAD": 'nwcapture-load'
}

CAPTURE_TRIGGER = 'aqts-capture-trigger-QA-aqtsCaptureTrigger'

"""
This is supposed to be all lambda functions invoked by the state machine.
"""
LAMBDA_FUNCTIONS = [
    f"aqts-capture-raw-load-{stage}-iowCapture",
    f"aqts-ts-type-router-{stage}-determineRoute",
    f"aqts-capture-ts-description-{stage}-processTsDescription",
    f"aqts-capture-ts-corrected-{stage}-preProcess",
    f"aqts-capture-ts-field-visit-{stage}-preProcess",
    f"aqts-capture-field-visit-metadata-{stage}-preProcess",
    f"aqts-capture-field-visit-transform-{stage}-transform",
    f"aqts-capture-discrete-loader-{stage}-loadDiscrete",
    f"aqts-capture-dvstat-transform-{stage}-transform",
    f"aqts-capture-ts-loader-{stage}-loadTimeSeries",
    f"aqts-capture-error-handler-{stage}-aqtsErrorHandler"
]

# Default snapshot identifier, may be overridden by passing a custom
# snapshot identifier in the step function event
two_days_ago = datetime.datetime.now() - datetime.timedelta(2)
month = str(two_days_ago.month)
if len(month) == 1:
    month = f"0{month}"
day = str(two_days_ago.day)
if len(day) == 1:
    day = f"0{day}"
SNAPSHOT_IDENTIFIER = f"rds:nwcapture-prod-external-{two_days_ago.year}-{month}-{day}-10-08"

"""
DB Info
"""
DB_INSTANCE_IDENTIFIER = 'nwcapture-load-instance1'
DB_INSTANCE_CLASS = 'db.r5.8xlarge'
ENGINE = 'aurora-postgresql'
DB_CLUSTER_IDENTIFIER = 'nwcapture-load'
NWCAPTURE_REAL = f"NWCAPTURE-DB-{stage}"
NWCAPTURE_LOAD = 'NWCAPTURE-DB-LOAD'

"""
SQS Queues
"""
CAPTURE_TRIGGER_QUEUE = f"aqts-capture-trigger-queue-{stage}"
ERROR_QUEUE = f"aqts-capture-error-queue-{stage}"

"""
Buckets
"""
TEST_BUCKET = 'iow-retriever-capture-load'
SRC_BUCKET = 'iow-retriever-capture-reference'
REAL_BUCKET = f"iow-retriever-capture-{stage.lower()}"

"""
Boto clients
"""
secrets_client = boto3.client('secretsmanager', os.environ['AWS_DEPLOYMENT_REGION'])
rds_client = boto3.client('rds', os.environ['AWS_DEPLOYMENT_REGION'])
lambda_client = boto3.client('lambda', os.getenv('AWS_DEPLOYMENT_REGION'))
sqs_client = boto3.client('sqs', os.getenv('AWS_DEPLOYMENT_REGION'))
s3_client = boto3.client('s3', os.getenv('AWS_DEPLOYMENT_REGION'))
cloudwatch_client = boto3.client('cloudwatch', os.getenv('AWS_DEPLOYMENT_REGION', 'us-west-2'))
s3 = boto3.resource('s3', os.getenv('AWS_DEPLOYMENT_REGION'))

ALARMS = {
    f"aqts-capture-dvstat-transform-{stage}-error-alarm",
    f"aqts-capture-error-handler-{stage}-error-alarm",
    f"aqts-capture-field-visit-transform-{stage}-error-alarm",
    f"aqts-capture-field-visit-metadata-{stage}-error-alarm",
    f"aqts-capture-raw-load-{stage}-error-alarm",
    f"aqts-capture-raw-load-medium-{stage}-error-alarm",
    f"aqts-capture-state-machine-{stage}-duration-alarm",
    f"aqts-capture-trigger-{stage}-error-alarm",
    f"aqts-capture-ts-corrected-{stage}-error-alarm",
    f"aqts-capture-ts-description-{stage}-error-alarm",
    f"aqts-capture-ts-field-visit-{stage}-error-alarm",
    f"aqts-capture-ts-loader-{stage}-error-alarm"
}


def delete_db_cluster(event, context):
    logger.info(event)
    rds_client.delete_db_cluster(
        DBClusterIdentifier=DB_CLUSTER_IDENTIFIER,
        SkipFinalSnapshot=True
    )


def modify_db_cluster(event, context):
    """
    When we restore the database from a production snapshot,
    we don't know the passwords.  So, modify the postgres password here
    so we can work with the database.
    :param event:
    :param context:
    :return:
    """
    logger.info(event)
    rds_client.modify_db_cluster(
        DBClusterIdentifier=DB_CLUSTER_IDENTIFIER,
        ApplyImmediately=True,
        MasterUserPassword='Password123'
    )


def delete_db_instance(event, context):
    logger.info(event)
    rds_client.delete_db_instance(
        DBInstanceIdentifier=DB_INSTANCE_IDENTIFIER,
        SkipFinalSnapshot=True
    )


def create_db_instance(event, context):
    logger.info(event)
    rds_client.create_db_instance(
        DBInstanceIdentifier=DB_INSTANCE_IDENTIFIER,
        DBInstanceClass=DB_INSTANCE_CLASS,
        DBClusterIdentifier=DB_CLUSTER_IDENTIFIER,
        Engine=ENGINE
    )


def copy_s3(event, context):
    logger.info(event)
    """
    Copy files from the 'reference' bucket to the trigger bucket to simulate
    a full run.
    :param event:
    :param context:
    :return:
    """
    logger.info(f"about to copy from SRC_BUCKET {SRC_BUCKET} to TEST_BUCKET {TEST_BUCKET}")
    resp = s3_client.list_objects_v2(Bucket=SRC_BUCKET)
    keys = []
    for obj in resp['Contents']:
        keys.append(obj['Key'])

    s3_resource = boto3.resource('s3')
    for key in keys:
        copy_source = {
            'Bucket': SRC_BUCKET,
            'Key': key
        }
        logger.info(f"key = {key}")
        bucket = s3_resource.Bucket(TEST_BUCKET)
        bucket.copy(copy_source, key)


def restore_db_cluster(event, context):
    logger.info(event)
    """
    By default we try to restore the production snapshot that
    is two days old.  If a specific snapshot needs to be used
    for the test, it can be passed in as part of an event when
    the step function is invoked with the key 'snapshotIdentifier'.

    Restoring an aurora db cluster from snapshot takes one to two hours.
    """

    original = secrets_client.get_secret_value(
        SecretId=NWCAPTURE_REAL
    )
    secret_string = json.loads(original['SecretString'])
    kms_key = str(secret_string['KMS_KEY_ID'])
    subnet_name = str(secret_string['DB_SUBGROUP_NAME'])
    vpc_security_group_id = str(secret_string['VPC_SECURITY_GROUP_ID'])
    if not kms_key or not subnet_name or not vpc_security_group_id:
        raise Exception(f"Missing db configuration data {secret_string}")
    my_snapshot_identifier = SNAPSHOT_IDENTIFIER
    if event is not None:
        if event.get("snapshotIdentifier") is not None:
            my_snapshot_identifier = event.get("snapshotIdentifier")

    rds_client.restore_db_cluster_from_snapshot(
        DBClusterIdentifier=DB_CLUSTER_IDENTIFIER,
        SnapshotIdentifier=my_snapshot_identifier,
        Engine=ENGINE,
        EngineVersion='11.7',
        Port=5432,
        DBSubnetGroupName=subnet_name,
        DatabaseName='nwcapture-load',
        EnableIAMDatabaseAuthentication=False,
        EngineMode='provisioned',
        DBClusterParameterGroupName='aqts-capture',
        DeletionProtection=False,
        CopyTagsToSnapshot=False,
        KmsKeyId=kms_key,
        VpcSecurityGroupIds=[
            vpc_security_group_id
        ],
        Tags=[
            {
                'Key': 'Name',
                'Value': 'NWISWEB-CAPTURE-RDS-AURORA-LOAD-TEST'
            },
            {
                'Key': 'wma:organization',
                'Value': 'IOW'
            },
            {
                'Key': 'wma:role',
                'Value': 'etl'
            },
            {
                'Key': 'wma:system',
                'Value': 'NWIS'
            },
            {
                'Key': 'wma:subSystem',
                'Value': 'NWISWeb - Capture'
            }
        ]
    )


def enable_trigger(event, context):
    logger.info(event)
    """
    Enable the trigger on the bucket (after test, restoring things to normal)
    if the real db is on.
    :param event:
    :param context:
    :return:
    """
    active_dbs = _describe_db_clusters('stop')
    logger.info(f"active_dbs {active_dbs}")
    if DB["LOAD"] in active_dbs:
        logger.info(f"DB {DB['LOAD']} Active, going to enable trigger")
        response = lambda_client.list_event_source_mappings(FunctionName=CAPTURE_TRIGGER)
        logger.info(f"Response from listing event source mappings trigger queue {response}")
        if len(response['EventSourceMappings']) == 0:
            raise Exception(f"Event Source Mappings are empty for trigger queue {response}")
        for item in response['EventSourceMappings']:
            lambda_client.update_event_source_mapping(UUID=item['UUID'], Enabled=True)
    else:
        raise Exception("nwcapture-load db was off")


def disable_trigger_if_real_db_is_off(event, context):
    logger.info(event)
    """
    We are cleaning up the test here.  The load test db is being deleted, and we need to restore
    the real db to the correct state.  If the db is off, we want the trigger to be disabled.  If
    it's on, we assume it should be enabled.
    """
    active_dbs = _describe_db_clusters('stop')
    logger.info(f"active_dbs {active_dbs}")
    set_enabled = False
    if DB[stage] in active_dbs:
        set_enabled = True
    response = lambda_client.list_event_source_mappings(FunctionName=CAPTURE_TRIGGER)
    if len(response['EventSourceMappings']) == 0:
        raise Exception(f"Event Source Mappings are empty for trigger queue {response}")
    for item in response['EventSourceMappings']:
        lambda_client.update_event_source_mapping(UUID=item['UUID'], Enabled=set_enabled)


def add_notification_to_test_bucket(event, context):
    logger.info(event)
    response = _add_notification_to_bucket(TEST_BUCKET)
    logger.info(f"test bucket response {response}")
    response = _remove_notification_from_bucket(REAL_BUCKET)
    logger.info(f"real bucket response {response}")


def wait_for_test_to_finish(event, context):
    response = cloudwatch_client.get_metric_data(
        MetricDataQueries=[
            {
                'Id': 'loadTestCpuUtilization',
                'MetricStat': {
                    'Metric': {
                        'Namespace': 'AWS/RDS',
                        'MetricName': 'CPUUtilization',
                        'Dimensions': [
                            {
                                "Name": "DBInstanceIdentifier",
                                "Value": DB_INSTANCE_IDENTIFIER
                            }]
                    },
                    'Period': 60,
                    'Stat': 'Maximum',
                }
            }
        ],
        StartTime=(datetime.datetime.now() - datetime.timedelta(seconds=300)).timestamp(),
        EndTime=datetime.datetime.now().timestamp()
    )
    for value in response['MetricDataResults'][0]['Values']:
        if value > 0:
            raise Exception(f"db is still busy at {datetime.datetime.now()}")


def remove_notification_from_test_bucket(event, context):
    logger.info(event)
    response = _remove_notification_from_bucket(TEST_BUCKET)
    logger.info(f"test bucket response {response}")
    response = _add_notification_to_bucket(REAL_BUCKET)
    logger.info(f"real bucket response {response}")


def run_integration_tests(event, context):
    logger.info(event)
    """
    Integration tests will go here.  Right now the idea is that the pre-test
    will save a TEST_RESULT object up in the bucket and that the integration tests will
    write to that same object, so when everything finishes it will be like a report.
    That's just a placeholder idea.
    :param event:
    :param context:
    :return:
    """

    original = secrets_client.get_secret_value(
        SecretId=NWCAPTURE_LOAD,
    )
    secret_string = json.loads(original['SecretString'])
    db_host = secret_string['DATABASE_ADDRESS']
    db_user = secret_string['SCHEMA_OWNER_USERNAME']
    db_name = secret_string['DATABASE_NAME']
    db_password = secret_string['SCHEMA_OWNER_PASSWORD']

    logger.info(f"{db_host} {db_user} {db_name} {db_password}")
    rds = RDS(db_host, db_user, db_name, db_password)
    sql = "select count(1) from capture.json_data"
    result = rds.execute_sql(sql)
    logger.info(f"RESULT: {result}")

    obj = s3.Object('iow-retriever-capture-load', 'TEST_RESULTS')
    logger.info(f"read content from S3: {obj}")
    content = json.loads(obj.get()['Body'].read().decode('utf-8'))
    logger.info(f"after json loads {content}")
    content["End Time"] = str(datetime.datetime.now())
    content["End Count"] = result

    start_date_time_obj = datetime.datetime.strptime(content["StartTime"], '%Y-%m-%d %H:%M:%S.%f')

    elapsed_time = datetime.datetime.now().timestamp() - start_date_time_obj.timestamp()

    for alarm in ALARMS:
        response = _get_cloudwatch_alarm_history(start_date_time_obj, alarm)
        content = _update_results_for_alarm(response, content, alarm)

    content["ElapsedTimeInSeconds"] = elapsed_time

    logger.info(f"Writing this to S3 {json.dumps(content)}")
    s3.Object('iow-retriever-capture-load', 'TEST_RESULTS').put(Body=json.dumps(content))


def _update_results_for_alarm(response, content, alarm):
    is_alarm = False
    history_list = []
    for item in response['AlarmHistoryItems']:
        history_list.append(item['HistorySummary'])
        if "to ALARM" in item['HistorySummary']:
            is_alarm = True
    if is_alarm is True:
        content[alarm] = f"FAIL {history_list}"
    else:
        content[alarm] = f"PASS: {history_list}"
    return content


def pre_test(event, context):
    logger.info(event)
    """
    This is a place holder that will inspect the beginning state of the load test db
    and save some data so that it can be compared with the db after the integration tests run
    :param event:
    :param context:
    :return:
    """
    original = secrets_client.get_secret_value(
        SecretId=NWCAPTURE_LOAD,
    )
    secret_string = json.loads(original['SecretString'])
    logger.info(f"retrieving secrets from {NWCAPTURE_LOAD} {secret_string}")
    db_host = secret_string['DATABASE_ADDRESS']
    db_user = secret_string['SCHEMA_OWNER_USERNAME']
    db_name = secret_string['DATABASE_NAME']
    db_password = secret_string['SCHEMA_OWNER_PASSWORD']
    logger.info(f"db_host= {db_host} db_password= {db_password}")
    rds = RDS(db_host, db_user, db_name, db_password)
    sql = "select count(1) from capture.json_data"
    result = rds.execute_sql(sql)
    logger.info(f"RESULT: {result}")

    content = {"StartTime": str(datetime.datetime.now()), "StartCount": result}
    logger.info(f"Writing this to S3 {json.dumps(content)}")
    s3.Object('iow-retriever-capture-load', 'TEST_RESULTS').put(Body=json.dumps(content))


def falsify_secrets(event, context):
    logger.info(event)
    """
    1. call lambda_client.get_function_configuration() to get original env variables
    2. call secrets_client.get_secret_value() to get the secrets we want to change
    3. replace the env variable for "AQTS_SCHEMA_OWNER_PASSWORD" or "TRANSFORM_SCHEMA_OWNER_PASSWORD"
       with the new password
    4. replace the env variable for "AQTS_DATABASE_ADDRESS" or "TRANSFORM_DATABASE_ADDRESS"
       with the new database address
    5. update the environment variables
    :param event:
    :param context:
    :return:
    """
    _replace_secrets(NWCAPTURE_LOAD)


def restore_secrets(event, context):
    logger.info(event)
    _replace_secrets(NWCAPTURE_REAL)


def modify_schema_owner_password(event, context):
    logger.info(event)
    """
    We don't know the password for 'capture_owner' on the production db,
    but we have already changed the postgres password in the modifyDbCluster step.
    So change the password for 'capture_owner' here.
    :param event:
    :param context:
    :return:
    """
    original = secrets_client.get_secret_value(
        SecretId=NWCAPTURE_LOAD,
    )
    secret_string = json.loads(original['SecretString'])
    db_host = secret_string['DATABASE_ADDRESS']
    db_name = secret_string['DATABASE_NAME']
    rds = RDS(db_host, 'postgres', db_name, 'Password123')
    sql = "alter user capture_owner with password 'Password123'"
    rds.alter_permissions(sql)

    sqs = boto3.client('sqs', os.getenv('AWS_DEPLOYMENT_REGION'))
    queue_info = sqs.get_queue_url(QueueName=CAPTURE_TRIGGER_QUEUE)
    sqs.purge_queue(QueueUrl=queue_info['QueueUrl'])
    queue_info = sqs.get_queue_url(QueueName=ERROR_QUEUE)
    sqs.purge_queue(QueueUrl=queue_info['QueueUrl'])


def _replace_secrets(secret_id):
    original = secrets_client.get_secret_value(
        SecretId=secret_id
    )
    secret_string = json.loads(original['SecretString'])
    db_password = str(secret_string['SCHEMA_OWNER_PASSWORD'])
    db_address = str(secret_string['DATABASE_ADDRESS'])

    for lambda_function in LAMBDA_FUNCTIONS:

        response = lambda_client.get_function_configuration(
            FunctionName=lambda_function
        )
        my_env_variables = response['Environment']['Variables']
        logger.info(f"BEFORE function {lambda_function} my_env_variables= {my_env_variables}")
        if my_env_variables.get("AQTS_SCHEMA_OWNER_PASSWORD") is not None:
            my_env_variables["AQTS_SCHEMA_OWNER_PASSWORD"] = db_password
        elif my_env_variables.get("TRANSFORM_SCHEMA_OWNER_PASSWORD") is not None:
            my_env_variables["TRANSFORM_SCHEMA_OWNER_PASSWORD"] = db_password
        if my_env_variables.get("AQTS_DATABASE_ADDRESS") is not None:
            my_env_variables["AQTS_DATABASE_ADDRESS"] = db_address
        elif my_env_variables.get("TRANSFORM_DATABASE_ADDRESS") is not None:
            my_env_variables["TRANSFORM_DATABASE_ADDRESS"] = db_address
        if my_env_variables.get("DB_PASSWORD") is not None:
            my_env_variables["DB_PASSWORD"] = db_password
        if my_env_variables.get("DB_HOST") is not None:
            my_env_variables["DB_HOST"] = db_address

        logger.info(f"AFTER function {lambda_function} my_env_variables= {my_env_variables}")

        lambda_client.update_function_configuration(
            FunctionName=lambda_function,
            Environment={
                'Variables': my_env_variables
            }
        )


def _describe_db_clusters(action):
    # Get all the instances
    my_rds = boto3.client('rds', os.getenv('AWS_DEPLOYMENT_REGION', 'us-west-2'))
    response = my_rds.describe_db_clusters()
    all_dbs = response['DBClusters']
    if action == "stop":
        # Filter on the ones that are running
        rds_cluster_identifiers = [x['DBClusterIdentifier'] for x in all_dbs if x['Status'] == 'available']
        return rds_cluster_identifiers


def _remove_notification_from_bucket(bucket_name):
    bucket_notification = s3.BucketNotification(bucket_name)
    bucket_notification.load()
    response = bucket_notification.put(
        NotificationConfiguration={
            'QueueConfigurations': [
            ]
        }
    )
    bucket_notification.load()
    return response


def _add_notification_to_bucket(bucket_name):
    bucket_notification = s3.BucketNotification(bucket_name)
    my_queue_url = ""
    response = sqs_client.list_queues()
    for url in response['QueueUrls']:
        if CAPTURE_TRIGGER_QUEUE in url:
            my_queue_url = url
            break
    if len(my_queue_url) == 0:
        raise Exception(f"Couldnt find queue URL for CAPTURE_TRIGGER_QUEUE {CAPTURE_TRIGGER_QUEUE} response {response}")
    response = sqs_client.get_queue_attributes(
        QueueUrl=my_queue_url,
        AttributeNames=['QueueArn']
    )
    my_queue_arn = response['Attributes']['QueueArn']
    response = bucket_notification.put(
        NotificationConfiguration={
            'QueueConfigurations': [
                {
                    'QueueArn': my_queue_arn,
                    'Events': [
                        's3:ObjectCreated:*'
                    ]
                }
            ]
        }
    )
    bucket_notification.load()
    return response


def _get_cloudwatch_alarm_history(start_time, alarm):
    response = cloudwatch_client.describe_alarm_history(
        AlarmName=alarm,
        AlarmTypes=[
            'MetricAlarm',
        ],
        HistoryItemType='StateUpdate',
        StartDate=start_time,
        EndDate=datetime.datetime.now(),
        ScanBy='TimestampDescending'
    )
    return response
