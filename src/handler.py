import json
import os
import boto3
import datetime
import logging

from src.rds import RDS

""" 
As of right now, the plan is to always deploy and run on QA.  However, if in the future that changes, 
use the 'stage' variable to update everything so it will automatically work on other stages.
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

"""
The default snapshot identifier is the production snapshot from two days ago. You can override this by passing in
a different 'snapshotIdentifier' when you launch the state machine.
"""
two_days_ago = datetime.datetime.now() - datetime.timedelta(2)
month = str(two_days_ago.month)
if len(month) == 1:
    month = f"0{month}"
day = str(two_days_ago.day)
if len(day) == 1:
    day = f"0{day}"
SNAPSHOT_IDENTIFIER = f"rds:nwcapture-prod-external-{two_days_ago.year}-{month}-{day}-10-08"

"""
Every aurora cluster requires at least one db instance.
"""
DB_INSTANCE_IDENTIFIER = 'nwcapture-load-instance1'

"""
TODO make this configurable similar to snapshotIdentifier
"""
DB_INSTANCE_CLASS = 'db.r5.8xlarge'

ENGINE = 'aurora-postgresql'

TEST_BUCKET = 'iow-retriever-capture-load'
REAL_BUCKET = f"iow-retriever-capture-{stage.lower()}"
SRC_BUCKET = 'iow-retriever-capture-reference'
DB_CLUSTER_IDENTIFIER = 'nwcapture-load'
NWCAPTURE_REAL = f"NWCAPTURE-DB-{stage}"
NWCAPTURE_LOAD = 'NWCAPTURE-DB-LOAD'
CAPTURE_TRIGGER = f"aqts-capture-trigger-queue-{stage}"
QUEUES = [f"aqts-capture-error-queue-{stage}", f"aqts-capture-trigger-queue-{stage}"]

secrets_client = boto3.client('secretsmanager', os.environ['AWS_DEPLOYMENT_REGION'])
rds_client = boto3.client('rds', os.environ['AWS_DEPLOYMENT_REGION'])
lambda_client = boto3.client('lambda', os.getenv('AWS_DEPLOYMENT_REGION'))
sqs_client = boto3.client('sqs', os.getenv('AWS_DEPLOYMENT_REGION'))
s3_client = boto3.client('s3', os.getenv('AWS_DEPLOYMENT_REGION'))
s3 = boto3.resource('s3', os.getenv('AWS_DEPLOYMENT_REGION'))
cloudwatch_client = boto3.client('cloudwatch', os.getenv('AWS_DEPLOYMENT_REGION', 'us-west-2'))


def delete_db_cluster(event, context):
    rds_client.delete_db_cluster(
        DBClusterIdentifier=DB_CLUSTER_IDENTIFIER,
        SkipFinalSnapshot=True
    )


def modify_db_cluster(event, context):
    """
    When we restore the database from a production snapshot, we don't know the passwords.  So, modify the 
    postgres password here so we can work with the database.
    :param event:
    :param context:
    :return:
    """
    rds_client.modify_db_cluster(
        DBClusterIdentifier=DB_CLUSTER_IDENTIFIER,
        ApplyImmediately=True,
        MasterUserPassword='Password123'
    )


def delete_db_instance(event, context):
    rds_client.delete_db_instance(
        DBInstanceIdentifier=DB_INSTANCE_IDENTIFIER,
        SkipFinalSnapshot=True
    )


def create_db_instance(event, context):
    rds_client.create_db_instance(
        DBInstanceIdentifier=DB_INSTANCE_IDENTIFIER,
        DBInstanceClass=DB_INSTANCE_CLASS,
        DBClusterIdentifier=DB_CLUSTER_IDENTIFIER,
        Engine=ENGINE
    )


def copy_s3(event, context):
    """
    Copy files from the 'reference' bucket to the trigger bucket to simulate a full run.
    :param event:
    :param context:
    :return:
    """

    # START TEMPORARY
    logger.info("Start temporary copy")
    resp = s3_client.list_objects_v2(Bucket=REAL_BUCKET)
    keys = []
    for obj in resp['Contents']:
        keys.append(obj['Key'])
    logger.info(f"Key total: {len(keys)}")
    s3_resource = boto3.resource('s3')
    for key in keys:
        copy_source = {
            'Bucket': REAL_BUCKET,
            'Key': key
        }
        logger.info(f"KEY: {key}")
        bucket = s3_resource.Bucket(TEST_BUCKET)
        bucket.copy(copy_source, key)
    logger.info("finish temporary copy")
    # END TEMPORARY

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
        bucket = s3_resource.Bucket(TEST_BUCKET)
        bucket.copy(copy_source, key)


def wait_for_processing(event, context):
    response = cloudwatch_client.get_metric_data(
        MetricDataQueries=[
            {
                'Id': 'cpu_1',
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
                    'Period': 300,
                    'Stat': 'Maximum',
                }
            }
        ],
        StartTime=(datetime.datetime.now() - datetime.timedelta(seconds=300)).timestamp(),
        EndTime=datetime.datetime.now().timestamp()
    )
    db_utilization = response['MetricDataResults'][0]['Values'][0]
    if db_utilization > 1:
        raise Exception(f"Database is still busy {response}")


def enable_triggers(function_names, db_name):
    active_dbs = _describe_db_clusters('stop')
    if db_name not in active_dbs:
        return f"DB {db_name} is not active, skip enable of triggers"

    my_lambda = boto3.client('lambda', os.getenv('AWS_DEPLOYMENT_REGION', 'us-west-2'))
    for function_name in function_names:
        response = my_lambda.list_event_source_mappings(FunctionName=function_name)
        for item in response['EventSourceMappings']:
            response = my_lambda.get_event_source_mapping(UUID=item['UUID'])
            logger.info(f"before enabling trigger {response}")
            if response['State'] in ('Disabled', 'Disabling', 'Updating', 'Creating'):
                my_lambda.update_event_source_mapping(UUID=item['UUID'], Enabled=True)
                response = my_lambda.get_event_source_mapping(UUID=item['UUID'])
                return f"Trigger should be enabled.  function name: {function_name} item: {response}"
    return f"Trigger not enabled, even though db {db_name} was active function_name {function_name}"


def restore_db_cluster(event, context):
    """
    By default we try to restore the production snapshot that is two days old.  If a specific snapshot needs to be used
    for the test, it can be passed in as part of an event when the step function is invoked with the key 
    'snapshotIdentifier'.

    Restoring an aurora db cluster from snapshot takes one to two hours.
    """

    original = secrets_client.get_secret_value(
        SecretId=NWCAPTURE_REAL
    )
    secret_string = json.loads(original['SecretString'])
    kms_key = str(secret_string['KMS_KEY_ID'])
    subgroup_name = str(secret_string['DB_SUBGROUP_NAME'])
    vpc_security_group_id = str(secret_string['VPC_SECURITY_GROUP_ID'])
    if not kms_key or not subgroup_name or not vpc_security_group_id:
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
        DBSubnetGroupName=subgroup_name,
        DatabaseName='nwcapture-load',
        EnableIAMDatabaseAuthentication=False,
        EngineMode='provisioned',
        DBClusterParameterGroupName='aqts-capture',
        DeletionProtection=False,
        CopyTagsToSnapshot=False,
        KmsKeyId=kms_key,
        VpcSecurityGroupIds=[
            vpc_security_group_id
        ]
    )


#
# def xxdisable_trigger(event, context):
#     """
#     Disable the trigger on the real bucket (disrupting test tier while load test is in progress).
#     :param event:
#     :param context:
#     :return:
#     """
#     response = lambda_client.list_event_source_mappings(FunctionName=CAPTURE_TRIGGER)
#     for item in response['EventSourceMappings']:
#         lambda_client.update_event_source_mapping(UUID=item['UUID'], Enabled=False)
#     return True
#
#
# def xxenable_trigger(event, context):
#     """
#     Enable the trigger on the real bucket (after test, restoring things to normal)
#     if the real db is on.
#     :param event:
#     :param context:
#     :return:
#     """
#     active_dbs = _describe_db_clusters('stop')
#     if DB[stage] in active_dbs:
#         logger.info("DB Active, going to enable trigger")
#         response = lambda_client.list_event_source_mappings(FunctionName=CAPTURE_TRIGGER)
#         for item in response['EventSourceMappings']:
#             lambda_client.update_event_source_mapping(UUID=item['UUID'], Enabled=True)
#         return True
#     logger.info("DB Inactive, don't enable trigger")
#     return False


def add_trigger_to_bucket(event, context):
    """
    We have two buckets and one queue.  There's no way to disable event notification for a bucket, you have
    to remove it.  So here, to enable the trigger for testing we remove it from the real bucket, purge the
    queues, and then add the trigger to the test bucket.
    :param event:
    :param context:
    :return:
    """
    logger.info(f"REAL BUCKET {REAL_BUCKET}")
    logger.info(f"TEST BUCKET {TEST_BUCKET}")
    _remove_trigger(REAL_BUCKET)
    _purge_queues(QUEUES)
    _add_trigger(TEST_BUCKET)
    trigger_enabled = enable_triggers(CAPTURE_TRIGGER, DB["LOAD"])
    logger.info(f"Was the trigger enabled on {CAPTURE_TRIGGER} for {DB['LOAD']}?  {trigger_enabled}")


def remove_trigger_from_bucket(event, context):
    _remove_trigger(TEST_BUCKET)
    _purge_queues(QUEUES)
    _add_trigger(REAL_BUCKET)
    trigger_enabled = enable_triggers(CAPTURE_TRIGGER, DB[stage])
    logger.info(f"Was the trigger enabled on {CAPTURE_TRIGGER} for {DB[stage]}?  {trigger_enabled}")


def run_integration_tests(event, context):
    """
    Integration tests will go here.  Right now the idea is that the pre-test will save a TEST_RESULT object up 
    in the bucket and that the integration tests will write to that same object, so when everything finishes 
    it will be like a report. That's just a placeholder idea.
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

    obj = s3.Object('iow-retriever-capture-load', 'TEST_RESULTS')
    content = json.loads(obj.get()['Body'].read().decode('utf-8'))
    content["End Time"] = str(datetime.datetime.now())
    content["End Count"] = result
    s3.Object('iow-retriever-capture-load', 'TEST_RESULTS').put(Body=json.dumps(content))


def pre_test(event, context):
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
    rds = RDS(db_host, db_user, db_name, db_password)
    sql = "select count(1) from capture.json_data"
    result = rds.execute_sql(sql)

    content = {"StartTime": str(datetime.datetime.now()), "StartCount": result}
    s3.Object(TEST_BUCKET, 'TEST_RESULTS').put(Body=json.dumps(content))


def falsify_secrets(event, context):
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
    _replace_secrets(NWCAPTURE_REAL)


def modify_schema_owner_password(event, context):
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


def _add_trigger(bucket):
    logger.info(f"_add_trigger to bucket {bucket}")
    bucket_notification = s3.BucketNotification(bucket)
    my_queue_url = ""
    response = sqs_client.list_queues()
    for url in response['QueueUrls']:
        if CAPTURE_TRIGGER in url:
            logger.info(f"found url {url}")
            my_queue_url = url
            break
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
    logger.info(f"bucket_notification.put ... {response}")


def _remove_trigger(bucket):
    logger.info(f"_remove_trigger from bucket {bucket}")
    bucket_notification = s3.BucketNotification(bucket)
    bucket_notification.load()
    response = bucket_notification.put(
        NotificationConfiguration={
            'QueueConfigurations': [
            ]
        }
    )
    bucket_notification.load()
    logger.info(f"trigger should be removed {response}")


def _purge_queues(queue_names):
    for queue_name in queue_names:
        sqs = boto3.client('sqs', os.getenv('AWS_DEPLOYMENT_REGION'))
        queue_info = sqs.get_queue_url(QueueName=queue_name)
        sqs.purge_queue(QueueUrl=queue_info['QueueUrl'])
