import json
import os
import boto3
import datetime
import logging

from src.rds import RDS

log_level = os.getenv('LOG_LEVEL', logging.ERROR)
logger = logging.getLogger()
logger.setLevel(log_level)

two_days_ago = datetime.datetime.now() - datetime.timedelta(2)

LAMBDA_FUNCTIONS = [
    'aqts-capture-raw-load-TEST-iowCapture',
    'aqts-ts-type-router-TEST-determineRoute',
    'aqts-capture-ts-description-TEST-processTsDescription',
    'aqts-capture-ts-corrected-TEST-preProcess',
    'aqts-capture-ts-field-visit-TEST-preProcess',
    'aqts-capture-field-visit-metadata-TEST-preProcess',
    'aqts-capture-field-visit-transform-TEST-transform',
    'aqts-capture-discrete-loader-TEST-loadDiscrete',
    'aqts-capture-dvstat-transform-TEST-transform',
    'aqts-capture-ts-loader-TEST-loadTimeSeries',
    'aqts-capture-error-handler-TEST-aqtsErrorHandler'
]

month = str(two_days_ago.month)
if len(month) == 1:
    month = f"0{month}"
day = str(two_days_ago.day)
if len(day) == 1:
    day = f"0{day}"

# Default snapshot identifier, may be overridden by passing a custom
# snapshot identifier in the step function event
SNAPSHOT_IDENTIFIER = f"rds:nwcapture-prod-external-{two_days_ago.year}-{month}-{day}-10-08"
DB_INSTANCE_IDENTIFIER = 'nwcapture-load-instance1'
DB_INSTANCE_CLASS = 'db.r5.8xlarge'
ENGINE = 'aurora-postgresql'
DEST_BUCKET = 'iow-retriever-capture-load'
SRC_BUCKET = 'iow-retriever-capture-reference'
DB_CLUSTER_IDENTIFIER = 'nwcapture-load'
NWCAPTURE_TEST = 'NWCAPTURE-DB-TEST'
NWCAPTURE_LOAD = 'NWCAPTURE-DB-LOAD'

TEST_LAMBDA_TRIGGERS = [
    'aqts-capture-trigger-TEST-aqtsCaptureTrigger', 'aqts-capture-trigger-tmp-TEST-aqtsCaptureTrigger']

secrets_client = boto3.client('secretsmanager', os.environ['AWS_DEPLOYMENT_REGION'])
rds_client = boto3.client('rds', os.environ['AWS_DEPLOYMENT_REGION'])
lambda_client = boto3.client('lambda', os.getenv('AWS_DEPLOYMENT_REGION'))
sqs_client = boto3.client('sqs', os.getenv('AWS_DEPLOYMENT_REGION'))
s3_client = boto3.client('s3', os.getenv('AWS_DEPLOYMENT_REGION'))
s3 = boto3.resource('s3', os.getenv('AWS_DEPLOYMENT_REGION'))


def delete_db_cluster(event, context):
    rds_client.delete_db_cluster(
        DBClusterIdentifier=DB_CLUSTER_IDENTIFIER,
        SkipFinalSnapshot=True
    )


def modify_db_cluster(event, context):
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
        bucket = s3_resource.Bucket(DEST_BUCKET)
        bucket.copy(copy_source, key)


def restore_db_cluster(event, context):
    """
    By default we try to restore the production snapshot that
    is two days old.  If a specific snapshot needs to be used
    for the test, it can be passed in as part of an event when
    the step function is invoked with the key 'snapshotIdentifier'.
    """

    my_snapshot_identifier = SNAPSHOT_IDENTIFIER
    if event is not None:
        if event.get("snapshotIdentifier") is not None:
            my_snapshot_identifier = event.get("snapshotIdentifier")

    response = rds_client.restore_db_cluster_from_snapshot(
        DBClusterIdentifier=DB_CLUSTER_IDENTIFIER,
        SnapshotIdentifier=my_snapshot_identifier,
        Engine=ENGINE,
        EngineVersion='11.7',
        Port=5432,
        DBSubnetGroupName='nwisweb-capture-rds-aurora-test-dbsubnetgroup-41wlnfwg5krt',
        DatabaseName='nwcapture-load',
        EnableIAMDatabaseAuthentication=False,
        EngineMode='provisioned',
        DBClusterParameterGroupName='aqts-capture',
        DeletionProtection=False,
        CopyTagsToSnapshot=False,
        KmsKeyId='7654bdeb-56cd-4826-8e79-f9b8f9a53209',
        VpcSecurityGroupIds=[
            'sg-d0d1feaf',
        ],
    )


def falsify_secrets(event, context):
    original = secrets_client.get_secret_value(
        SecretId=NWCAPTURE_TEST,
    )
    secret_string = json.loads(original['SecretString'])
    orig_username = str(secret_string['SCHEMA_OWNER_USERNAME'])
    secret_string['SCHEMA_OWNER_USERNAME_BACKUP'] = orig_username
    orig_password = str(secret_string['SCHEMA_OWNER_PASSWORD'])
    secret_string['SCHEMA_OWNER_PASSWORD_BACKUP'] = orig_password
    orig_host = str(secret_string['DATABASE_ADDRESS'])
    secret_string['DATABASE_ADDRESS_BACKUP'] = orig_host
    secret_string['DATABASE_ADDRESS'] = 'nwcapture-load.cluster-c8adwxz9sely.us-west-2.rds.amazonaws.com'
    secret_string['SCHEMA_OWNER_USERNAME'] = "postgres"
    secret_string['SCHEMA_OWNER_PASSWORD'] = "Password123"

    secrets_client.update_secret(SecretId=NWCAPTURE_TEST, SecretString=json.dumps(secret_string))


def restore_secrets(event, context):
    original = secrets_client.get_secret_value(
        SecretId=NWCAPTURE_TEST,
    )
    secret_string = json.loads(original['SecretString'])
    original_username = str(secret_string['SCHEMA_OWNER_USERNAME_BACKUP'])
    original_password = str(secret_string['SCHEMA_OWNER_PASSWORD_BACKUP'])
    original_host = str(secret_string['DATABASE_ADDRESS_BACKUP'])
    secret_string['SCHEMA_OWNER_USERNAME'] = original_username
    secret_string['SCHEMA_OWNER_PASSWORD'] = original_password
    secret_string['DATABASE_ADDRESS'] = original_host
    del secret_string['SCHEMA_OWNER_USERNAME_BACKUP']
    del secret_string['SCHEMA_OWNER_PASSWORD_BACKUP']
    del secret_string['DATABASE_ADDRESS_BACKUP']
    secrets_client.update_secret(SecretId=NWCAPTURE_TEST, SecretString=json.dumps(secret_string))


def disable_trigger(event, context):
    for function_name in TEST_LAMBDA_TRIGGERS:
        response = lambda_client.list_event_source_mappings(FunctionName=function_name)
        for item in response['EventSourceMappings']:
            lambda_client.update_event_source_mapping(UUID=item['UUID'], Enabled=False)
    return True


def enable_trigger(event, context):
    for function_name in TEST_LAMBDA_TRIGGERS:
        response = lambda_client.list_event_source_mappings(FunctionName=function_name)
        for item in response['EventSourceMappings']:
            lambda_client.update_event_source_mapping(UUID=item['UUID'], Enabled=True)
    return True


def add_trigger_to_bucket(event, context):
    bucket_notification = s3.BucketNotification('iow-retriever-capture-load')
    bucket_notification.load()
    my_queue_url = ""
    response = sqs_client.list_queues()
    for url in response['QueueUrls']:
        if "aqts-capture-trigger-queue-TEST" in url:
            my_queue_url = url
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


def remove_trigger_from_bucket(event, context):
    bucket_notification = s3.BucketNotification('iow-retriever-capture-load')
    bucket_notification.load()
    my_queue_url = ""
    response = sqs_client.list_queues()
    for url in response['QueueUrls']:
        if "aqts-capture-trigger-queue-TEST" in url:
            my_queue_url = url

    response = bucket_notification.put(
        NotificationConfiguration={
            'QueueConfigurations': [
            ]
        }
    )
    bucket_notification.load()


def run_integration_tests(event, context):
    original = secrets_client.get_secret_value(
        SecretId=NWCAPTURE_TEST,
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
    content["End Time"] = datetime.datetime.now()
    content["End Count"] = result
    logger.info(f"Writing this to S3 {json.dumps(content)}")
    s3.Object('iow-retriever-capture-load', 'TEST_RESULTS').put(Body=json.dumps(content))


def pre_test(event, context):
    original = secrets_client.get_secret_value(
        SecretId=NWCAPTURE_TEST,
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

    content = {"StartTime": datetime.datetime.now(), "StartCount": result}
    logger.info(f"Writing this to S3 {json.dumps(content)}")
    s3.Object('iow-retriever-capture-load', 'TEST_RESULTS').put(Body=json.dumps(content))


def modify_env_variables(event, context):
    response = secrets_client.create_secret(
        Name=NWCAPTURE_LOAD
    )
    original = secrets_client.get_secret_value(
        SecretId=NWCAPTURE_TEST,
    )
    secret_string = json.loads(original['SecretString'])
    secrets_client.update_secret(SecretId=NWCAPTURE_LOAD, SecretString=json.dumps(secret_string))

    # original = secrets_client.get_secret_value(
    #     SecretId=NWCAPTURE_LOAD
    # )
    # secret_string = json.loads(original['SecretString'])
    # orig_username = str(secret_string['SCHEMA_OWNER_USERNAME'])
    # secret_string['SCHEMA_OWNER_USERNAME_BACKUP'] = orig_username
    # orig_password = str(secret_string['SCHEMA_OWNER_PASSWORD'])
    # secret_string['SCHEMA_OWNER_PASSWORD_BACKUP'] = orig_password
    # orig_host = str(secret_string['DATABASE_ADDRESS'])
    # secret_string['DATABASE_ADDRESS_BACKUP'] = orig_host
    # secret_string['DATABASE_ADDRESS'] = 'nwcapture-load.cluster-c8adwxz9sely.us-west-2.rds.amazonaws.com'
    # secret_string['SCHEMA_OWNER_USERNAME'] = "postgres"
    # secret_string['SCHEMA_OWNER_PASSWORD'] = "Password123"
    #
    # for lambda_function in LAMBDA_FUNCTIONS:
    #     response = lambda_client.update_function_configuration(
    #         FunctionName=lambda_function,
    #         Environment={
    #             'Variables': {
    #                 'env_var': 'hello'
    #             }
    #         }
    #     )
