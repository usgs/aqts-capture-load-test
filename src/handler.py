import json
import os
import boto3
import datetime
import logging

from src.rds import RDS

logger = logging.getLogger()
logger.setLevel(logging.INFO)
two_days_ago = datetime.datetime.now() - datetime.timedelta(2)

month = str(two_days_ago.month)
if len(month) == 1:
    month = f"0{month}"
day = str(two_days_ago.day)
if len(day) == 1:
    day = f"0{day}"

# Default snapshot identifier, may be overridden in restoreDbCluster
SNAPSHOT_IDENTIFIER = f"rds:nwcapture-prod-external-{two_days_ago.year}-{month}-{day}-10-08"
DB_INSTANCE_IDENTIFIER = 'nwcapture-load-instance1'
DB_INSTANCE_CLASS = 'db.r5.8xlarge'
ENGINE = 'aurora-postgresql'
DEST_BUCKET = 'iow-retriever-capture-load'
SRC_BUCKET = 'iow-retriever-capture-reference'
DB_CLUSTER_IDENTIFIER = 'nwcapture-load'
NWCAPTURE_TEST = 'NWCAPTURE-DB-TEST'

TEST_LAMBDA_TRIGGERS = [
    'aqts-capture-trigger-TEST-aqtsCaptureTrigger', 'aqts-capture-trigger-tmp-TEST-aqtsCaptureTrigger']

secrets_client = boto3.client('secretsmanager', os.environ['AWS_DEPLOYMENT_REGION'])
rds_client = boto3.client('rds', os.environ['AWS_DEPLOYMENT_REGION'])
lambda_client = boto3.client('lambda', os.getenv('AWS_DEPLOYMENT_REGION'))
sqs_client = boto3.client('sqs', os.getenv('AWS_DEPLOYMENT_REGION'))


def delete_db_cluster(event, context):
    response = rds_client.delete_db_cluster(
        DBClusterIdentifier=DB_CLUSTER_IDENTIFIER,
        SkipFinalSnapshot=True
    )
    return {
        'statusCode': 200,
        'message': f"Db cluster should be deleted {response}"
    }


def modify_db_cluster(event, context):
    response = rds_client.modify_db_cluster(
        DBClusterIdentifier=DB_CLUSTER_IDENTIFIER,
        ApplyImmediately=True,
        MasterUserPassword='Password123'
    )
    return {
        'statusCode': 200,
        'message': f"Db cluster should be modified {response}"
    }


def delete_db_instance(event, context):
    response = rds_client.delete_db_instance(
        DBInstanceIdentifier=DB_INSTANCE_IDENTIFIER,
        SkipFinalSnapshot=True
    )
    return {
        'statusCode': 200,
        'message': f"Db cluster should be deleted {response}"
    }


def create_db_instance(event, context):
    response = rds_client.create_db_instance(
        DBInstanceIdentifier=DB_INSTANCE_IDENTIFIER,
        DBInstanceClass=DB_INSTANCE_CLASS,
        DBClusterIdentifier=DB_CLUSTER_IDENTIFIER,
        Engine=ENGINE
    )
    return {
        'statusCode': 201,
        'message': f"Db instance should be created {response}"
    }


def delete_bucket(event, context):
    s3 = boto3.resource('s3')
    bucket = s3.Bucket('my-bucket')
    bucket.objects.all().delete()
    client = boto3.client('s3')
    response = client.delete_bucket(
        Bucket=DEST_BUCKET
    )


def copy_s3(event, context):
    # TODO modify aqts-capture-trigger to have a fake trigger bucket that works the same
    # as the real trigger bucket (name: aqts-retriever-capture-load)

    s3_client = boto3.client('s3')
    resp = s3_client.list_objects_v2(Bucket=SRC_BUCKET)
    keys = []
    for obj in resp['Contents']:
        keys.append(obj['Key'])

    s3_resource = boto3.resource('s3')
    count = 0
    for key in keys:
        copy_source = {
            'Bucket': SRC_BUCKET,
            'Key': key
        }
        bucket = s3_resource.Bucket(DEST_BUCKET)
        bucket.copy(copy_source, key)
        count = count + 1
    return {
        'statusCode': 200,
        'message': f"copy_s3 copied {count} objects"
    }


def restore_db_cluster(event, context):
    logger.debug(f"event: {event}")
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
    return {
        'statusCode': 201,
        'message': f"Db cluster should be restored {response}"
    }


def falsify_secrets(event, context):
    original = secrets_client.get_secret_value(
        SecretId=NWCAPTURE_TEST,
    )
    logger.info(f"secrets before falsify: {original['SecretString']}")
    secret_string = json.loads(original['SecretString'])
    secret_string['TEST_BUCKET'] = DEST_BUCKET
    orig_username = str(secret_string['SCHEMA_OWNER_USERNAME'])
    secret_string['SCHEMA_OWNER_USERNAME_BACKUP'] = orig_username
    orig_password = str(secret_string['SCHEMA_OWNER_PASSWORD'])
    secret_string['SCHEMA_OWNER_PASSWORD_BACKUP'] = orig_password
    secret_string['SCHEMA_OWNER_USERNAME'] = "postgres"
    secret_string['SCHEMA_OWNER_PASSWORD'] = "Password123"
    logger.info(f"secrets after falsify: {secret_string}")

    secrets_client.update_secret(SecretId=NWCAPTURE_TEST, SecretString=json.dumps(secret_string))


def restore_secrets(event, context):
    original = secrets_client.get_secret_value(
        SecretId=NWCAPTURE_TEST,
    )
    logger.info(f"secrets before restore: {original['SecretString']}")

    secret_string = json.loads(original['SecretString'])
    original_username = str(secret_string['SCHEMA_OWNER_USERNAME_BACKUP'])
    original_password = str(secret_string['SCHEMA_OWNER_PASSWORD_BACKUP'])
    secret_string['SCHEMA_OWNER_USERNAME'] = original_username
    secret_string['SCHEMA_OWNER_PASSWORD'] = original_password
    del secret_string['SCHEMA_OWNER_USERNAME_BACKUP']
    del secret_string['SCHEMA_OWNER_PASSWORD_BACKUP']
    logger.info(f"secrets before restore: {secret_string}")

    secrets_client.update_secret(SecretId=NWCAPTURE_TEST, SecretString=json.dumps(secret_string))



def disable_trigger(event, context):
    logger.debug("trying to disable trigger")
    for function_name in TEST_LAMBDA_TRIGGERS:
        response = lambda_client.list_event_source_mappings(FunctionName=function_name)
        for item in response['EventSourceMappings']:
            # lambda_client.update_event_source_mapping(UUID=item['UUID'], Enabled=False)
            returned = lambda_client.get_event_source_mapping(UUID=item['UUID'])
            logger.debug(f"Trigger should be disabled.  function name: {function_name} item: {returned}")
    return True


def enable_trigger(event, context):
    logger.debug("trying to enable trigger")
    for function_name in TEST_LAMBDA_TRIGGERS:
        response = lambda_client.list_event_source_mappings(FunctionName=function_name)
        for item in response['EventSourceMappings']:
            lambda_client.update_event_source_mapping(UUID=item['UUID'], Enabled=True)
            returned = lambda_client.get_event_source_mapping(UUID=item['UUID'])
            logger.debug(f"Trigger should be enabled.  function name: {function_name} item: {returned}")
    return True


def add_trigger_to_bucket(event, context):
    s3 = boto3.resource('s3')
    bucket_notification = s3.BucketNotification('iow-retriever-capture-load')
    bucket_notification.load()
    my_queue_url = ""
    response = sqs_client.list_queues()
    for url in response['QueueUrls']:
        if "aqts-capture-trigger-queue-TEST" in url:
            my_queue_url = url
    logger.info(f"using {my_queue_url}")
    response = sqs_client.get_queue_attributes(
        QueueUrl=my_queue_url,
        AttributeNames=['QueueArn']
    )
    my_queue_arn = response['Attributes']['QueueArn']
    logger.info(f"MY QUEUE ARN: {my_queue_arn}")

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
    logger.info(f"response {response}")


def remove_trigger_from_bucket(event, context):
    s3 = boto3.resource('s3')
    bucket_notification = s3.BucketNotification('iow-retriever-capture-load')
    bucket_notification.load()
    my_queue_url = ""
    response = sqs_client.list_queues()
    for url in response['QueueUrls']:
        if "aqts-capture-trigger-queue-TEST" in url:
            my_queue_url = url
    logger.info(f"using {my_queue_url}")
    response = sqs_client.get_queue_attributes(
        QueueUrl=my_queue_url,
        AttributeNames=['QueueArn']
    )
    my_queue_arn = response['Attributes']['QueueArn']
    logger.info(f"MY QUEUE ARN: {my_queue_arn}")

    response = bucket_notification.put(
        NotificationConfiguration={
            'QueueConfigurations': [
            ]
        }
    )
    bucket_notification.load()
    logger.info(f"response {response}")


def run_integration_tests(event, context):
    original = secrets_client.get_secret_value(
        SecretId=NWCAPTURE_TEST,
    )
    secret_string = json.loads(original['SecretString'])
    logger.info(f"secrets {secret_string}")
    os.environ['DB_HOST'] = secret_string['DATABASE_ADDRESS']
    os.environ['DB_USER'] = secret_string['SCHEMA_OWNER_USERNAME']
    os.environ['DB_NAME'] = secret_string['DATABASE_NAME']
    os.environ['DB_PASSWORD'] = secret_string['SCHEMA_OWNER_PASSWORD']
    rds = RDS()
    sql = "select count(1) from json_data"
    result = rds.execute_sql(sql)
    logger.info(f"RESULT: {result}")
