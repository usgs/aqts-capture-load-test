import os
import boto3
import datetime
import logging

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
two_days_ago = datetime.datetime.now() - datetime.timedelta(2)

month = str(two_days_ago.month)
if len(month) == 1:
    month = f"0{month}"
day = str(two_days_ago.day)
if len(day) == 1:
    day = f"0{day}"

SNAPSHOT_IDENTIFIER = f"rds:nwcapture-prod-external-{two_days_ago.year}-{month}-{day}-10-08"
DB_INSTANCE_IDENTIFIER = 'nwcapture-load-instance1'
DB_INSTANCE_CLASS = 'db.r5.8xlarge'
ENGINE = 'aurora-postgresql'
DEST_BUCKET = 'iow-retriever-capture-load'
SRC_BUCKET = 'iow-retriever-capture-reference'
DB_CLUSTER_IDENTIFIER = 'nwcapture-load'


def delete_db_cluster(event, context):
    client = boto3.client('rds', os.environ['AWS_DEPLOYMENT_REGION'])
    response = client.delete_db_cluster(
        DBClusterIdentifier=DB_CLUSTER_IDENTIFIER,
        SkipFinalSnapshot=True
    )
    return {
        'statusCode': 200,
        'message': f"Db cluster should be deleted {response}"
    }


def modify_db_cluster(event, context):
    client = boto3.client('rds', os.environ['AWS_DEPLOYMENT_REGION'])
    response = client.modify_db_cluster(
        DBClusterIdentifier=DB_CLUSTER_IDENTIFIER,
        ApplyImmediately=True,
        MasterUserPassword='Password123'
    )
    return {
        'statusCode': 200,
        'message': f"Db cluster should be modified {response}"
    }


def delete_db_instance(event, context):
    client = boto3.client('rds', os.environ['AWS_DEPLOYMENT_REGION'])
    response = client.delete_db_instance(
        DBInstanceIdentifier=DB_INSTANCE_IDENTIFIER,
        SkipFinalSnapshot=True
    )
    return {
        'statusCode': 200,
        'message': f"Db cluster should be deleted {response}"
    }


def create_db_instance(event, context):
    client = boto3.client('rds', os.environ['AWS_DEPLOYMENT_REGION'])
    response = client.create_db_instance(
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

    client = boto3.client('rds', os.environ['AWS_DEPLOYMENT_REGION'])
    response = client.restore_db_cluster_from_snapshot(
        DBClusterIdentifier=DB_CLUSTER_IDENTIFIER,
        SnapshotIdentifier=SNAPSHOT_IDENTIFIER,
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


def disable_main_trigger(event, context):
    logger.debug(
        "Here we will disable the main trigger on the real bucket.  See aqts-capture-ecosystem-switch.")


def enable_main_trigger(event, context):
    logger.debug(
        "Here we will enable the main trigger on the real bucket.  See aqts-capture-ecosystem-switch.")


def falsify_secrets(event, context):
    logger.debug(
        "Here we will modify the secrets for nwcapture-test so that lambdas talk to nwcapture-load.")


def restore_secrets(event, context):
    logger.debug(
        "Here we will restore the secrets for nwcapture-test.")


def run_integration_tests(event, context):
    logger.debug("Here we will run tests and report results.")
