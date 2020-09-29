import os
import boto3
import datetime
import logging

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
two_days_ago = datetime.datetime.now() - datetime.timedelta(2)

DB_CLUSTER_IDENTIFIER = 'nwcapture-load'

SNAPSHOT_IDENTIFIER = f"rds:nwcapture-prod-external-{two_days_ago.year}-{two_days_ago.month}-{two_days_ago.day}-10-08"
DB_INSTANCE_IDENTIFIER = 'nwcapture-load-instance1'
DB_INSTANCE_CLASS = 'db.r5.8xlarge'
ENGINE = 'aurora-postgresql'


def delete_db_cluster(event, context):
    client = boto3.client('rds', os.environ['AWS_DEPLOYMENT_REGION'])
    response = client.delete_db_cluster(
        DBClusterIdentifier=DB_CLUSTER_IDENTIFIER,
        SkipFinalSnapshot=True
    )


def delete_db_instance(event, context):
    client = boto3.client('rds', os.environ['AWS_DEPLOYMENT_REGION'])
    response = client.delete_db_instance(
        DBInstanceIdentifier=DB_INSTANCE_IDENTIFIER,
        SkipFinalSnapshot=True
    )


def create_db_instance(event, context):
    client = boto3.client('rds', os.environ['AWS_DEPLOYMENT_REGION'])
    response = client.create_db_instance(
        DBInstanceIdentifier=DB_INSTANCE_IDENTIFIER,
        DBInstanceClass=DB_INSTANCE_CLASS,
        DBClusterIdentifier=DB_CLUSTER_IDENTIFIER,
        MasterUsername='capture_owner',
        MasterUserPassword='FooDog123',
        Engine=ENGINE
    )


def copy_s3(event, context):
    # TODO modify aqts-capture-trigger to have a fake trigger bucket that works the same
    # as the real trigger bucket (name: aqts-retriever-capture-load)
    pass
    # s3 = boto3.resource('s3')
    # src_bucket = s3.Bucket('iow-retriever-capture-reference')
    # dest_bucket = s3.Bucket('iow-retriever-capture-load')
    # dest_bucket.objects.all().delete()  # this is optional clean bucket
    # count = 0
    # for obj in src_bucket.objects.all():
    #     s3.Object('dest_bucket', obj.key).put(Body=obj.get()["Body"].read())
    #     count = count + 1
    # return {
    #     'statusCode': 200,
    #     'message': f"copy_s3 copied {count} objects"
    # }


def restore_db_cluster(event, context):
    logger.debug(f"SNAPSHOT_IDENTIFIER: {SNAPSHOT_IDENTIFIER}")

    client = boto3.client('rds', os.environ['AWS_DEPLOYMENT_REGION'])
    response = client.restore_db_cluster_from_snapshot(
        DBClusterIdentifier=DB_CLUSTER_IDENTIFIER,
        SnapshotIdentifier=SNAPSHOT_IDENTIFIER,
        Engine=ENGINE,
        EngineVersion='11.7',
        Port=5477,
        DBSubnetGroupName='nwisweb-capture-rds-aurora-test-dbsubnetgroup-41wlnfwg5krt',
        DatabaseName='nwcapture-load',
        EnableIAMDatabaseAuthentication=True,
        EngineMode='provisioned',
        DBClusterParameterGroupName='aqts-capture',
        DeletionProtection=False,
        CopyTagsToSnapshot=False,
        VpcSecurityGroupIds=[
            'sg-d0d1feaf',
        ],
    )
    return {
        'statusCode': 201,
        'message': f"Db cluster should be restored {response}"
    }
