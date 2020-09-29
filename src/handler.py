import os
import boto3
import logging

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)


def delete_db_cluster(event, context):
    client = boto3.client('rds', os.environ['AWS_DEPLOYMENT_REGION'])
    response = client.delete_db_cluster(
        DBClusterIdentifier='nwcapture-load',
        SkipFinalSnapshot=True
    )


def restore_db_cluster(event, context):
    db_cluster_identifier = 'nwcapture-load';
    snapshot_identifier = 'rds:nwcapture-qa-2020-09-27-06-15'
    client = boto3.client('rds', os.environ['AWS_DEPLOYMENT_REGION'])

    response = client.restore_db_cluster_from_snapshot(
        DBClusterIdentifier=db_cluster_identifier,
        SnapshotIdentifier=snapshot_identifier,
        Engine='aurora-postgresql',
        EngineVersion='11.7',
        Port=5477,
        DBSubnetGroupName='nwisweb-capture-rds-aurora-test-dbsubnetgroup-41wlnfwg5krt',
        DatabaseName='nwcapture-load',
        EnableIAMDatabaseAuthentication=True,
        EngineMode='provisioned',
        DBClusterParameterGroupName='aqts-capture',
        DeletionProtection=False,
        CopyTagsToSnapshot=False
    )
    # TODO handle errors
    return {
        'statusCode': 201,
        'message': f"Db cluster should be created {response}"
    }
