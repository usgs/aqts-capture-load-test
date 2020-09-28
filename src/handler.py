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

    # {
    #     'Marker': 'string',
    #     'DBClusterSnapshots': [
    #         {
    #             'AvailabilityZones': [
    #                 'string',
    #             ],
    #             'DBClusterSnapshotIdentifier': 'string',
    #             'DBClusterIdentifier': 'string',
    #             'SnapshotCreateTime': datetime(2015, 1, 1),
    #             'Engine': 'string',
    #             'AllocatedStorage': 123,
    #             'Status': 'string',
    #             'Port': 123,
    #             'VpcId': 'string',
    #             'ClusterCreateTime': datetime(2015, 1, 1),
    #             'MasterUsername': 'string',
    #             'EngineVersion': 'string',
    #             'LicenseModel': 'string',
    #             'SnapshotType': 'string',
    #             'PercentProgress': 123,
    #             'StorageEncrypted': True | False,
    #             'KmsKeyId': 'string',
    #             'DBClusterSnapshotArn': 'string',
    #             'SourceDBClusterSnapshotArn': 'string',
    #             'IAMDatabaseAuthenticationEnabled': True | False
    #         },
    #     ]
    # }
    response = client.describe_db_cluster_snapshots(
        DBClusterIdentifier=db_cluster_identifier,
        DBClusterSnapshotIdentifier=snapshot_identifier
    )
    if response.get('DbClusterSnapshots') is None:
        return {
            'statusCode': 200,
            'message': f"DbCluster already exists, skipping {response}"
        }

    response = client.restore_db_cluster_from_snapshot(
        DBClusterIdentifier=db_cluster_identifier,
        SnapshotIdentifier=snapshot_identifier,
        Engine='aurora-postgresql',
        EngineVersion='11.7',
        Port=5477,
        DBSubnetGroupName='nwisweb-capture-rds-aurora-test-dbsubnetgroup-41wlnfwg5krt',
        DatabaseName='nwcapture-load',
        # TODO 'WMA-TEST' -- doesnt exist or dont have permission?
        # KmsKeyId='WMA-TEST',
        EnableIAMDatabaseAuthentication=True,
        EngineMode='provisioned',
        DBClusterParameterGroupName='aqts-capture',
        DeletionProtection=False,
        CopyTagsToSnapshot=False
    )
    return {
        'statusCode': 201,
        'message': f"Db cluster should be created {response}"
    }
