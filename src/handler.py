import os
import boto3

def delete_db_cluster(event, context):
    client = boto3.client('rds', os.environ['AWS_DEPLOYMENT_REGION'])
    response = client.delete_db_cluster(
        DBClusterIdentifier='nwcapture-load',
        SkipFinalSnapshot=True
    )


def restore_db_cluster(event, context):
    client = boto3.client('rds', os.environ['AWS_DEPLOYMENT_REGION'])
    response = client.restore_db_cluster_from_snapshot(
        # AvailabilityZones=[
        #    'string',
        # ],
        DBClusterIdentifier='nwcapture-load',
        # SnapshotIdentifier='nwcapture-prod-external-final',
        SnapshotIdentifier='nwcapture-test-20200611',
        Engine='aurora-postgresql',
        # TODO update to 11.7 when we have an 11.7 snapshot
        EngineVersion='10.7',
        Port=5477,
        DBSubnetGroupName='nwisweb-capture-rds-aurora-test-dbsubnetgroup-41wlnfwg5krt',
        DatabaseName='nwcapture-load',
        # OptionGroupName='string',
        # VpcSecurityGroupIds=[
        #    'string',
        # ],
        # Tags=[
        #    {
        #        'Key': 'string',
        #        'Value': 'string'
        #    },
        # ],
        KmsKeyId='WMA-TEST',
        EnableIAMDatabaseAuthentication=True,
        # EnableCloudwatchLogsExports=[
        #    'string',
        # ],
        EngineMode='provisioned',
        # TODO use aqts-capture when we have an 11.7 snapshot
        # DBClusterParameterGroupName='aqts-capture',
        DBClusterParameterGroupName='aqtstest',
        DeletionProtection=False,
        CopyTagsToSnapshot=False
        # Domain='string',
        # DomainIAMRoleName='string'
    )
