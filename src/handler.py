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
        SnapshotIdentifier='rds:nwcapture-qa-2020-09-27-06-15',
        Engine='aurora-postgresql',
        EngineVersion='11.7',
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
        # TODO 'WMA-TEST' -- doesnt exist or dont have permission?
        # KmsKeyId='WMA-TEST',
        EnableIAMDatabaseAuthentication=True,
        # EnableCloudwatchLogsExports=[
        #    'string',
        # ],
        EngineMode='provisioned',
        DBClusterParameterGroupName='aqts-capture',
        DeletionProtection=False,
        CopyTagsToSnapshot=False
        # Domain='string',
        # DomainIAMRoleName='string'
    )
