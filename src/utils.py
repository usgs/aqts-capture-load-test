# import logging
# import os
# import boto3
import logging

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

# def describe_db_clusters(action):
#    my_rds = boto3.client('rds', os.environ['AWS_DEPLOYMENT_REGION'])
#    # Get all the instances
#    response = my_rds.describe_db_clusters()
#    all_dbs = response['DBClusters']
#    if action == "start":
#        # Filter on the one that are not running yet
#        rds_cluster_identifiers = [x['DBClusterIdentifier'] for x in all_dbs if x['Status'] != 'available']
#        return rds_cluster_identifiers
#    if action == "stop":
#        # Filter on the one that are running

#        rds_cluster_identifiers = [x['DBClusterIdentifier'] for x in all_dbs if x['Status'] == 'available']
#        return rds_cluster_identifiers


# def start_db_cluster(cluster_identifier):
#    my_rds = boto3.client('rds', os.environ['AWS_DEPLOYMENT_REGION'])
#    my_rds.start_db_cluster(
#        DBClusterIdentifier=cluster_identifier
#    )
#    return True


# def stop_db_cluster(cluster_identifier):
#    my_rds = boto3.client('rds', os.environ['AWS_DEPLOYMENT_REGION'])
#    my_rds.stop_db_cluster(
#        DBClusterIdentifier=cluster_identifier
#    )
#    return True


# def purge_queue(queue_name):
#    sqs = boto3.client('sqs', os.getenv('AWS_DEPLOYMENT_REGION'))
#    queue_info = sqs.get_queue_url(QueueName=queue_name)
#    sqs.purge_queue(QueueUrl=queue_info['QueueUrl'])


# def disable_triggers(function_names):
#    my_lambda = boto3.client('lambda', os.getenv('AWS_DEPLOYMENT_REGION'))
#    logger.debug("trying to disable triggers")
#    for function_name in function_names:
#        response = my_lambda.list_event_source_mappings(FunctionName=function_name)
#        for item in response['EventSourceMappings']:
#            my_lambda.update_event_source_mapping(UUID=item['UUID'], Enabled=False)
#            returned = my_lambda.get_event_source_mapping(UUID=item['UUID'])
#            logger.debug(f"Trigger should be disabled.  function name: {function_name} item: {returned}")
#    return True

# def enable_triggers(function_names):
#    my_lambda = boto3.client('lambda', os.getenv('AWS_DEPLOYMENT_REGION'))
#    logger.debug("trying to enable triggers")
#    for function_name in function_names:
#        response = my_lambda.list_event_source_mappings(FunctionName=function_name)
#        for item in response['EventSourceMappings']:
#            my_lambda.update_event_source_mapping(UUID=item['UUID'], Enabled=True)
#            returned = my_lambda.get_event_source_mapping(UUID=item['UUID'])
#            logger.debug(f"Trigger should be enabled.  function name: {function_name} item: {returned}")
#    return True
