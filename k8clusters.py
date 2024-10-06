import dataiku
import pandas as pd, numpy as np
from dataiku import pandasutils as pdu
import datetime
from azure.identity import DefaultAzureCredential
from azure.mgmt.resource import ResourceManagementClient
from azure.identity import  DefaultAzureCredential
from azure.mgmt.compute import ComputeManagementClient
from azure.mgmt.monitor import MonitorManagementClient
from azure.mgmt.containerservice import ContainerServiceClient
import os
import itertools
from azure.core.paging import ItemPaged

os.environ["AZURE_CLIENT_ID"] = "client-ID"
os.environ["AZURE_TENANT_ID"] = "tenant-ID"
os.environ["AZURE_CLIENT_SECRET"] = "secret"

credential = DefaultAzureCredential()
subscription_id = "9873b25c-ff9f-4ddb-9657-4d7032405519"
client = ResourceManagementClient(credential, subscription_id)
compute_client = ComputeManagementClient(credential, subscription_id)
monitor_client = MonitorManagementClient(credential, subscription_id)
containerservice_client = ContainerServiceClient(credential, subscription_id)

def list_clusters():
    return containerservice_client.managed_clusters.list()

def is_clusters_paged_empty(item_paged: ItemPaged) -> bool:
    first_item = next(item_paged, None)
    if first_item is None:
        return True
    else:
        # If the iterator is not empty, recreate it with the first item included
        item_paged = itertools.chain([first_item], item_paged)
        return False

def delete_cluster(resource_group_name, cluster_name):
    containerservice_client.managed_clusters.begin_delete(
        resource_group_name,
        cluster_name
    ).result


def last_usage(resourceid):
    results = []
    for r in resourceid:
        d_date = datetime.datetime.now() 
        today = d_date.strftime('%Y-%m-%dT%H:%M:%S.%fZ') 
        start_date = (datetime.datetime.now() - datetime.timedelta(days=15)).strftime('%Y-%m-%dT%H:%M:%S.%fZ')
        filter = " and ".join([ "eventTimestamp ge '{}'".format(start_date), "eventTimestamp le '{}'".format(today), "resourceURI eq '{}'".format(r)])
        select = ",".join([ "eventName", "operationName", "eventTimestamp", "resourceId"])
        activity_logs = monitor_client.activity_logs.list( filter=filter, select=select )
        for log in activity_logs:
            if log.operation_name.localized_value == 'Delete Managed Cluster' or log.operation_name.localized_value == 'Create or Update Managed Cluster':
                r_dict = dict({
                'event_timestamp': log.event_timestamp,
                'operation_name' : log.operation_name.localized_value,
                'resource_id': log.resource_id
                })
                results.append(r_dict)
    return results