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

os.environ["AZURE_CLIENT_ID"] = "client-ID"
os.environ["AZURE_TENANT_ID"] = "tenant-ID"
os.environ["AZURE_CLIENT_SECRET"] = "secret"

credential = DefaultAzureCredential()
subscription_id = "9873b25c-ff9f-4ddb-9657-4d7032405519"

client = ResourceManagementClient(credential, subscription_id)
compute_client = ComputeManagementClient(credential, subscription_id)
monitor_client = MonitorManagementClient(credential, subscription_id)

def get_all_instances():
    return compute_client.virtual_machines.list_all()

def get_instance_ids():
    results = []
    all_vms = get_all_instances()
    for vm in all_vms:
        general_view = vm.id.split("/")
        resource_group = general_view[4]
        vm_name = general_view[-1]
        vm = compute_client.virtual_machines.get(resource_group, vm_name, expand='instanceView')
        results.append(vm.id)
    return results


def get_az_instances():
    results = []
    vm_list = compute_client.virtual_machines.list_all()
    for vm_general in vm_list:
        general_view = vm_general.id.split("/")
        resource_group = general_view[4]
        vm_name = general_view[-1]
        vm = compute_client.virtual_machines.get(resource_group, vm_name, expand='instanceView')
        r_dict = dict({
            'provisioning_state': vm.provisioning_state,
            'vm_name' : vm.name,
            'vm_id' : vm.id,
            'vm_tags': vm.tags,
            'vm_location': vm.location
            })
        for stat in vm.instance_view.statuses:
            r_dict['status']= stat.code
        results.append(r_dict)
    return results


def last_usage():
    resourceid = get_instance_ids()
    results = []
    for r in resourceid:
        d_date = datetime.datetime.now() 
        today = d_date.strftime('%Y-%m-%dT%H:%M:%S.%fZ') 
        start_date = (datetime.datetime.now() - datetime.timedelta(days=60)).strftime('%Y-%m-%dT%H:%M:%S.%fZ')
        filter = " and ".join([ "eventTimestamp ge '{}'".format(start_date), "eventTimestamp le '{}'".format(today), "resourceURI eq '{}'".format(r)])
        select = ",".join([ "eventName", "operationName", "eventTimestamp", "resourceId"])
        activity_logs = monitor_client.activity_logs.list( filter=filter, select=select )
        for log in activity_logs:
             if log.operation_name.localized_value == 'Deallocate Virtual Machine' or log.operation_name.localized_value == 'Start Virtual Machine' or log.operation_name.localized_value == 'Create or Update Virtual Machine':
                r_dict = dict({
                'event_timestamp': log.event_timestamp,
                'operation_name' : log.operation_name.localized_value,
                'resource_id': log.resource_id
                })
                results.append(r_dict)
    return results


# https://learn.microsoft.com/en-us/python/api/azure-mgmt-compute/azure.mgmt.compute.v2019_03_01.operations.virtualmachinesoperations?view=azure-python#azure-mgmt-compute-v2019-03-01-operations-virtualmachinesoperations-begin-deallocate

def stop_running_instances(resource_group, vm_name):
    vm = compute_client.virtual_machines.get(resource_group, vm_name)
    if 'DkuKeepRunning' not in vm.tags:
        compute_client.virtual_machines.begin_deallocate(resource_group, vm_name)
        
def delete_vm(resource_group, vm_name):
    compute_client.virtual_machines.begin_delete(resource_group, vm_name)
    

            
        
        
        
        
        
        
        
        
        
        
        
        