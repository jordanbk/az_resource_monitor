import dataiku
from dataiku.scenario import Scenario
import pandas as pd, numpy as np
from dataiku import pandasutils as pdu
import datetime
from azure.identity import DefaultAzureCredential
from azure.mgmt.resource import ResourceManagementClient
from azure.identity import  DefaultAzureCredential
from azure.mgmt.compute import ComputeManagementClient
from azure.mgmt.monitor import MonitorManagementClient
from azure.core import exceptions
import logging
from typing import Optional, Dict
import os

os.environ["AZURE_CLIENT_ID"] = "client-ID"
os.environ["AZURE_TENANT_ID"] = "tenant-ID"
os.environ["AZURE_CLIENT_SECRET"] = "secret"

credential = DefaultAzureCredential()
subscription_id = "9873b25c-ff9f-4ddb-9657-4d7032405519"

client = ResourceManagementClient(credential, subscription_id)
compute_client = ComputeManagementClient(credential, subscription_id)

def get_resource_groups() -> list:
    resource_name = []
    for i in client.resource_groups.list():
        resource_name.append(i.name)
    return resource_name

def get_all_az_disks() -> list:
    results = []
    resource_groups = get_resource_groups()
    for r in resource_groups:
        disks = compute_client.disks.list_by_resource_group(r)
        for disk in disks:
            r_dict = dict({
            'id': disk.id,
            'disk_name' : disk.name,
            'disk_state': disk.disk_state,
            'disk_size' : disk.disk_size_gb,
            'disk_managed_by': disk.managed_by,
            'disk_tags': disk.tags,
            'disk_location': disk.location,
            # This property represents when the disk’s state was last updated. For an unattached disk, this will show the time when the disk was unattached
            'disk_last_update': disk.last_ownership_update_time
            })
            results.append(r_dict)
    return results
        
# def get_az_instance_by_rg(rg):
#     disks = compute_client.disks.list_by_resource_group(rg)
#     for disk in disks:
#         return disk
    
def get_disk_ids() -> list:
    results = list()
    all_disks = get_all_az_disks()
    for id in all_disks:
        ids = id['id']
        results.append(ids)
    return results


def snapshot_disk(resource_group_name, disk_name, snapshot_name: Optional[str] = None,
               tags: Optional[Dict[str, str]] = None):
    
    """Create a snapshot of the disk."""
    
    disk = compute_client.disks.get(resource_group_name, disk_name)

    if not snapshot_name:
      snapshot_name = disk_name + '_snapshot'
    #   truncate_at = 80 - 1
    #   snapshot_name = disk_name[:truncate_at]

    creation_data = {
        'location': disk.location,
        'creation_data': {
            'sourceResourceId': disk.id,
            'create_option': 'Copy'
        }
    }

    if tags:
      creation_data['tags'] = 'support_resource_control'

    try:
      request = compute_client.snapshots.begin_create_or_update(
          resource_group_name,
          snapshot_name,
          creation_data)
      while not request.done():
        sleep(5)  # Wait 5 seconds before checking snapshot status again
      snapshot = request.result()
      return snapshot
    except Exception as e:
        logging.error("Error cloning Azure volume {}. Failed with error {}.".
                      format(disk_name, e))
    
def delete_disk(resource_group_name, disk_name):
    compute_client.disks.begin_delete(resource_group_name, disk_name)
    

def sendEmail(senderEmail, recipientEmail, subject, message):

    scenario = Scenario()
    # Create a message sender
    sender = scenario.get_message_sender(channel_id = "mail")

    sender.set_params(sender=senderEmail,
                 recipient=recipientEmail, #multiple emails 
                 subject=subject,
                 message=message)
    sender.send()
    
