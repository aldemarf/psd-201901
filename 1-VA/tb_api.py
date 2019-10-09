#!/usr/bin/env python

import json
import logging
import requests

logging.basicConfig(level=logging.INFO)

def get_tenant_token(host='localhost', port='9090', user='tenant@thingsboard.org', pwd='tenant'):
    """ Returns tenants token """
    user_id = {"username": user, "password": pwd}
    headers = {'Content-Type': 'application/json', 'Accept': 'application/json'}
    url = 'http://{}:{}/api/auth/login'.format(host, port)

    result = requests.post(url, json=user_id, headers=headers)
    data = json.loads(result.content)

    if result.status_code == 200:
        token = data['token']
        return token
    else:
        logging.error('status response: {} -- {}'.format(data['status'], data['message']))
        return None


def get_tenant_devices(host='localhost', port='9090', token='', **kwargs):
    """ Returns tenant attached devices """
    token = 'Bearer {}'.format(token)
    headers = {'Accept': 'application/json', 'X-Authorization': token}
    url = 'http://{}:{}/api/tenant/devices?'.format(host, port)

    deviceType = kwargs.get('deviceType')
    textSearch = kwargs.get('textSearch')
    idOffset = kwargs.get('idOffset')
    textOffset = kwargs.get('textOffset')
    limit = kwargs.get('limit')

    if deviceType:
        url += 'type={}&'.format(deviceType)
    if textSearch:
        url += 'textSearch={}&'.format(textSearch)
    if idOffset:
        url += 'idOffset={}&'.format(idOffset)
    if textOffset:
        url += 'textOffset={}&'.format(textOffset)
    if limit:
        url += 'limit={}&'.format(limit)

    url = url[:-1]
    result = requests.get(url, headers=headers)
    data = json.loads(result.content)

    if result.status_code == 200:
        devices = data['data']
        return devices
    else:
        logging.error('status response: {} -- {}'.format(data['status'], data['message']))
        return None


def get_device_id(device):
    """ Returns devices ids """
    if isinstance(device, list):
        ids = [item['id']['id'] for item in device]
        return ids
    else:
        return device['id']['id']


def get_device_credential(device_id='', host='localhost', port='9090', token=''):
    """ Returns a single device credential : String"""
    token = 'Bearer {}'.format(token)
    headers = {'Accept': 'application/json', 'X-Authorization': token}

    url = 'http://{}:{}/api/device/{}/credentials'.format(host, port, device_id)

    result = requests.get(url, headers=headers)
    data = json.loads(result.content)
    #logging.warning("------------BODY-------------")
    #logging.warning(headers)
    #logging.warning("------------CONTENT-------------")
    #logging.warning(result.content)
    if result.status_code == 200:
        credential = data['credentialsId']
        return credential
    else:
        logging.error('status response: {} -- {}'.format(data['status'], data['message']))
        return None


def get_devices_credentials(devices_list=[], host='localhost', port='9090', token=''):
    """ Returns a dictionary with device id and its credential {id : credential}"""
    if len(devices_list) == 0:
        return None
    else:
        get = get_device_credential
        credentials = {device: get(device, host, port, token) for device in devices_list}
        return credentials


def create_device(device_name, device_type, device_label='', host='localhost', port='9090', token=''):
    """ create a single device and returns its the TB object """
    token = 'Bearer {}'.format(token)
    headers = {'Content-Type': 'application/json', 'Accept': 'application/json', 'X-Authorization': token}
    url = 'http://{}:{}/api/device'.format(host, port)
    device = {"name": device_name, "type": device_type, "label" : device_label}

    result = requests.post(url, headers=headers, json=device)
    data = json.loads(result.content)

    if result.status_code == 200:
        return data
    else:
        logging.error('status response: {} -- {}'.format(data['status'], data['message']))
        return None


def createDashboard():
    token = 'Bearer {}'.format(token)


########################################################
################       TEST FIELD       ################
########################################################

# token = get_tenant_token()
# devices_list = get_tenant_devices(limit=1000, token=token)
# id_list = get_device_id(devices_list, token=token)
# credentials = get_devices_credentials(devices_list=id_list, token=token)


########################################################
################       TEST FIELD       ################
########################################################