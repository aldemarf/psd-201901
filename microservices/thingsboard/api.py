import json
import logging
import requests

logging.basicConfig(level=logging.INFO)


def get_tenant_token(host='localhost', port='9090', user='tenant@thingsboard.org', pwd='tenant'):
    """ Returns tenants token """
    user_id = {"username": user, "password": pwd}
    headers = {'Content-Type': 'application/json', 'Accept': 'application/json'}
    url = f'http://{host}:{port}/api/auth/login'

    result = requests.post(url, json=user_id, headers=headers)
    data = json.loads(result.content)

    if result.status_code == 200:
        token = data['token']
        return token
    else:
        logging.error(f'status response: {data["status"]} -- {data["message"]}')
        return None


def get_tenant_devices(host='localhost', port='9090', token='', **kwargs):
    """ Returns tenant attached devices """
    headers = {'Accept': 'application/json', 'X-Authorization': f'Bearer {token}'}
    url = f'http://{host}:{port}/api/tenant/devices?'

    deviceType = kwargs.get('deviceType')
    textSearch = kwargs.get('textSearch')
    idOffset = kwargs.get('idOffset')
    textOffset = kwargs.get('textOffset')
    limit = kwargs.get('limit')

    if deviceType:
        url += f'type={deviceType}&'
    if textSearch:
        url += f'textSearch={textSearch}&'
    if idOffset:
        url += f'idOffset={idOffset}&'
    if textOffset:
        url += f'textOffset={textOffset}&'
    if limit:
        url += f'limit={limit}&'

    url = url[:-1]
    result = requests.get(url, headers=headers)
    data = json.loads(result.content)

    if result.status_code == 200:
        devices = data['data']
        return devices
    else:
        logging.error(f'status response: {data["status"]} -- {data["message"]}')
        return None


def get_device_id(device):
    """ Returns devices ids """
    if isinstance(device, list):
        ids = [item['id']['id'] for item in device]
        return ids
    else:
        return device['id']['id']


def get_device_name(device):
    """ Returns devices names """
    if isinstance(device, list):
        ids = [item['name'] for item in device]
        return ids
    else:
        return device['name']


def get_device_credential(device_id='', host='localhost', port='9090', token=''):
    """ Returns a single device credential : String"""
    headers = {'Accept': 'application/json', 'X-Authorization': f'Bearer {token}'}
    url = f'http://{host}:{port}/api/device/{device_id}/credentials'

    result = requests.get(url, headers=headers)
    data = json.loads(result.content)

    if result.status_code == 200:
        credential = data['credentialsId']
        return credential
    else:
        logging.error('status response: {data["status"]} -- {data["message"]}')
        return None


def get_devices_credentials(devices_list=(), host='localhost', port='9090', token=''):
    """ Returns a dictionary with device id and its credential {id : credential}"""
    if len(devices_list) == 0:
        return None
    else:
        get = get_device_credential
        credentials = {device: get(device, host, port, token) for device in devices_list}
        return credentials


def create_device(device_name, device_type, device_label='', host='localhost', port='9090', token=''):
    """ create a single device and returns its the TB object """
    headers = {'Content-Type': 'application/json', 'Accept': 'application/json', 'X-Authorization': f'Bearer {token}'}
    url = f'http://{host}:{port}/api/device'
    device = {"name": device_name, "type": device_type, "label": device_label}

    result = requests.post(url, headers=headers, json=device)
    data = json.loads(result.content)

    if result.status_code == 200:
        return data
    else:
        logging.error(f'status response: {data["status"]} -- {data["message"]}')
        return None


def get_station_location(host='localhost', port='9090', token=''):
    # headers = {'Content-Type': 'application/json', 'Accept': 'application/json', 'X-Authorization': f'Bearer {token}'}
    # url = f'http://{host}:{port}/api/device'
    # device = {"name": device_name, "type": device_type, "label": device_label}
    pass


def create_dashboard(token):
    token = f'Bearer {token}'
    return NotImplemented


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
