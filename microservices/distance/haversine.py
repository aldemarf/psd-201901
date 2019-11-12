from thingsboard.api import *

from math import acos, cos, sin, asin, sqrt, radians


def haversine(lat1, lon1, lat2, lon2, radius=6371):
    """ Earth radius = 6.371km """

    delta_lat = radians(lat2 - lat1)
    delta_lon = radians(lon2 - lon1)
    lat1 = radians(lat1)
    lat2 = radians(lat2)

    distance = 2 * radius * asin(
        sqrt(
            sin(delta_lat / 2) ** 2
            + cos(lat1) * cos(lat2) * sin(delta_lon / 2) ** 2
        )
    )
    return distance


def get_stations_location(station_type='ESTAÇÃO METEOROLÓGICA'):
    tenant_token = get_tenant_token()
    tenant_devices = get_tenant_devices(token=tenant_token, limit='10000')

    devices = {device['name']: [device['id']['id'], device['id']['entityType']]
               for device in tenant_devices
               if device['type'] == station_type}

    get_lat_lon = get_latest_telemetry_wo_timestamp
    stations_coordinates = {name: get_lat_lon(id_, type_, token=tenant_token, keys='latitude,longitude')
                            for name, (id_, type_) in devices.items()}

    return stations_coordinates


def calc_distance(lat='', lon='', method='haversine'):
    if method == 'haversine':

        stations_distances = get_stations_location()
        for station, coordinates in stations_distances.items():
            dist = haversine(lat, lon, float(coordinates['latitude']), float(coordinates['longitude']))
            stations_distances[station]['distance'] = dist

        return stations_distances
