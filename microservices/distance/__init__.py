from distance.method import haversine, euclidian
from thingsboard.api import *


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
    stations_distances = get_stations_location()

    if method.lower() == 'haversine':
        for station, coordinates in stations_distances.items():
            dist = haversine(lat, lon, float(coordinates['latitude']), float(coordinates['longitude']))
            stations_distances[station]['distance'] = dist

    elif method.lower() == 'euclidian':
        for station, coordinates in stations_distances.items():
            dist = euclidian(lat, lon, float(coordinates['latitude']), float(coordinates['longitude']))
            stations_distances[station]['distance'] = dist

    return stations_distances


def nearest(lat, lon, n=1, method='haversine'):
    stations = calc_distance(lat, lon, method=method)
    stations = [(item[0], item[1]['distance']) for item in stations.items()]
    stations.sort(key=lambda item: item[1])
    n_nearest = stations[:n]
    return n_nearest


# print(nearest(-8.063149, -34.871098, 8, 'haversine '))  # Marco-Zero coord.
# print(nearest(-8.063149, -34.871098, 8, 'euclidian'))  # Marco-Zero coord.

