from math import acos, cos, sin, asin, sqrt, radians


def dist_haversine(lat1, lon1, lat2, lon2, radius=6371):
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
