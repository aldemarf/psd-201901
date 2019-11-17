import numpy as np
from statistics import *

import distance


def idw_weight(x, power=1):
    return 1 / (x ** power)


def idw(heat_indexes, distances, power):
    hi_dist = {staCode if distance > 0
               else 'hi_value':
               [distance, heat_indexes[staCode]] if distance > 0
               else heat_indexes[staCode]
               for staCode, distance in distances.items()}

    if 'hi_value' in hi_dist:
        return hi_dist['hi_value']

    weights = 0
    numerator = 0
    for staCode, (distance, hi) in hi_dist:
        weight = idw_weight(distance)
        weights += weight
        numerator += hi * weight

    return numerator / weights


def dw(heat_indexes, distances):
    hi_dist = {staCode if distance > 0
               else 'hi_value':
                   [distance, heat_indexes[staCode]] if distance > 0
                   else heat_indexes[staCode]
               for staCode, distance in distances.items()}

    if 'hi_value' in hi_dist:
        return hi_dist['hi_value']

    distance_sum = 0
    numerator = 0
    for staCode, (distance, hi) in hi_dist:
        distance_sum += distance
        numerator += hi * distance

    return numerator / distance_sum


# distances = distance.nearest(-8.063149, -34.871098, 8, 'haversine')
# hi = list()
# hi = idw(hi, distances, 1)
