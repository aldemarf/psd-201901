from math import sqrt

def ctf(temp):
    return (temp * 9 / 5 + 32)

def heatIndex(t=89.6, rh=35):

    hi = (1.1 * t) + (0.047 * rh) - 10.3

    if (hi < 80):
        return hi

    else:

        hi = -42.379 + (2.04901523 * t + 10.14333127 * rh)
        - (0.22475541 * t * rh) - (0.00683783 * t**2)
        - (0.05481717 * rh**2) + (0.00122874 * t**2 * rh)
        + (0.00085282 * t * rh**2) - (0.00000199 * t**2 * rh**2)


        # 80 <= T <= 112 && RH <= 13%
        if (rh < 13) and (t >= 80 and t <= 112):
            adjustmentSubtraction = ((13 - rh) / 4) * sqrt((17 - abs(t - 95) / 17))
            return hi - adjustmentSubtraction
        # 80 <= T <= 87 && RH > 85%
        elif (rh > 85) and (t>= 80 and t <= 87):
            adjustmentAddition = ((rh - 85) / 10) * ((87 - t) / 5)
            return hi + adjustmentAddition

        return hi