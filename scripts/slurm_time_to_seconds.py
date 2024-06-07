#!/usr/bin/env python3
import sys


def hhmmss_to_seconds(t):
    t_s = 0
    for i in range(len(t) - 1):
        t_s = 60 * (t_s + t[i])
    return t_s + t[-1]

time_str = str(sys.argv[1])
time_s = 0

time_str_daytime = sys.argv[1].split('-')
if len(time_str_daytime) == 2:
    time_s = int(time_str_daytime[0]) * 24 * 60 * 60
time = [int(x) for x in time_str_daytime[-1].split(':')]
time_s += hhmmss_to_seconds(time)

print(time_s)
