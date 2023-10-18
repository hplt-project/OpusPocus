#!/usr/bin/env python3
import sys

time = [int(x) for x in str(sys.argv[1]).split(":")]
time_s = 0
for i in range(len(time) - 1):
    time_s = 60 * (time_s + time[i])
time_s += time[-1]
print(time_s)
