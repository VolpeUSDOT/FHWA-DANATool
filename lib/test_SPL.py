# -*- coding: utf-8 -*-
"""
Created on Tue Nov  8 18:27:43 2022

@author: aaron.hastings
"""

import pandas as pd
import numpy as np
import array as arr
from Sound_Pressure_Level_Metrics import SoundPressureLevel as spl

times = pd.date_range("00:00", "23:00", freq="60min")

df = pd.DataFrame(np.random.randn(len(times), 4), 
                  index=times, columns=['A', 'B', 'C', 'spl'])



df['spl'] = 60

print(spl.Leq_24_Hour(df, 'spl'))

l = ['1','2']

print(spl.Leq_24_Hour(l, 'spl'))


l = [1,2]

print(spl.Leq_24_Hour(l, 'spl'))

b = arr.array('d', [2.5, 3.2, 3.3])

print(spl.Leq_24_Hour(b, 'spl'))

t = ('1','2')

print(spl.Leq_24_Hour(t, 'spl'))


# SHould fail
print(spl.Leq_24_Hour(df))

l = ['1','a']

print(spl.Leq_24_Hour(l, 'spl'))





# print(spl.LDN(df, 'spl'))
# print(spl.LDEN(df, 'spl'))