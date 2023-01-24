# -*- coding: utf-8 -*-
"""
Created By: Volpe National Transportation Systems Center
Created on Thu Nov 10 08:05:52 2022
Based On: TNMAide.xlsm created by Cambridge Systematics 

Last Revision on June 9, 2022
VERSION 2.0

@author: aaron.hastings
"""

import pandas as pd
import numpy as np

# This script computes sound levels at a 50 ft reference location using:
# Traffic Data fron DANA: Vol and Speed by Vehicle Type
#                         For each hour of each day over year (std and leap)
#                         For up to two links
# TNM REMELs Equations
# Assumed 
#
# ASSUMPTIONS:
# 1) Links are "infintely" long
# 2) All traffic on link is at center of lane nearest reference location (see ascii sketch)
#         TBD: This could be improved by distributing traffic and accounting for additional distance
# 3) Free-field divergence to account for far link (far lane) uses basic following model 
#         far lane correction = 15*log10(dist(near lane middle) / dist(far lane middle)) 
#
#         Far Link Lanes       Median          Near Link Lanes          Ref Mic        
#      |        v         | x x x x x x x x |         ^        | 
#      |        v         | x x x x x x x x |         ^        | 
#      |        v         | x x x x x x x x |         ^        | 
#      |        v         | x x x x x x x x |         ^        | 
#      |        v         | x x x x x x x x |         ^        |             o
#      |        v         | x x x x x x x x |         ^        | 
#      |        v         | x x x x x x x x |         ^        | 
#      |        v         | x x x x x x x x |         ^        | 
#      |        v         | x x x x x x x x |                  | 