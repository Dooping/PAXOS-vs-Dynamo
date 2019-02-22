# -*- coding: utf-8 -*-
"""
Created on Thu Nov 17 09:21:38 2016

@author: David Gago
"""


import numpy as np
import matplotlib.pyplot as plt
import matplotlib
matplotlib.style.use('ggplot')

datap = np.loadtxt('readsp.dat', delimiter = '\t')
datad = np.loadtxt('readsd.dat', delimiter = '\t')

datap = np.reshape(datap, -1, 'C')
datap = float(1)/datap

datad = np.reshape(datad, -1, 'C')
datad = float(1)/datad





plt.figure(figsize = (8,8), frameon = False)
plt.boxplot([datad,datap],showfliers = False)
plt.ylim((-1,10000))
plt.xticks([1,2,3],['dynamo','paxos'])
plt.yticks(np.arange(0, 10000, 500))
plt.gca().xaxis.grid(False)
plt.title('Operacoes por segundo')
plt.show
