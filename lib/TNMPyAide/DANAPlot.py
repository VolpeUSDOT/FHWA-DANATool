# -*- coding: utf-8 -*-
"""
Created on Wed May 17 08:05:41 2023

@author: aaron.hastings
"""

import matplotlib.pyplot as plt
import matplotlib
import numpy as np
import math
from IPython import get_ipython
from inspect import currentframe, getframeinfo

class DANAPlot:

    # plt.style.available
    style = 'tableau-colorblind10'
    
    backend_options = ['module://ipykernel.pylab.backend_inline', 'Qt5Agg', 'TkAgg']
    backend =  backend_options[2]
    # See also https://matplotlib.org/stable/users/explain/backends.html
    # useage = 
    plt.switch_backend(backend)
    
    axis_params = {'visible' : True, 
                   'which' : 'major', 
                   'axis' : 'both', 
                   'color' : 'black', 
                   'linestyle' : '--', 
                   'linewidth' : 0.5,
                   'alpha' : 0.5,
                   'zorder' : 0}
    
    line_plot_params = {'visible' : True, 
                        'color' : 'r', 
                        'linestyle' : ['solid', 'dotted','dashed', 'dashdot'], 
                        'linewidth' : 2,
                        'marker' : ['','o','v','>','<','v','^'],
                        'zorder' : 2}
    
    bar_plot_params = {'visible' : True, 
                        'edgecolor' : 'white', 
                        'linewidth' : 0.7,
                        'zorder' : 2}
    
    # for list of makers, see https://matplotlib.org/stable/api/markers_api.html
    
    @classmethod # Want access to class variables
    def Bar_Plot(cls, x, y, xlabel = 'X', ylabel = 'Y', title = '', width = 1, xlims = None, ylims = None, xtickstep = None, ytickstep = None, fig = None, ax = None):
        x = np.nan_to_num(x, neginf=0)
        y = np.nan_to_num(y, neginf=0)

        # Handle missing inputs
        if xlims is None:
            xlims = (math.floor(np.amin(x)), math.ceil(np.amax(x)))
            
        if ylims is None:
            ylims = (math.floor(np.amin(y)), 1.1 * math.ceil(np.amax(y)))
        
        if xtickstep is None:
            xtickstep = round( ( math.ceil(np.amax(x)) - math.floor(np.amin(x)) ) / 10 )
            if xtickstep <= 0:
                xtickstep = ( math.ceil(np.amax(x)) - math.floor(np.amin(x)) ) / 10
        
        if ytickstep is None:
            ytickstep = round( ( math.ceil(np.amax(y)) - math.floor(np.amin(y)) ) / 10 )
            if ytickstep <= 0:
                ytickstep = ( math.ceil(np.amax(y)) - math.floor(np.amin(y)) ) / 10

        plt.style.use(cls.style)
        plt.switch_backend(cls.backend)
        
        fig, ax = DANAPlot.Set_Ax(fig = fig, ax = ax)

        ax.bar(x.reshape(-1), y.reshape(-1), 
               width=width, 
               edgecolor=cls.bar_plot_params['edgecolor'], 
               linewidth=cls.bar_plot_params['linewidth'], 
               zorder=cls.bar_plot_params['zorder'])
        
        ax.set(xlim=xlims, xticks=np.arange(xlims[0], xlims[1], xtickstep))
        ax.set(ylim=ylims, yticks=np.arange(ylims[0], ylims[1], ytickstep))
        ax.set(xlabel = xlabel)
        ax.set(ylabel = ylabel)
        ax.set(title=title)
        plt.grid(visible=cls.axis_params['visible'], 
                 which=cls.axis_params['which'], 
                 axis=cls.axis_params['axis'], 
                 color=cls.axis_params['color'], 
                 linestyle=cls.axis_params['linestyle'],
                 linewidth=cls.axis_params['linewidth'],
                 alpha=cls.axis_params['alpha'])
        
        fig.set_tight_layout(True)
        plt.show() # Needed for some backends, creates a new instance of same figure (not a new figure) with updated information
        return fig, ax
    
    @staticmethod
    # Plot a histogram
    def Histogram(data, bin_centers, xlabel = 'X', ylabel = 'Count', title = '', width = None, xlims = None, ylims = None, xtickstep = None, ytickstep = None):
        # bin_centers = numpy array

        # Handle missing inputs
        if xtickstep is None:
            xtickstep = bin_centers[1] - bin_centers[0]
        if width is None:
            width = xtickstep

        bin_edges = bin_centers.reshape(-1) - xtickstep / 2
        bin_edges = np.append(bin_edges, bin_edges[-1] + xtickstep)
                
        counts, bin_edges = np.histogram(data, bin_edges)
        
        if ytickstep is None:
            ytickstep = round( ( math.ceil(np.amax(counts)) - math.floor(np.amin(counts)) ) / 10 )
            if ytickstep <= 0:
                ytickstep = round( ( math.ceil(np.amax(counts)) - math.floor(np.amin(counts)) ) / 10, 1 )
            if ytickstep <= 0:
                ytickstep = ( math.ceil(np.amax(counts)) - math.floor(np.amin(counts)) ) / 10
            
        if xlims is None:
            xlims = (math.floor(np.amin(bin_centers)) - xtickstep, math.ceil(np.amax(bin_centers)) + xtickstep)
            
        if ylims is None:
            ylims = (0, 1.1 * math.ceil(np.amax(counts)))
        
        fig, ax = DANAPlot.Bar_Plot(bin_centers, counts, xlabel = xlabel, ylabel = ylabel, title = title, width = width, xlims = xlims, ylims = ylims, xtickstep = xtickstep, ytickstep = ytickstep)
        fig.set_tight_layout(True)
        return [fig, ax]

    @classmethod # Want access to class variables
    # Create a new line plot for one or many lines
    def Line_Plot(cls, x, y, xlabel = 'X', ylabel = 'Y', title = '', xlims = None, ylims = None, xtickstep = None, ytickstep = None, fig = None, ax = None):
        x = np.nan_to_num(x, neginf=0)
        y = np.nan_to_num(y, neginf=0)
        
        # Handle missing inputs
        if xlims is None:
            xlims = (math.floor(np.amin(x)), math.ceil(np.amax(x)))
            
        if ylims is None:
            ylims = (0.9 * math.floor(np.amin(y)), 1.1 * math.ceil(np.amax(y)))
        
        if xtickstep is None:
            xtickstep = round( ( math.ceil(np.amax(x)) - math.floor(np.amin(x)) ) / 10 )
            if xtickstep <= 0:
                xtickstep = ( math.ceil(np.amax(x)) - math.floor(np.amin(x)) ) / 10
        
        if ytickstep is None:
            ytickstep = round( ( math.ceil(np.amax(y)) - math.floor(np.amin(y)) ) / 10 )
            if ytickstep <= 0:
                ytickstep = ( math.ceil(np.amax(y)) - math.floor(np.amin(y)) ) / 10

        plt.style.use(cls.style)
        plt.switch_backend(cls.backend)
        styles = [('solid', 'solid'),
                ('densely dotted',        (0, (1, 1))),
                ('long dash with offset', (5, (10, 3))),
                ('loosely dashed',        (0, (5, 10))),
                ('densely dashed',        (0, (5, 1))),

                ('loosely dashdotted',    (0, (3, 10, 1, 10))),
                ('dashdotted',            (0, (3, 5, 1, 5))),
                ('densely dashdotted',    (0, (3, 1, 1, 1))),

                ('dashdotdotted',         (0, (3, 5, 1, 5, 1, 5))),
                ('densely dashdotdotted', (0, (3, 1, 1, 1, 1, 1)))]
        
        fig, ax = DANAPlot.Set_Ax(fig = fig, ax = ax)
        if x.ndim == 1:
            ax.plot(x, y, 
                    linewidth=cls.line_plot_params['linewidth'],
                    zorder=cls.line_plot_params['zorder'])
        
        else:
            for row in range(0,len(y),1):
                ax.plot(x[row], y[row], linestyle=styles[row][1],
                        linewidth=cls.line_plot_params['linewidth'],
                        zorder=cls.line_plot_params['zorder'])
                
        ax.set(xlim=xlims, xticks=np.arange(xlims[0], xlims[1], xtickstep))
        ax.set(ylim=ylims, yticks=np.arange(ylims[0], ylims[1], ytickstep))
        ax.set(xlabel = xlabel)
        ax.set(ylabel = ylabel)
        ax.set(title=title)
        
        plt.grid(visible=cls.axis_params['visible'], 
                 which=cls.axis_params['which'], 
                 axis=cls.axis_params['axis'], 
                 color=cls.axis_params['color'], 
                 linestyle=cls.axis_params['linestyle'],
                 linewidth=cls.axis_params['linewidth'],
                 alpha=cls.axis_params['alpha'])
        
        fig.set_tight_layout(True)
        return fig, ax
    
    @staticmethod 
    # Add legend entries to existing plot
    def Add_Legend(fig, ax, labels):
        plt.figure(fig.number)
        plt.sca(ax)
        lines = ax.get_lines()
        idx = 0
        for line in lines:
            if idx < len(labels):
                line.set_label(labels[idx])
        
            idx = idx + 1
        ax.legend(bbox_to_anchor=(1.04, 1), borderaxespad=0)
        
    @staticmethod 
    # Create / set appropriate figure and axes
    def Set_Ax(fig = None, ax = None):
        if fig is None: 
            if ax is not None:
                fig = plt.figure()
                ax = plt.subplot(ax) # Watch the difference between subplot and subplots!!
                                     # ax should be something like 311 (schema) or 1 (pos) 
            else:
                fig, ax = plt.subplots() 
        elif ax is not None:
            if isinstance(ax, matplotlib.axes._axes.Axes):
                # Then the axes exists and we just need to make it current
                plt.figure(fig.number)
                plt.sca(ax)
            else:
                # A figure exists but we want a new axes or axes schema
                plt.figure(fig.number)
                ax = plt.subplot(ax) # ax should be something like 311 (schema) or 1 (pos) 
        else:
            print('Warning no axes handle or schema given, but expected. Creating a new figure with single subplot')
            frameinfo = getframeinfo(currentframe())
            print(frameinfo.filename, frameinfo.lineno)
            print('')
            fig, ax = plt.subplots() 

        return fig, ax