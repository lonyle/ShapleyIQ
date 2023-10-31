import json
import os
import numpy as np
import matplotlib.pyplot as plt
from collections import OrderedDict

from rca4tracing.rca.experiment.plot.get_bar_plot import alg_to_label

import matplotlib
font = {'size': 14, 'weight':'bold'}
matplotlib.rc('font', **font)

def plot_running_time(input_path, image_name, 
             algorithms=['ShapleyValueRCA', 'MicroHECL', 'MicroRCA', 'TON', 'MicroRank']):
    marker_vec = [None, 'o', 'x', 'D', None]
    linestyle_vec = [None, None, None, None, 'dashed']

    fig = plt.figure(figsize=(9,2.2))
    ax = fig.add_axes([0.08, 0.25, 0.9, 0.74])

    with open(input_path + 'running_time.json') as f:
        print (input_path + 'running_time.json')
        running_time = json.load(f)

    print_average_running_time(running_time)

    idx = 0
    for algorithm in algorithms:
        time_dict = running_time[algorithm]
        time_dict.pop('all')
        ordered_time_dict = OrderedDict(sorted(time_dict.items(), key=lambda x: int(x[0])))
        x_vec = list(ordered_time_dict.keys())
        x_vec = [int(x) for x in x_vec]
        y_vec = list(ordered_time_dict.values())
        y_vec = [y['average']*1000 for y in y_vec]

        if algorithm in alg_to_label:
            label = alg_to_label[algorithm]
        else:
            label = algorithm

        color = 'blue' if algorithm == 'ShapleyValueRCA' else 'black'
        # if x_vec[0] == 1:
        #     x_vec = x_vec[1:]
        #     y_vec = y_vec[1:]
        ax.plot(x_vec, y_vec, label=label, color=color, linewidth=3, linestyle=linestyle_vec[idx],
                marker=marker_vec[idx], markersize=6, fillstyle='none', markeredgewidth=2)

        idx += 1

        # if algorithm == 'ShapleyValueRCA':
        #     print (x_vec)
        #     print (y_vec)

    ax.set_yscale('log')

    plt.xlim([0, max(x_vec)*1.4])
    plt.ylabel('avg. time (ms)', weight='bold')
    plt.xlabel('number of spans in a trace', weight='bold')
    plt.legend(loc='upper right', frameon=False, fontsize=14)
    
    path = 'rca4tracing/rca/experiment/plot/images/'
    folder = os.path.exists(path)
    if not folder:
        os.makedirs(path)
    plt.savefig(path + image_name + '.eps', dpi=1000)
    plt.show()
    plt.close()

def print_average_running_time(running_time, algorithms=['ShapleyValueRCA', 'MicroHECL', 'MicroRCA', 'TON', 'MicroRank']):
    result_dict = dict()
    for algorithm in algorithms:
        time_dict = running_time[algorithm]

        if algorithm in alg_to_label:
            label = alg_to_label[algorithm]
        else:
            label = algorithm
        
        result_dict[ label ] = time_dict["all"]['average']

    print ("the average running time of different algorithms: (Table 4 in the paper, unit: second)\n", result_dict)
        

if __name__ == '__main__':
    prefix = 'rca4tracing/rca/experiment/output_data/'

    plot_running_time(prefix + 'experiment_jaeger/_result_summary_jaeger_trace[200][5][5][]_trace/', 
                      'jaeger_trace_time', 
                    # algorithms=['ShapleyValueRCA']
                    )
    
    


    