import os
import numpy as np
import matplotlib.pyplot as plt

import matplotlib
font = {'size': 14, 'weight':'bold'}
matplotlib.rc('font', **font)

data_dict = {
    'one lock': {
        "ShapleyRCA": 0.923076923,
        "Slowest": 0.461538462,
        "Deepest": 0.846153846
    },
    'two locks': {
        "ShapleyRCA": 0.833333333,
        "Slowest": 0,
        "Deepest": 0.666666667
    },
    'resource': {
        "ShapleyRCA": 1,
        "Slowest": 1,
        "Deepest": 0
    },
    'mixed lock/resource': {
        "ShapleyRCA": 1,
        "Slowest": 0.666666667,
        "Deepest": 0.5
    },
    'average': {
        "ShapleyRCA": 0.948717949,
        "Slowest": 0.631578947,
        "Deepest": 0.461538462
    }
}

def get_bar_graph(image_name, 
                  data_dict, # keys corresponds to the the x ticks
                  algorithms=['ShapleyRCA', 'Slowest', 'Deepest'], 
                  xtick = [],
                  ylabel='accuracy (%)'):      
    
    fill_vec = [True, False, False, False, False]
    hatch_vec = [None, None, 'xx', '//', '\\\\']

    fig = plt.figure(figsize=(9,1.7))
    # fig = plt.figure(figsize=(9,2.5))
    # ax = fig.add_axes([0.06, 0.09, 0.92, 0.88])
    ax = fig.add_axes([0.09, 0.17, 0.9, 0.82])
    
    y = dict()
    for algorithm in algorithms:
        y[algorithm] = []
    
    for score_dict in data_dict.values():
        for key, value in score_dict.items():
            if key in y:
                y[key].append(value*100)

    xticks = np.arange(len(xtick))

    width=0.23

    for idx in range(len(algorithms)):
        algorithm = algorithms[idx]
        label = algorithm
        ax.bar(xticks + width*idx, y[algorithm], width=width, label=label, \
                fill=fill_vec[idx], hatch=hatch_vec[idx], color='gray')

    for k in range(len(data_dict)):
        key = list(data_dict.keys())[k]
        for index in range(len(algorithms)):
            algorithm = algorithms[index]
            value = data_dict[key][algorithm] * 100
            plt.text(index*width+k, value+0.01, 100 if value==100 else round(value, 1), ha='center', fontsize=11)

    plt.xlim([-0.3, 6.3])
    plt.ylim([0, 112])
    # plt.title('precision rate')
    # plt.xlabel("top-k")
    plt.legend(loc='upper right', frameon=False, fontsize=14)
    plt.xticks(xticks+width, xtick)
    plt.ylabel(ylabel, weight='bold')
    

    path = 'rca4tracing/rca/experiment/plot/images/'
    folder = os.path.exists(path)
    if not folder:
        os.makedirs(path)
    plt.savefig(path + image_name + '.eps', dpi=1000)
    # plt.savefig(path + image_name + '.png', dpi=1000)
    
    plt.show()
    plt.close()

if __name__ == '__main__':
    # single lock- contention, 2) single resource-contention, 
    # 3) two lock-contentions and 4) mixed lock and resource contention.
    get_bar_graph('ps_bar',
                  data_dict, 
                  xtick=['one lock', 'two locks', 'resource', 'mixed', 'average'],
                  )