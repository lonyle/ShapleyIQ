import matplotlib.pyplot as plt
import matplotlib
import json
import numpy as np
font = {'size': 14, 'weight':'bold'}
matplotlib.rc('font', **font)


def mean_absolute_percentage_error(y_true, y_pred): 
    y_true, y_pred = np.array(y_true), np.array(y_pred)
    return np.mean(np.abs((y_true - y_pred) / y_true)) * 100

def plot_jaeger_inner(result, title):
    fig = plt.figure(figsize=(9,2.3))
    ax = fig.add_axes([0.1, 0.24, 0.89, 0.75])

    # linewidth=3, linestyle=linestyle_vec[idx],
                    # marker=marker_vec[idx], markersize=6, fillstyle='none', markeredgewidth=2

    zipped = zip(result['est_delay_list'], result['RT_average_list'], result['RT_max_list'], result['adjusted_delay_list'])
    zipped = sorted(zipped)
    result['est_delay_list'], result['RT_average_list'], result['RT_max_list'], result['adjusted_delay_list'] = zip(*zipped)


    # users_list = result['users_list']
    users_list = result['est_delay_list']
    ax.plot(users_list, result['est_delay_list'], label='estimation', color='blue', linewidth=3)
    ax.plot(users_list, result['RT_min_list'], '--', label='min RT', color='black', linewidth=2,
        marker='v', markersize=6, fillstyle='none', markeredgewidth=2)
    ax.plot(users_list, result['RT_average_list'], '--', label='average RT', color='black', linewidth=2,
        marker='o', markersize=6, fillstyle='none', markeredgewidth=2)
    ax.plot(users_list, result['RT_max_list'], '--', label='95% RT', color='black', linewidth=2,
        marker='^', markersize=6, fillstyle='none', markeredgewidth=2)
    plt.legend(loc='upper left', frameon=False, fontsize=14)
    # plt.xlabel('number of users (related to workload intensity)', weight='bold')
    plt.xlabel('estimation of delay by queueing model', weight='bold')
    plt.ylabel('delay or RT (ms)', weight='bold')
    path = 'rca4tracing/rca/experiment/plot/images/'
    plt.savefig(path + title + '.eps', dpi=1000)
    plt.show()

def plot_jaeger():
    filename = 'rca4tracing/rca/multiple_metric_rca/data/for_plot.json'
    result = json.load(open(filename))
    mape = mean_absolute_percentage_error(result['RT_median_list'], result['est_delay_list'])
    print (f'mape for jaeger: {mape}')

    mape_adjusted = mean_absolute_percentage_error(result['RT_median_list'], result['adjusted_delay_list'])
    print (f'mape after adjustment: {mape_adjusted}')

    plot_jaeger_inner(result, 'queue_estimate_jaeger')

    

def plot_dbaas():
    filename = 'rca4tracing/rca/multiple_metric_rca/data/dbaas/data.json'
    result = json.load(open(filename))

    for key in result: # remove the gc case
        result[key] = result[key][:-1]

    
    zipped = zip(result['est_delay_list'], result['RT_average_list'], result['RT_max_list'], result['adjusted_delay_list'])
    zipped = sorted(zipped)
    result['est_delay_list'], result['RT_average_list'], result['RT_max_list'], result['adjusted_delay_list'] = zip(*zipped)

    mape = mean_absolute_percentage_error(result['RT_average_list'], result['est_delay_list'])
    mape_adjusted = mean_absolute_percentage_error(result['RT_average_list'], result['adjusted_delay_list'])

    print (f'mape for dbaas: {mape}')
    print (f'mape after adjustment: {mape_adjusted}')

    fig = plt.figure(figsize=(9,2.3))
    ax = fig.add_axes([0.1, 0.24, 0.89, 0.75])
    ax.plot(result['est_delay_list'], result['est_delay_list'], label='estimation', color='blue', linewidth=3)    
    ax.plot(result['est_delay_list'], result['RT_average_list'], '--', label='average RT', color='black', linewidth=3,
        marker='o', markersize=6, fillstyle='none', markeredgewidth=2)
    ax.plot(result['est_delay_list'], result['RT_max_list'], '--', label='max RT', color='black', linewidth=3,
        marker='^', markersize=6, fillstyle='none', markeredgewidth=2)
    plt.yscale('log', base=10) 
    plt.legend(loc='lower right', frameon=False, fontsize=14)
    plt.xlabel('estimation of delay by queueing model', weight='bold')
    plt.ylabel('delay or RT (s)', weight='bold')

    path = 'rca4tracing/rca/experiment/plot/images/'
    plt.savefig(path + 'queue_estimate_dbaas' + '.eps', dpi=1000)

    plt.show()


if __name__ == '__main__':
    plot_jaeger()
    plot_dbaas()