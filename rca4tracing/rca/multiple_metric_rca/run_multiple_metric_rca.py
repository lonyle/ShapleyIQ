''' use the data stored in file to do analysis 
'''
import json
import numpy as np 
import pandas as pd

from rca4tracing.rca.multiple_metric_rca.data.generate_data import prefix
from rca4tracing.rca.multiple_metric_rca.gamma_distributor import GammaDistributor
from rca4tracing.rca.multiple_metric_rca.queue_model import QueueEstimator

def load_data(data_id):
    metric_jaeger_filename = prefix + f"{data_id}_jaeger.json"
    metric_prom_filename = prefix + f"{data_id}_prom.json"

    with open(metric_jaeger_filename) as jaeger_file:
        stat_list = json.load(jaeger_file)

    with open(metric_prom_filename) as prom_file:
        multi_time_series = json.load(prom_file)

    return multi_time_series, stat_list 

def get_jaeger_stat(data_id):
    service_name, users, run_time = data_id.split('_')
    # prefix = "rca4tracing/fault_injection/workload_generator/output"
    prefix = "rca4tracing/rca/multiple_metric_rca/raw_data/trainticket_locust"
    filename = prefix + f'/result_{users}_{run_time}_stats.csv'

    df = pd.read_csv(filename)
    sub_df = df[df['Name'] == 'search_ticket_expected']
    qps = RT_median = RT_average = RT_max = RT_min = None
    for index, row in sub_df.iterrows():
        qps = row['Requests/s']
        RT_median = row['Median Response Time']
        RT_average = row['Average Response Time']
        # RT_max = row['Max Response Time']
        RT_min = row['Min Response Time']
        RT_max = row['95%']
    return qps, RT_median, RT_average, RT_max, RT_min


def estimate_wait(data_id):
    multi_time_series, stat_list = load_data(data_id)
    qps, RT_median, RT_average, RT_max, RT_min = get_jaeger_stat(data_id)

    RT_list = [stat['RT'] for stat in stat_list]
    timestamp_list = [stat['timestamp'] for stat in stat_list]

    normal_thread_count = 5
    thread_metrics = ['jvm_threads_state_timed-waiting']

    wait_count_list = np.zeros_like(multi_time_series['jvm_threads_state_timed-waiting'])
    for metric in thread_metrics:
        wait_count_list += np.asarray(multi_time_series[metric])
    wait_count_list -= normal_thread_count
    
    queue_est = QueueEstimator()
    qps_list = queue_est.timestamp2qps(timestamp_list)

    # delay_list = queue_est.queueing_model(wait_count_list, qps_list, qps_interval_len=30)
    delay_list = queue_est.queueing_model(wait_count_list, [qps], qps_interval_len=30)

    # queue_est.plot(RT_list, delay_list, normal_RT=10)   
    
    return delay_list 

def get_delta_RT(data_id, source='log', normal_RT=0):
    if source == 'trace':
        multi_time_series, stat_list = load_data(data_id)

        RT_list = [stat['RT'] for stat in stat_list]
        
        delta_RT_list = [RT-normal_RT for RT in RT_list]
        return np.average(delta_RT_list)
    elif source == 'log':
        qps, RT_median, RT_average, RT_max, RT_min = get_jaeger_stat(data_id)
        return RT_median - normal_RT

def estimate_gc(data_id):
    multi_time_series, stat_list = load_data(data_id)
    gc_pause_time = np.asarray(multi_time_series['gc_time']) * 1000 # use unit ms
    start_pos = (len(gc_pause_time)-1)//2+1
    return gc_pause_time[start_pos:]

def run(service_name, users, run_time, user_factor_time=0):
    data_id = f"{service_name}_{users}_{run_time}s"    

    delay_list = estimate_wait(data_id)
    gc_pause_time = np.max( estimate_gc(data_id) )

    mean_vec = [np.average(delay_list), gc_pause_time, user_factor_time]

    delta_RT = get_delta_RT(data_id) + user_factor_time # add the user factor time
    k_vec=[2, 10, 2]
    print ('before gamma distribution:', mean_vec, 'delta_RT:', delta_RT)
    gamma_distributor = GammaDistributor(mean_vec, k_vec, delta_RT)
    ret = gamma_distributor.distribute()
    print ('after gamma distribution:', ret)

    adjusted_delay = ret[0]

    return delta_RT, np.average(delay_list), adjusted_delay

def run_all_estimate():
    from rca4tracing.rca.multiple_metric_rca.data.generate_data import users_list
    service_name = 'ts-ticketinfo-service'
    # users_list = [1, 5, 10, 50, 100, 200, 500]

    delta_RT_list = []
    est_delay_list = []
    RT_average_list = []
    RT_median_list = []
    RT_max_list = []
    RT_min_list = []
    adjusted_delay_list = []
    for run_time in [30, 60, 90]:
        for users in users_list:
            delta_RT, est_delay, adjusted_delay = run(service_name, users, run_time)

            data_id = f"{service_name}_{users}_{run_time}s"
            qps, RT_median, RT_average, RT_max, RT_min = get_jaeger_stat(data_id)

            RT_median_list.append(RT_median)
            RT_average_list.append(RT_average)
            RT_max_list.append(RT_max)
            RT_min_list.append(RT_min)
            delta_RT_list.append(delta_RT)
            est_delay_list.append(est_delay)
            adjusted_delay_list.append(adjusted_delay)

    result = {
        'users_list': users_list,
        'est_delay_list': est_delay_list,
        'delta_RT_list': delta_RT_list,
        'RT_min_list': RT_min_list,
        'RT_median_list': RT_median_list,
        'RT_average_list': RT_average_list,
        'RT_max_list': RT_max_list,
        'adjusted_delay_list': adjusted_delay_list
    }
    # print (delta_RT_list, est_delay_list)
    # plt.plot(delta_RT_list, est_delay_list)

    import json
    filename = 'rca4tracing/rca/multiple_metric_rca/data/for_plot.json'
    with open(filename, 'w') as outfile:
        json.dump(result, outfile, indent=4)
    
if __name__ == '__main__':
    run_all_estimate()
    # run ('ts-ticketinfo-service', 200, run_time=60)
    # run ('ts-ticketinfo-service', 200, user_factor_time=1000, run_time=60) 