''' this file is originally in fault_injection folder
'''

import json
import os 
import time
import itertools

from rca4tracing.fault_injection.generate_test import CaseGenerator
from rca4tracing.fault_injection.data_collector import DataCollector #collect
from rca4tracing.rca.shapley_value_rca import ShapleyValueRCA
from rca4tracing.datasources.jaeger.driver import JaegerDriver

from rca4tracing.rca.experiment.get_rca_data_jaeger import get_callers_of_anomalies

from rca4tracing.common.utils import get_proj_dir

from rca4tracing.rca.experiment.evaluator import Evaluator

proj_dir = get_proj_dir()
result_folder_name = os.path.join(proj_dir, 'rca4tracing/fault_injection/results')
data_folder_name = os.path.join(proj_dir, 'rca4tracing/fault_injection/data')

from rca4tracing.fault_injection.config import system_type, web_host, jaeger_host

def get_look_back_seconds(normal_seconds, anomaly_seconds, services_delay):
    look_back_seconds = int((normal_seconds + anomaly_seconds) \
            + len(services_delay)*30) + 60 # recent one minute
    # look_back_seconds = 1800
    return look_back_seconds

def prepare_data(services_delay, 
                 users=1,
                 spawn_rate=1,
                 normal_seconds=5, 
                 anomaly_seconds=5,
                 data_filename=None):
    # inject_for_services(services_delay,
    #                     normal_seconds=normal_seconds, 
    #                     anomaly_seconds=anomaly_seconds)
    case_generator = CaseGenerator(services_delay=services_delay,
                                   normal_seconds=normal_seconds,
                                   anomaly_seconds=anomaly_seconds,
                                   users=users,
                                   spawn_rate=spawn_rate,
                                   system_type=system_type,
                                   web_host=web_host,
                                   jaeger_host=jaeger_host)
    start_time = case_generator.generate()

    # look_back_seconds = get_look_back_seconds(normal_seconds, anomaly_seconds, services_delay)    
    look_back_seconds = int(time.time() - start_time)

    data_collector = DataCollector(host=jaeger_host)
    traces, trace_ids = data_collector.collect(look_back_seconds=look_back_seconds, 
                                            minDuration_us=min(services_delay.values())*1000,
                                            service=list(services_delay.keys())[-1], # if the last injected is anomaly, all is anomaly
                                            anomaly_condition_ms=services_delay
                                            )
    if len(traces) != len(trace_ids):
        print ('Error! len(traces) != len(trace_ids)')
    data = []
    for idx in range(len(trace_ids)):
        data.append({'trace': json.dumps(traces[idx]), 'traceID': trace_ids[idx]})
        trace_filename = data_folder_name + '/traces/' + trace_ids[idx] + '.json'
        with open(trace_filename, 'w+') as f:
            json.dump(traces[idx], f, indent=4)
    with open(data_filename, 'w+') as f:
        json.dump(data, f, indent=4)

# def run_one_trace_id_old(trace, services_delay, evaluation_list):
#     shapley_rca = ShapleyValueRCA()
#     ret = shapley_rca.analyze_traces([trace], strategy='avg_by_prob', sort_result=True)
#     evaluation = evaluate_one_trace(services_delay.keys(), trace, ret)
#     evaluation['traceID'] = trace_ids[idx]
    
#     # print (ret)
#     evaluation_list.append(evaluation)
#     print (evaluation)
#     return evaluation

def run_one_trace_id(evaluator, 
                    trace_id, 
                    anomaly_conditions={}, 
                    root_causes=[]):
    anomaly_traces_with_root_causes = {
        trace_id: root_causes
    }
    evaluator.evaluate(anomaly_traces_with_root_causes=anomaly_traces_with_root_causes,
                       anomaly_conditions=anomaly_conditions,
                       mode='trace') 

def run_operations(evaluator, 
                   anomaly_conditions={}, 
                   look_back_seconds=900,
                   timestamp=None, 
                   root_causes=[],
                   mode='operation'):
    evaluator.evaluate(anomaly_conditions=anomaly_conditions,
                       look_back_seconds=look_back_seconds,
                       timestamp=timestamp,
                       root_causes=root_causes,
                       mode=mode)

def get_evaluator(annotation='', select_root_cause_num=1, top_k_list=[1,2,3,4,5]):
    input_path = 'rca4tracing/rca/experiment/input_data/experiment_jaeger/'
    output_path='rca4tracing/rca/experiment/output_data/experiment_jaeger/'    

    algorithm_with_params={
        'MicroHECL': {}, 
        'MicroRCA': {'operation_only': True}, 
        'TON': {'operation_only': True}, 
        'MicroRank': {'n_sigma': 1}, 
        'ShapleyValueRCA': {'using_cache': False, 'strategy': 'ave_by_prob'},
        # 'CauseInfer': {}
    }

    evaluator = Evaluator(input_path, output_path,
                          algorithm_with_params=algorithm_with_params,
                          top_k_list=top_k_list,
                          data_source='jaeger',
                          annotation = annotation,
                          select_root_cause_num=select_root_cause_num)
    return evaluator

def get_last_timestamp(traces):
    last_request_timestamp = -1
    for trace in traces:
        timestamp = trace[0]['timestamp'] // (10**6) # TODO: 13 digits?
        if timestamp > last_request_timestamp:
            last_request_timestamp = timestamp
    return last_request_timestamp

def run_one_experiment(services_delay, 
                       users=1,
                       spawn_rate=1,
                       normal_seconds=5, 
                       anomaly_seconds=5,
                       donot_collect_data=False,
                       evaluator=None,
                       sleep_seconds=90,
                       mode='trace'): # 'trace', 'operation', 'global'
    # if there exists data, use the old data
    key_value_pairs = []
    for service in services_delay:
        key_value_pairs.append(f"{service}{services_delay[service]}")
    filename = "_".join( key_value_pairs ) + f"_users{users}_spawn_rate{spawn_rate}" + '.json'
    data_filename = data_folder_name + '/' + filename
    print (data_filename)
    if not os.path.isfile(data_filename):
        if donot_collect_data:
            return # do not collect data
            
        prepare_data(services_delay, 
                     users=users,
                     spawn_rate=spawn_rate,
                     normal_seconds=normal_seconds, 
                     anomaly_seconds=anomaly_seconds,
                     data_filename=data_filename)
        # only apply the sleep after injection
        time.sleep(sleep_seconds) # to ensure the got traces are about the current fault
    # else:
    #     time.sleep(5) # sleep for a while

    with open(data_filename, 'r') as f:
        data = json.load(f)
        traces = [json.loads(item['trace']) for item in data]
        trace_ids = [item['traceID'] for item in data]

    last_request_timestamp = get_last_timestamp(traces)

    print (f'********************* {list(services_delay.keys())} *********************')
    look_back_seconds = get_look_back_seconds(normal_seconds, anomaly_seconds, services_delay)
    root_causes = list(services_delay.keys())
    if mode == 'trace':
        for idx in range(len(traces)):
            ''' replace this part by using run_rca
            '''
            trace = traces[idx]
            trace_id = trace_ids[idx]
            # run_one_trace_id_old(trace, services_delay, evaluation_list)
            print (f"run_one_trace_id: {trace_id}")
            run_one_trace_id(evaluator, 
                             trace_id=trace_id,  
                             anomaly_conditions=services_delay,
                             root_causes=root_causes)
    elif mode == 'operation':
        minDuration_ms = min(services_delay.values())
        affected_operations = find_affected_nodes(traces, minDuration_ms*1000)
        for affected_operation in affected_operations:
            run_operations(evaluator, 
                           anomaly_conditions = {affected_operation: minDuration_ms}, 
                           timestamp = last_request_timestamp,
                           look_back_seconds = look_back_seconds,
                           root_causes = root_causes,
                           mode = 'operation')
    elif mode == 'global':
        # get all the abnormal traces and analyze
        run_operations(evaluator, 
                       anomaly_conditions = services_delay, 
                       timestamp = last_request_timestamp,
                       look_back_seconds = look_back_seconds,
                       root_causes=root_causes, 
                       mode='global')

def find_affected_nodes(traces, minDuration_us):
    ''' find the services who has operations whose duration > minDuration_us
    '''
    affected_nodes = set()
    for trace in traces:
        for span in trace:
            if span['elapsed'] > minDuration_us:
                affected_nodes.add(span['serviceName'])
    return affected_nodes

def get_all_services():
    # services = JaegerDriver().get_all_services(lookback_seconds=3600*24*30)
    # services = ['ts-preserve-service']
    services = ['ts-train-service', 'ts-order-other-service', 'ts-payment-service', 'ts-config-service', 'ts-ticketinfo-service', 'ts-security-service', 'ts-seat-service', 'ts-station-service', 'ts-food-service', 'ts-order-service', 'ts-inside-payment-service', 'ts-preserve-service', 'ts-route-service', 'ts-consign-service', 'ts-contacts-service', 'ts-price-service', 'ts-basic-service', 'ts-food-map-service', 'ts-consign-price-service', 'ts-travel2-service', 'ts-travel-service', 'ts-execute-service']
    return services

def evaluate_one_trace(anomaly_services, trace, rca_result):
    # because the network delay affects the caller, we should currently use the caller as the groundtruth
    # true_anomalies = get_callers_of_anomalies(anomaly_services, trace)

    # current version: we know the exact anomaly
    true_anomalies = list(anomaly_services.keys())
    # print (f'true anomalies: {true_anomalies}')
    result = dict()
    for top_k in [1, 2, 3, 4]:
        if len(rca_result) < top_k:
            continue

        rca_services = [item.split(':')[0] for item in rca_result]
        dedup_result = []
        for service in rca_services:
            if service not in dedup_result:
                dedup_result.append(service)

        top_service_set = set(dedup_result[:top_k])
        result[f'top{top_k}_recall'] = int(true_anomalies.issubset(top_service_set))
    result['true_anomalies'] = list(true_anomalies)
    result['rca_result'] = rca_result
    # result['trace'] = json.dumps(trace)
    return result   


def get_summary():
    service_list = get_all_services()
    metric_list = ['top1_recall', 'top3_recall']
    count_dict = dict()
    for metric in metric_list:
        count_dict[metric] = 0

    total = 0

    for service in service_list:
        filename = service+'.json'
    
        full_filename = result_folder_name+'/'+filename
        if os.path.isfile(full_filename):
            with open(full_filename, 'r') as f:
                evaluation_list = json.load(f)
                for evaluation in evaluation_list:
                    # added on 2022-3-11: if more than one anomalies, skip
                    if len(evaluation['true_anomalies']) > 1:
                        continue 
                    total += 1
                    for metric in metric_list:
                        count_dict[metric] += evaluation[metric]

    for metric in metric_list:
        print (f'average {metric} is {count_dict[metric]/total}')
    print (f'number of cases: {total}')

def run_all_experiments(sleep_seconds=0, 
                        donot_collect_data=True, 
                        root_cause_num=1,
                        service_combs=[],
                        delay_list = [200], #[100, 200, 500, 1000]
                        users_list = [5], #[1, 5, 10, 20]
                        spawn_rate_list = [5], #[1, 5, 10, 20]
                        exp_name='jaeger_trace' # matches the figure_name
    ):
    ''' only one injected operation
    '''
    service_list = get_all_services()
    
    params_to_str = f"{delay_list}{users_list}{spawn_rate_list}{[]}"
    
    if not service_combs:
        service_combs = list(itertools.combinations(set(service_list), root_cause_num))

    evaluator = get_evaluator(annotation=exp_name + params_to_str, 
                              select_root_cause_num=root_cause_num
                            )

    for service_comb in service_combs:         
        for delay in delay_list:
            services_delay = {}
            for service in service_comb:
                services_delay[service] = delay
            # services_delay = {
            #     service: delay
            # }  
            for users in users_list:
                for spawn_rate in spawn_rate_list:   
                                     
                    normal_seconds = 15
                    anomaly_seconds = 20 #30
                    run_one_experiment(services_delay, 
                                    users=users, 
                                    spawn_rate=spawn_rate,
                                    normal_seconds=normal_seconds, 
                                    anomaly_seconds=anomaly_seconds,
                                    evaluator=evaluator,
                                    donot_collect_data=donot_collect_data, # by default, use the injected faults
                                    sleep_seconds=sleep_seconds)
                    # try:
                    #     run_one_experiment(services_delay, 
                    #                 users=users, 
                    #                 spawn_rate=spawn_rate,
                    #                 normal_seconds=normal_seconds, 
                    #                 anomaly_seconds=anomaly_seconds,
                    #                 evaluator=evaluator,
                    #                 donot_collect_data=donot_collect_data, # by default, use the injected faults
                    #                 sleep_seconds=sleep_seconds)
                    # except Exception as e:
                    #     print (f"Exception: {str(e)}")

def run_multiple_root_causes(collect_data):
    service_list = ['ts-travel2-service', 'ts-ticketinfo-service',
                'ts-route-service', 'ts-station-service',
                'ts-basic-service', 'ts-travel-service',
                'ts-ticketinfo-service', 'ts-order-servic',
                'ts-preserve-service'
               ]

    service_combs = list(itertools.combinations(set(service_list), 2))

    run_all_experiments(sleep_seconds=90, 
                        donot_collect_data=(not collect_data), 
                        root_cause_num=2,
                        service_combs=service_combs,
                        exp_name='jaeger_multi_root_cause')

def run_single_root_cause(collect_data):
    run_all_experiments(sleep_seconds=90, 
                        donot_collect_data=(not collect_data), 
                        root_cause_num=1,
                        service_combs=[],
                        exp_name='jaeger_trace')
    
def run_different_delay(collect_data):
    for delay in [500]:#, 200, 500, 1000]:
        run_all_experiments(sleep_seconds=90, 
                            donot_collect_data=(not collect_data), 
                            root_cause_num=1,
                            service_combs=[],
                            delay_list=[delay],
                            exp_name='different_delay'
                            )


def run_different_users(collect_data):
    for user in [1, 10, 20]:
        run_all_experiments(sleep_seconds=90, 
                            donot_collect_data=(not collect_data), 
                            root_cause_num=1,
                            service_combs=[],
                            users_list=[user],
                            exp_name='different_users')

if __name__ == '__main__':
    # run_different_delay(collect_data=False)

    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('--exp',
                        help='run which experiment',
                        default='trace')
    parser.add_argument('--collect_data',
                        help='whether to collect the data',
                        default=False)
    args = parser.parse_args()

    # collect_data = False # whether to collect the data
    collect_data = args.collect_data
    if args.exp == 'multiple_root_causes':
        run_multiple_root_causes(collect_data)
    elif args.exp == 'trace': # the default param
        run_single_root_cause(collect_data)

    # elif args.exp == 'different_delay':
    #     run_different_delay(collect_data)

    # elif args.exp == 'different_users':
    #     run_different_users(collect_data)

    
