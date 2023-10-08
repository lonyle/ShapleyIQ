import os
import numpy as np
import json
import time
import matplotlib.pyplot as plt
from datetime import datetime
from dataclasses import dataclass

from rca4tracing.rca.experiment.run_rca import RunRCA

from rca4tracing.common.logger import setup_logger
LOG = setup_logger(__name__, module_name='rca')

def operation_to_service(rca_result):
    ''' for jaeger data, to post-process the results
    '''
    rca_services = [item.split(':')[0] for item in rca_result]
    dedup_result = []
    for service in rca_services:
        if service not in dedup_result:
            dedup_result.append(service)
    return dedup_result

class Evaluator:
    def __init__(self, 
                 input_path, 
                 output_path,
                 algorithm_with_params={
                     'MicroHECL': {}, 
                     'MicroRCA': {'operation_only': True}, 
                     'TON': {'operation_only': True}, 
                     'MicroRank': {}, 
                     'ShapleyValueRCA': {}
                 },
                 top_k_list=[1,2,3],
                 annotation = '',
                 data_source='dbaas',
                 select_root_cause_num=1 # specify the number of root causes 
                ):
        self.input_path = input_path
        self.output_path = output_path
        self.algorithm_with_params = algorithm_with_params
        self.top_k_list = top_k_list
        self.data_source = data_source
        self.select_root_cause_num = select_root_cause_num

        if self.data_source == 'jaeger':
            import rca4tracing.fault_injection.config as config
            if config.system_type == 'k8s':
                from rca4tracing.fault_injection.ssh_controller_k8s import SshControlerK8s
                self.ip_mapping = SshControlerK8s().get_ip_mapping()
            else:
                self.ip_mapping = None
        else:
            self.ip_mapping = None

        time_str = datetime.now().strftime("%Y%m%d-%H:%M")
        # self.output_folder = self.output_path+f"_result_summary_{time_str}{annotation}"
        self.output_folder = self.output_path+f"_result_summary_{annotation}" # this time, we do not add the time_str, but the experiment name
        
        # the result: count the top k hit times
        self.top_k = dict() # continuous monitoring the results
        self.running_time_stat = dict()
        for algorithm in algorithm_with_params:
            self.running_time_stat[algorithm] = dict()
            self.running_time_stat[algorithm]['all'] = {"count": 0, "sum": 0}

        for k in top_k_list:
            key = f'top_{k}_score_dict'
            self.top_k[key] = {}
            for algorithm in algorithm_with_params:
                self.top_k[key][algorithm] = 0
        
        self.cnt = 0
        self.bad_case_trace = []
        
        
    def evaluate(self,
                 anomaly_traces_with_root_causes=None,
                 anomaly_conditions=None,
                 timestamp=None,
                 look_back_seconds=None,
                 mode='trace',
                 root_causes=[] # only useful for operations
                ):

        if mode == 'trace':
            top_k_ratio = self.get_precision_rate(anomaly_traces_with_root_causes,                                         
                                        anomaly_conditions=anomaly_conditions)
        elif mode in ['operation', 'global']:
            top_k_ratio = self.get_precision_rate_oper(
                                anomaly_conditions=anomaly_conditions,
                                timestamp=timestamp,
                                look_back_seconds=look_back_seconds,
                                root_causes=root_causes)

        if top_k_ratio is None:
            return # no update

        output_folder = self.output_folder + f'_{mode}/'
        self.save_result(top_k_ratio, output_folder)
        self.get_bar_graph(top_k_ratio, output_folder,
                           algorithms=list(self.algorithm_with_params.keys()),
                           top_k_list=self.top_k_list)

    def update_result(self, result, algorithm, root_cause_list, num_spans=None, running_time=0):
        bad_case_traces = []
        if self.data_source == 'dbaas':
            location_list = list(result.keys())
        elif self.data_source == 'jaeger':
            location_list = operation_to_service(result)

        self.running_time_stat[algorithm]['all']['count'] += 1
        self.running_time_stat[algorithm]['all']['sum'] += running_time
        if num_spans is not None:
            num_spans_str = str(num_spans)
            if num_spans_str not in self.running_time_stat[algorithm]:
                self.running_time_stat[algorithm][num_spans_str] = {"count": 0, "sum": 0}
            self.running_time_stat[algorithm][num_spans_str]['count'] += 1
            self.running_time_stat[algorithm][num_spans_str]['sum'] += running_time
            
        for k in self.top_k_list:
            if set(root_cause_list).issubset( set(location_list[:k]) ):
                self.top_k[f'top_{k}_score_dict'][algorithm] += 1
                # print(f'top_{k}')    
            else:
                if algorithm == 'ShapleyValueRCA':
                    LOG.error (f"true root cause: {set(root_cause_list)}, our output: {set(location_list[:k])}")
                    # bad_case_traces.append(trace_id)
        return bad_case_traces

    def get_precision_rate_oper(self,
                                anomaly_conditions=None,
                                timestamp=None,
                                look_back_seconds=None,
                                root_causes=[]):
        run_rca = RunRCA(self.input_path, self.output_path,
                         root_causes = root_causes,
                         anomaly_conditions = anomaly_conditions,
                         data_source = self.data_source,
                         timestamp = timestamp,
                         look_back_seconds = look_back_seconds,
                         ip_mapping = self.ip_mapping)
        if run_rca.empty_data:
            return 

        self.cnt += 1

        for algorithm in self.algorithm_with_params:
            start_time = time.time()
            result, running_time = run_rca.run(algorithm, params=self.algorithm_with_params[algorithm])
            running_time = time.time() - start_time
            LOG.info (f'{algorithm}\n {result}')
            
            tmp_bad_cases = self.update_result(result, algorithm, root_causes, running_time=running_time)

        top_k_ratio = self.get_top_k_ratio()
        LOG.info(f"count: {self.cnt}, top_k_ratio: {top_k_ratio}")
        return top_k_ratio

    def get_precision_rate(self, 
                        anomaly_traces_with_root_causes,
                        anomaly_conditions=None,         
                        ):
        bad_case_traces = []
        for trace_id in anomaly_traces_with_root_causes:
            root_cause_list = anomaly_traces_with_root_causes[trace_id]

            if (self.select_root_cause_num is not None) and (len(root_cause_list) != self.select_root_cause_num):
                continue

            LOG.info('RunRCA on trace {}'.format(trace_id))

            run_rca = RunRCA(self.input_path, self.output_path, 
                            trace_id=trace_id, 
                            root_causes=root_cause_list,
                            data_source=self.data_source,
                            anomaly_conditions=anomaly_conditions,
                            ip_mapping=self.ip_mapping)   
            if run_rca.empty_data:
                continue   # skip this data point
            
            LOG.info(f'trace id: {trace_id}')
            LOG.info(f'root causes: {root_cause_list}')
            
            self.cnt += 1
            num_spans = len(run_rca.data.traces[0])
            for algorithm in self.algorithm_with_params:
                result, running_time = run_rca.run(algorithm, params=self.algorithm_with_params[algorithm])
                LOG.info (f'{algorithm}\n {result}')
                
                tmp_bad_cases = self.update_result(result, algorithm, root_cause_list, num_spans=num_spans, running_time=running_time)
                bad_case_traces += tmp_bad_cases

        LOG.debug('bad_case_traces: {}'.format(bad_case_traces))
        top_k_ratio = self.get_top_k_ratio()
        LOG.info(f"count: {self.cnt}, top_k_ratio: {top_k_ratio}")
        return top_k_ratio

    def get_top_k_ratio(self):
        top_k_ratio = dict()
        for key, score_dict in self.top_k.items():
            top_k_ratio[key] = dict()
            for algm, score in score_dict.items():
                if self.cnt != 0:
                    top_k_ratio[key][algm] = self.top_k[key][algm] / self.cnt                
                else:
                    LOG.debug("number of cases is 0")
                    top_k_ratio[key][algm] = 0
        return top_k_ratio

    def get_average_running_time(self):
        for algorithm in self.running_time_stat:
            for key in self.running_time_stat[algorithm]:
                stat = self.running_time_stat[algorithm][key]
                if stat['count'] != 0:
                    self.running_time_stat[algorithm][key]['average'] = stat['sum']/stat['count']
                else:
                    LOG.debug("number of cases is 0")
                    self.running_time_stat[algorithm][key]['average'] = 0
        return self.running_time_stat

    def save_result(self, top_k, output_path):
        path = output_path
        folder = os.path.exists(path)
        if not folder:
            os.makedirs(path)
        # top_k['count'] = self.cnt
        with open(path + 'top_k.json', 'w')as f:
            f.write(json.dumps(top_k, indent=4))
        with open(path + 'count.json', 'w')as f:
            f.write(json.dumps(self.cnt))

        ave_running_time = self.get_average_running_time()
        with open(path + 'running_time.json', 'w')as f:
            f.write(json.dumps(ave_running_time, indent=4))

    def get_bar_graph(self, top_k, output_path, algorithms=[], top_k_list=[]):  
        plt.figure()
        xtick = []
        for k in top_k_list:
            xtick.append(f"top-{k}") #= ["top-1", "top-3", "top-5"]
        y = dict()
        for algorithm in algorithms:
            y[algorithm] = []
        
        for score_dict in top_k.values():
            for key, value in score_dict.items():
                y[key].append(value)

        xticks = np.arange(len(xtick))

        for idx in range(len(algorithms)):
            algorithm = algorithms[idx]
            plt.bar(xticks + 0.1*idx, y[algorithm], width=0.1, label=algorithm)

        for k in range(len(top_k)):
            key = list(top_k.keys())[k]
            for index, value in enumerate(top_k[key].values()):
                plt.text(index*0.1+k, value+0.01, round(value, 2), ha='center', fontsize=8)

        # plt.xlim([-0.2, 4])
        plt.title('precision rate')
        plt.xlabel("top-k")
        plt.legend(loc='center left')
        plt.xticks(xticks+0.2, xtick)

        path = output_path
        folder = os.path.exists(path)
        if not folder:
            os.makedirs(path)
        plt.savefig(path + 'top_k.jpg')
        plt.close()

    