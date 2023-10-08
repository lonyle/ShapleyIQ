import numpy as np
import os
import pickle
import copy


from rca4tracing.rca.experiment.rca_data import RCAData 
from rca4tracing.fault_injection.data_collector import DataCollector #collect, collect_by_trace_id
from rca4tracing.fault_injection.trace_to_metric import Trace2Metric

import rca4tracing.fault_injection.config as config

from rca4tracing.common.logger import setup_logger
LOG = setup_logger(__name__, module_name='rca')

'''
we need to get the following data:
    self.edges = []
    self.nodes_id = []
    self.root_id = None
    self.traces = None # the traces used for shapley rca
    self.trace_data_dict = dict()
    self.request_timestamp = None # int, unit:second
    self.ts_data_dict = dict()
    self.metrics_statistical_data = dict()
    self.metrics_threshold = dict()
    self.operation_ip_dict = dict()
    self.ip_ts_data_dict = dict() # machine data
    self.normal_traces = dict()
    self.abnormal_traces = dict()
'''

METRIC_INTERVAL_SECONDS = 5

def get_callers_of_anomalies(anomaly_nodes, trace, port=None):
    # look at the http url
    callers = set()
    for span in trace:
        if 'tags' in span:
            for item in span['tags']:
                if item['key'] in ['http.url', 'https.url']:
                    url = item['value']
                    service_name = url.split('://')[1].split(':')[0]
                    if service_name in anomaly_nodes:
                        callers.add(span['serviceName'])
    return list(callers)


class RCADataJaeger(RCAData):
    def __init__(self, 
                 input_path, 
                 root_causes=None, # for jaeger data, we can label the root cause
                 trace_id=None, # if we specify the trace_id, we only process this trace_id
                 timestamp=None,
                 look_back_seconds = 90, # analyze the traces of recent how many seconds
                 anomaly_conditions = {},
                 metric_list=['MaxDuration', 'Duration'],
                 host=config.jaeger_host,
                 ip_mapping = None
                 ):
        super().__init__(root_causes, input_path, metric_list=metric_list)
        self.host = host 
        self.data_collector = DataCollector(host=host)

        self.empty_data = True # if we receive data, we set it to false

        self.trace_id = trace_id
        self.timestamp = timestamp
        self.look_back_seconds = look_back_seconds
        self.anomaly_conditions = anomaly_conditions 

        if self.trace_id is not None:
            self.case_id = self.trace_id # the id for this case
        else:
            self.case_id = f"{self.anomaly_conditions}_{self.look_back_seconds}" # combinations of anomaly conditions and look_back_seconds, for operator-level and global-level

        self.ip_mapping = ip_mapping
        if self.ip_mapping is None:
            if config.system_type == 'k8s':
                from rca4tracing.fault_injection.ssh_controller_k8s import SshControlerK8s
                self.ip_mapping = SshControlerK8s().get_ip_mapping()

        self.trace_dict = dict()

        # label the root cause
        if (root_causes is None) and (len(self.anomaly_conditions) == 0):
            LOG.warning('cannot label the root cause')    
        if root_causes is not None:
            self.root_causes = root_causes
        else:
            # not applicable for the cases of operation or global
            self.root_causes = self.find_true_root_causes(trace_id, self.anomaly_conditions)

    def find_true_root_causes(self, trace_id, services_delay):
        ''' k8s and docker environments are different
        '''
        if config.system_type == 'docker':
            traces, trace_ids = self.data_collector.collect_by_trace_id(trace_id)
            true_anomalies = get_callers_of_anomalies(list(services_delay.keys()), traces[0])
        elif config.system_type == 'k8s':
            true_anomalies = list(services_delay.keys())
        return true_anomalies

    def get_non_error_operations(self, operations):
        ''' if the return value is error, we will have multiple root nodes
        '''
        non_error_operations = []
        for operation in operations:
            if operation.split(':')[1] != 'error':
                non_error_operations.append(operation)
        return non_error_operations

    def process_one_trace(self, trace, trace_id):
        ''' process one trace data (which is anomaly trace)
        '''
        import rca4tracing.web.edge_functions as edge_func

        path_id, edges, nodes_id, trace_data_dict, root_cause, \
                adjusted_contribution_dict, request_timestamp, root_name_list = \
                    edge_func.get_pid_edges_v2(trace_id, trace)

        root_name_list = self.get_non_error_operations(root_name_list)
        if len(root_name_list) != 1: # only one root for a trace
            LOG.warning(f'incomplete trace {trace_id} with root list: {root_name_list}')
            root_id = None
        else:
            root_id = root_name_list[0]

        self.update_by_trace_info(edges, nodes_id, trace_data_dict, request_timestamp, root_id)
       
    def update_by_trace_info(self, edges, nodes_id, trace_data_dict, request_timestamp, root_id):
        ''' update data by traces
            if we have multiple traces, we will update the variables
        '''
        if root_id is None: # incomplete trace
            return 

        if self.empty_data: # initialize
            self.empty_data = False

            self.root_id = root_id # TODO: if we have multiple root_ids, do we still need this
            self.edges = edges 
            self.nodes_id = nodes_id 
            self.trace_data_dict = trace_data_dict 
            self.request_timestamp = request_timestamp # 10 digits, in seconds
        else:
            self.edges |= edges # merge two sets
            self.nodes_id = list( set(self.nodes_id) | set(nodes_id) )
            for node_id in trace_data_dict:
                # update Duration and serverIp
                if node_id not in self.trace_data_dict:
                    self.trace_data_dict[node_id] = trace_data_dict[node_id]
                else:
                    for field in trace_data_dict[node_id]:
                        if type(trace_data_dict[node_id][field]) is list:
                            self.trace_data_dict[node_id][field] += trace_data_dict[node_id][field]
                        else:
                            self.trace_data_dict[node_id][field] = \
                                max(self.trace_data_dict[node_id][field], trace_data_dict[node_id][field])
            
            self.request_timestamp = max(self.request_timestamp, request_timestamp)
            

    def get_one_trace(self, trace_id):
        ''' get one trace, return a list
        '''
        traces, trace_ids = self.data_collector.collect_by_trace_id(trace_id)
        if len(traces) != 1 or len(trace_ids) != 1:
            LOG.warning(f"len(traces) != 1 or len(trace_ids) != 1. trace: {traces}, trace_ids: {trace_ids}")
        return traces

    def get_traces(self, look_back_seconds, anomaly_conditions, end_time_seconds=None):
        ''' according to the condition, search for the normal trace and abnormal traces 

            get both normal and abnormal traces, return dictionaries in form of {trace_id: trace}
        '''
        abnormal_trace_dict = dict()
        normal_trace_dict = dict()

        rt_threshold = min(anomaly_conditions.values())*1000 # in us

        for service in anomaly_conditions:
            abnomral_traces, abnormal_trace_ids = self.data_collector.collect(look_back_seconds=look_back_seconds,
                                                        minDuration_us=rt_threshold,
                                                        service=service,
                                                        end_time_seconds=end_time_seconds)
            normal_traces, normal_trace_ids = self.data_collector.collect(look_back_seconds=look_back_seconds,
                                                        maxDuration_us=rt_threshold,
                                                        service=service,
                                                        end_time_seconds=end_time_seconds)
            
            LOG.info(f"num. of abnomral_traces: {len(abnomral_traces)}, num. of normal_traces: {len(normal_traces)}")
            # the above traces should be of the dbaas form
            for idx in range(len(abnomral_traces)):
                trace_id = abnormal_trace_ids[idx]
                if trace_id in abnormal_trace_dict:
                    continue
                trace = abnomral_traces[idx]                 
                abnormal_trace_dict[trace_id] = trace    

            for idx in range(len(normal_traces)):
                trace_id = normal_trace_ids[idx] 
                if trace_id in normal_trace_dict:
                    continue
                trace = normal_traces[idx]          
                normal_trace_dict[trace_id] = trace

        # for backward compatibility, construct trace_dict, including normal and abnormal traces
        trace_dict = {**abnormal_trace_dict, **normal_trace_dict}
        return trace_dict, abnormal_trace_dict, normal_trace_dict
        
    def get_metrics(self, 
                    start_time=None,
                    end_time=None,
                    traces=None):
        ''' ref: rca4tracing/rca/experiment/get_rca_data.py
        '''
        trace2metric = Trace2Metric()

        if traces is not None: # also insert the traces
            start_time, end_time = trace2metric.insert_traces(traces)
        else:
            if (start_time is None) or (end_time is None):
                LOG.warning("(start_time is None) or (end_time is None)")
            # else use the start_time and end_time

        for node_id in self.nodes_id:
            self.ts_data_dict[node_id] = dict()
            service_name, operation_name = node_id.split(':')
            value_dict, timestamp = trace2metric.get_metrics(start_time, end_time, 
                                                            service_name=service_name,
                                                            operation_name=operation_name,
                                                            metrics=self.metric_list,
                                                            group_by_seconds=METRIC_INTERVAL_SECONDS)
            LOG.debug(f"node_id: {node_id}, value_dict: {value_dict}")

            for metric in self.metric_list:
                if value_dict:
                    duration_ts_data = list(filter(lambda x: x != None, value_dict[metric]))
                    self.ts_data_dict[node_id][metric] = duration_ts_data
                else:
                    self.ts_data_dict[node_id][metric] = []
        return self.ts_data_dict

    def get_metrics_stat(self, ts_data_dict, anomaly_conditions):
        ''' get stat from metrics
        '''
        
        for node_id in ts_data_dict:
            self.metrics_statistical_data[node_id] = dict()
            self.metrics_threshold[node_id] = dict()

            for metric in self.metric_list:
                values = self.ts_data_dict[node_id][metric]
                if values:
                    stat = {
                        'mean': np.mean(values),
                        'std': np.std(values),
                        'count': len(values)
                    }
                else:
                    LOG.warning(f'the time series is empty for {node_id} {metric}')
                    stat = {'mean': 0, 'std': 0, 'count': 0}
                self.metrics_statistical_data[node_id][metric] = list(stat.values())
                upper_threshold = min(anomaly_conditions.values())*1000 # the threshold to distinuigh slow operations
                self.metrics_threshold[node_id][metric] = [None, upper_threshold]


    def get_machine_metrics(self, ip_list, start_timestamp, end_timestamp, 
                            duplicate=5): #because sample by 5 seconds
        ''' get the prometheus data for the machine
        '''
        from rca4tracing.rca.experiment.prom_query import PromQueryLocal
        
        look_back_seconds = (end_timestamp - start_timestamp) // 1000 # in seconds

        LOG.info('getting machine info...')
        ip_ts_data_dict = dict()
        
        prom_query = PromQueryLocal()
        for ip in ip_list:
            # print (to_timestamp)

            tmp_value_dict = prom_query.get_machine_perf(ip, \
                end_timestamp//1000, look_back_seconds=look_back_seconds)
            
            # filter out the keys which ends with 'timestamp'
            value_dict = dict()
            for key in tmp_value_dict:
                if not key.endswith('timestamp'):
                    values = []
                    for tmp_value in tmp_value_dict[key]:
                        values += [tmp_value] * duplicate
                    value_dict[key] = values
            ip_ts_data_dict[ip] = value_dict

        LOG.debug(f'machine metric value_dict: {ip_ts_data_dict}')
        LOG.info('got machine info.')
        return ip_ts_data_dict

    def load_all_data(self):
        ''' if the data exists, load it, else get the data
        '''
        path = self.input_path + '{}/'.format(self.case_id)
        filename = path + 'rca_data.pickle'
        # print (filename)
        # print (os.path.exists(filename))
        if os.path.exists(filename):
            LOG.info('loading the saved data')            
            rca_data = pickle.load(open(filename, 'rb'))
            if rca_data is not None:
                return rca_data
        else:
            if not os.path.exists(path):
                os.makedirs(path)
            rca_data = self.get_all_data()
            if rca_data is not None:
                pickle.dump(self, open(filename, 'wb'))
            else:
                pickle.dump(None, open(filename, 'wb'))
        return rca_data


    def get_all_data(self):       
        # 1. get and process the traces 
        if self.trace_id is not None: # diagnose by trace id
            traces = self.get_one_trace(self.trace_id)
            self.process_one_trace(traces[0], self.trace_id)
            self.traces = traces

            if self.request_timestamp is None:
                return None

            self.trace_dict, self.abnormal_trace_dict, self.normal_trace_dicts = \
                self.get_traces(self.look_back_seconds, self.anomaly_conditions, 
                                    end_time_seconds=self.request_timestamp+5)
        else: # diagnose by services (and look back seconds)  
            LOG.info (f"anomaly_conditions: {self.anomaly_conditions}")
            self.trace_dict, self.abnormal_trace_dict, self.normal_trace_dict = \
                self.get_traces(self.look_back_seconds, self.anomaly_conditions,
                                end_time_seconds=self.timestamp)
            for abnormal_trace_id in self.abnormal_trace_dict:
                self.process_one_trace(self.abnormal_trace_dict[abnormal_trace_id], abnormal_trace_id)
            self.traces = list(self.abnormal_trace_dict.values())
            LOG.debug (f"num. of abnormal traces is {len(self.abnormal_trace_dict)}")
            # ts_data_dict = self.get_metrics(traces=traces) # old version, insert traces

        if self.request_timestamp is None: # there is no data
            return None

        # 2. get metrics for the node ids
        end_timestamp = self.request_timestamp*1000 + 5*(10**3) # 13 digits
        start_timestamp = end_timestamp - (self.look_back_seconds+5)*(10**3) # 60 seconds ago
        ts_data_dict = self.get_metrics(start_time=start_timestamp, end_time=end_timestamp) # new version, get metrics by start_time and end_time

        self.get_metrics_stat(ts_data_dict, self.anomaly_conditions)

        # 3. get ips
        self.operation_ip_dict = self.get_operation_ip_dict(ip_mapping=self.ip_mapping)

        self.ip_list = list(set(self.operation_ip_dict.values()))

        # 4. get machine data
        # to_timestamp = self.request_timestamp + 31 # one more point 
        self.ip_ts_data_dict = self.get_machine_metrics(self.ip_list, start_timestamp, end_timestamp)

        if self.empty_data == True:
            return None
        return 0


if __name__ == '__main__':
    input_path = 'rca4tracing/rca/experiment/input_data/experiment_jaeger/'
    look_back_seconds = 90
    anomaly_conditions = {
        "ts-station-service": 100
    }
    rca_data = RCADataJaeger(input_path,
                            #  look_back_seconds=look_back_seconds,
                             trace_id = 'b74a6b62f12248d4',
                             anomaly_conditions=anomaly_conditions)
    rca_data.get_all_data()

        