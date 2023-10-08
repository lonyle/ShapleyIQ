from curses import keyname
# from gettext import npgettext
from multiprocessing import Value
import time
import json
import os

from traceback import print_tb

# from sklearn.metrics import top_k_accuracy_score
import rca4tracing.web.edge_functions as edge_func

from rca4tracing.api.perf_query import get_machines_perf
# from rca4tracing.periodic_task.error_trace_periodic_tasks import OperationAnomalyTrace
from rca4tracing.anomalydetection.data_query.influxdb_data_query import InfluxdbDataQuery
from rca4tracing.datasources.redis.driver import RedisDriver
from rca4tracing.api.api_get_threshold import ThresholdGetter
from rca4tracing.datasources.logstore.log_service_client import LogServiceClient
from rca4tracing.api.log_api import get_trace_detail_by_id

from rca4tracing.rca.experiment.rca_data import RCAData

from rca4tracing.common.logger import setup_logger
LOG = setup_logger(__name__, module_name='rca')

class RCADataDBaaS(RCAData):
    def __init__(self, 
                 input_path,
                 trace_id=None,
                 root_causes=None,
                 metric_list=['MaxDuration', 'Duration']):
        super().__init__(root_causes, input_path, metric_list=metric_list)
        self.trace_id = trace_id

        # for backward compatibility, construct trace_dict
        self.trace_dict = dict()

        # self.oat = OperationAnomalyTrace()
        self.data_query = InfluxdbDataQuery(metrics=['Duration', 'QPS', 'MaxDuration'], 
                                            dbname='dbpaas0826') 
        self.redis_driver = RedisDriver()
        self.threshold_getter = ThresholdGetter()
        self.log_service_client = LogServiceClient()

    def get_trace_by_id(self):
        self.spans = get_trace_detail_by_id(self.trace_id,
                                    int((time.time() - 3600 * 24 * 30) * 1000), # one month ago
                                    int(time.time() * 1000))
        path_id, self.edges, self.nodes_id, self.trace_data_dict, root_cause, \
            adjusted_contribution_dict, self.request_timestamp, self.root_name_list = \
            edge_func.get_pid_edges_v2(self.trace_id, self.spans)

    def get_ts_data_dict(self):
        ''' get time series data for each operations 
        '''
        to_timestamp = self.request_timestamp + 5 * 60 + 120 
        # self.ts_data_dict = {}
        for node_id in self.nodes_id:
            self.ts_data_dict[node_id] = {}
            value_data = self.data_query.get_data(node_id, to_timestamp - 60 * 60 , 
                                    to_timestamp, 'before')
            for metric in self.metric_list:
                if value_data:
                        duration_ts_data = list(value_data[metric])
                        for i in range(1, len(duration_ts_data), 1):
                            if duration_ts_data[i] == None:
                                duration_ts_data[i] = duration_ts_data[i-1]
                        for i in range(len(duration_ts_data)-2,-1,-1):
                            if duration_ts_data[i] == None:
                                duration_ts_data[i] = duration_ts_data[i+1]
                        self.ts_data_dict[node_id][metric] = duration_ts_data
                else:
                    self.ts_data_dict[node_id][metric] = []


    def get_metrics_statistical_data(self):
        ''' get cached mean, std, count, threshold for the historical time series
            if not exists, then use the recent time series to calculate
        '''
        # self.metrics_statistical_data = {}
        # self.metrics_threshold = {}
        for node_id in self.nodes_id:
            self.metrics_statistical_data[node_id] = {}
            self.metrics_threshold[node_id] = {}
            for metric in self.metric_list:
                key = "__mean_std_" + metric + "__::" + node_id
                self.metrics_statistical_data[node_id][metric] = self.redis_driver.r.hmget(key, ['mean', 'std', 'count'])
                self.metrics_threshold[node_id][metric] = self.threshold_getter.get_lower_upper_threshold(node_id, metric)


    def get_machine_data(self):
        ''' get machine metrics
        '''
        to_timestamp = self.request_timestamp + 5 * 60
        ip_ts_data_dict = {}
        machine_list = [ip + ':9077' for ip in self.ip_list]

        machines_ts_data_dict = get_machines_perf(machine_list, 
            look_back_seconds=60*60, timestamp=to_timestamp)
        for ip in self.ip_list:
            ip_ts_data_dict[ip] = machines_ts_data_dict[ip + ':9077']

        return ip_ts_data_dict

    def get_root_trace_dict(self):
        ''' get the traces that contains the root node
            including the normal traces and abnormal traces
        '''
        timestamp = self.request_timestamp * 1000
        from_time = timestamp - 10 * 60 * 1000
        to_time = timestamp + 5 * 60 * 1000
        service_name, operation_name = self.root_id.split(':')
        trace_id_list = []
        trace_id_list += self.log_service_client.get_traceId_by_spanName(operation_name, from_time,
                            to_time, get_error=None, serviceName=service_name,
                            limit=20, timestamp=None, return_timestamp=False,
                            duration=None)

        mean, std, count = self.metrics_statistical_data[self.root_id]['Duration']
        if mean and std:
            rt_threshold = float(mean) + float(std) * 3
        else:
            rt_threshold = None
        trace_id_list += self.log_service_client.get_traceId_by_spanName(operation_name, from_time,
                            to_time, get_error=None, serviceName=service_name,
                            limit=20, timestamp=None, return_timestamp=False,
                            duration=rt_threshold)

        # self.trace_dict = dict()
        for trace_id in trace_id_list:
            trace = self.log_service_client.get_trace_detail_by_id(trace_id, from_time,
                                                    to_time)    
            self.trace_dict[trace_id] = trace      

    def load_all_data(self):
        ''' if the data exists, load it, else get the data
        '''
        path = self.input_path + '{}/'.format(self.trace_id)
        if os.path.exists(path):
            LOG.info('data saved')
            self.read_data()
        else:
            LOG.info('get data')
            ret = self.get_all_data()
            if ret is None:
                return None # skip this data point
                
            self.save_data()
        return 0

    def get_all_data(self):
        self.get_trace_by_id()
        if len(self.root_name_list) != 1:
            LOG.info('incomplete trace data')
            return None
        else:
            self.root_id = self.root_name_list[0]
        self.get_ts_data_dict()
        self.get_metrics_statistical_data()
        self.operation_ip_dict = self.get_operation_ip_dict()
        self.ip_ts_data_dict = self.get_machine_data()
        self.get_root_trace_dict()
        return 0


    def save_data(self):
        path = self.input_path + '{}/'.format(self.trace_id)
        folder = os.path.exists(path)
        if not folder:
            os.makedirs(path)
        path = self.input_path + '{}/'.format(self.trace_id)
        folder = os.path.exists(path)
        if not folder:
            os.makedirs(path)

        data_dict = {
            'root_cause': self.root_causes,
            'spans': self.spans,
            'ts_data_dict': self.ts_data_dict,
            'metrics_statistical_data': self.metrics_statistical_data,
            'metrics_threshold': self.metrics_threshold,
            'operation_ip_dict': self.operation_ip_dict,
            'ip_ts_data_dict': self.ip_ts_data_dict,
            'trace_dict': self.trace_dict,
        }

        for key, value in data_dict.items():
            with open(path + key + '.json', 'w')as f:
                f.write(json.dumps(value, indent=4))


    def read_data(self):
        # this is the input path
        path = self.input_path + '{}/'.format(self.trace_id)

        data_dict = {
            'root_cause': 'self.root_causes',
            'spans': 'self.spans',
            'ts_data_dict': 'self.ts_data_dict',
            'metrics_statistical_data': 'self.metrics_statistical_data',
            'metrics_threshold': 'self.metrics_threshold',
            'operation_ip_dict': 'self.operation_ip_dict',
            'ip_ts_data_dict': 'self.ip_ts_data_dict',
            'trace_dict': 'self.trace_dict',
        }

        for key, value in data_dict.items():
            with open(path + key + '.json', 'r')as f:
                exec(value + '=json.load(f)')

        # compatible with old data:
        if type(self.root_causes) is str:
            self.root_causes = self.root_causes.split(',')

        path_id, self.edges, self.nodes_id, self.trace_data_dict, root_cause, \
            adjusted_contribution_dict, self.request_timestamp, self.root_name_list = \
            edge_func.get_pid_edges_v2(self.trace_id, self.spans)

        if len(self.root_name_list) != 1:
            LOG.info('incomplete trace data')
            return
        else:
            self.root_id = self.root_name_list[0]

        self.ip_list = list(set(self.operation_ip_dict.values()))

        self.traces = [self.spans] # compatibility

