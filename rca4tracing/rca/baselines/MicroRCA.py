import time
import random
import copy
import numpy as np


# from rca4tracing.anomalydetection.host_file_driver import HostFileDriver
# from rca4tracing.common import operation_block_list

from rca4tracing.graph.driver.elements import Edge

import rca4tracing.rca.baselines.utils as utils

from rca4tracing.common.logger import setup_logger
LOG = setup_logger(__name__, module_name='rca')

class MicroRCA():

    def __init__(self, edges, nodes_id, trace_data_dict, request_timestamp):
        self.edges = edges
        self.nodes_id = nodes_id
        self.trace_data_dict = trace_data_dict
        self.request_timestamp = request_timestamp
        self.metric_list = ['MaxDuration']
        self.machine_metrics = [
            'node_cpu_utilization', 
            'memory_utilization', 
            'root_partition_utilization', 
            'node_sockstat_TCP_tw'
            ]
        self.time_window = 15   # minutes


    def get_data(self, ts_data_dict=None, metrics_statistical_data=None, metrics_threshold=None, ip_ts_data_dict=None):

        if ts_data_dict:
            self.ts_data_dict = ts_data_dict
        else:
            from rca4tracing.anomalydetection.data_query.influxdb_data_query import InfluxdbDataQuery
            self.idq = InfluxdbDataQuery(metrics=self.metric_list, dbname='dbpaas0826') 
            self.ts_data_dict = self.get_duration_ts_data_dict()

        if metrics_statistical_data and metrics_threshold:
            self.metrics_statistical_data = metrics_statistical_data
            self.metrics_threshold = metrics_threshold
        else:
            from rca4tracing.datasources.redis.driver import RedisDriver
            from rca4tracing.api.api_get_threshold import ThresholdGetter
            self.redis_driver = RedisDriver()
            self.threshold_getter = ThresholdGetter()
            self.get_metrics_statistical_data()

        self.anomalous_nodes_list = self.get_anomalous_nodes()
        self.operation_ip_dict = self.get_operation_ip_dict()
        self.ip_list = list(set(self.operation_ip_dict.values()))

        self.ip_ts_data_dict = ip_ts_data_dict

        # we will not load machine data if not exists
        # if ip_ts_data_dict: 
        #     self.ip_ts_data_dict = ip_ts_data_dict
        # else:
        #     self.ip_ts_data_dict = self.get_host_ts_data_dict()


    def get_metrics_statistical_data(self):
        self.metrics_statistical_data = {}
        self.metrics_threshold = {}
        for node_id in self.nodes_id:
            self.metrics_statistical_data[node_id] = {}
            self.metrics_threshold[node_id] = {}
            for metric in self.metric_list:
                key = "__mean_std_" + metric + "__::" + node_id
                self.metrics_statistical_data[node_id][metric] = self.redis_driver.r.hmget(key, ['mean', 'std', 'count'])
                self.metrics_threshold[node_id][metric] = self.threshold_getter.get_lower_upper_threshold(node_id, metric)

    def anomaly_detection(self, node_id, metric_type, use_trace_data=True):
        metric_type_key_dict = {
            'RT': 'Duration',
            'EC': 'EC',
            'QPS': 'QPS'
        }

        # Using existing methods
        ts_data = self.ts_data_dict[node_id].get(metric_type_key_dict[metric_type])
        
        if not ts_data:
            return False

        normal_data = ts_data[:-self.time_window]
        # print(normal_data)
        if use_trace_data:
            data_to_detect = self.trace_data_dict[node_id]['Duration']
        else:
            data_to_detect = ts_data[-self.time_window:]

        statistic = self.metrics_statistical_data[node_id][metric_type_key_dict[metric_type]]
        # print(result)
        ymean = float(statistic[0]) if statistic[0] else np.mean(normal_data)
        ystd = float(statistic[1]) if statistic[1] else np.std(normal_data)
        threshold1 = ymean - 5 * ystd
        threshold2 = ymean + 5 * ystd

        upper_threshold = self.metrics_threshold[node_id][metric_type_key_dict[metric_type]]
        # print(upper_threshold)
        if upper_threshold[1] and upper_threshold[1] < threshold2:
            threshold2 = upper_threshold[1]

        # print(data_to_detect)
        # print(ymean, ystd, threshold1, threshold2) 
        # print(self.trace_data_dict[node_id]['Duration'])
        for value in data_to_detect:
            # if value > 200000:
            #     return True
            if (value < threshold1)|(value > threshold2):
                # print('anomaly', node_id, metric_type)
                return True
        # print('normal', node_id, metric_type)
        return False

    def get_duration_ts_data_dict(self, lookback_second=60*60):
        to_timestamp = self.request_timestamp + 5 * 60 + 120 
        duration_ts_data_dict = {}
        for node_id in self.nodes_id:
            duration_ts_data_dict[node_id] = {}
            value_data = self.idq.get_data(node_id, to_timestamp - lookback_second, 
                                    to_timestamp, 'before')
            for metric in self.metric_list:
                if value_data:
                        duration_ts_data = list(value_data['MaxDuration'])
                        for i in range(1, len(duration_ts_data), 1):
                            if duration_ts_data[i] == None:
                                duration_ts_data[i] = duration_ts_data[i-1]
                        for i in range(len(duration_ts_data)-2,-1,-1):
                            if duration_ts_data[i] == None:
                                duration_ts_data[i] = duration_ts_data[i+1]
                        duration_ts_data_dict[node_id][metric] = duration_ts_data
                else:
                    duration_ts_data_dict[node_id][metric] = []

        return duration_ts_data_dict

    def get_anomalous_nodes(self):

        anomalous_nodes_list = []
        for node_id in self.nodes_id: 
            if self.anomaly_detection(node_id, 'RT'):
            # if node_id in ['s2', 's3', 's4']:
                anomalous_nodes_list.append(node_id)

        return anomalous_nodes_list

    def get_operation_ip_dict(self):

        operation_ip_dict = {}
        for node_id in self.nodes_id:
            ip = self.trace_data_dict[node_id]['serverIp'][0]
            operation_ip_dict[node_id] = ip
        
        return operation_ip_dict

    def get_host_ts_data_dict(self):
        from rca4tracing.api.perf_query import get_machines_perf

        to_timestamp = self.request_timestamp + 5 * 60
        ip_ts_data_dict = {}
        for ip in self.ip_list:
            machines = ip + ':9077'
            machines_ts_data_dict = get_machines_perf([machines], 
                look_back_seconds=60*60, timestamp=to_timestamp)
            ip_ts_data_dict[ip] = machines_ts_data_dict[machines]

        return ip_ts_data_dict


    def anomalous_graph_extraction(self):
        adjacency_table = {}
        for edge in self.edges:
            if edge.source_id in self.anomalous_nodes_list or edge.target_id in self.anomalous_nodes_list:
                if edge.source_id not in adjacency_table:
                    adjacency_table[edge.source_id] = set()
                if not edge.source_id == edge.target_id:
                    adjacency_table[edge.source_id].add(edge.target_id)
        
        # add host ip
        for operation, host_ip in self.operation_ip_dict.items():
            if operation in self.anomalous_nodes_list:
                if operation not in adjacency_table:
                    adjacency_table[operation] = set()
                adjacency_table[operation].add(host_ip)

        return adjacency_table

    def pearson_correlation_function(self, a, b):
        return utils.similarity(a, b, default_value=0.01)
        # if a and b and np.var(a) and np.var(b):
        #     pearson_corr = abs(np.corrcoef(a, b)[0, 1])
        #     return pearson_corr
        # else:
        #     return 0.01

    def edge_weighting(self, alpha):
        weighted_adjacency_table = {}
        for source_id, target_id_set in self.anomalous_graph.items():
            weighted_adjacency_table[source_id] = {}
            for target_id in target_id_set:
                if target_id in self.anomalous_nodes_list:
                    weighted_adjacency_table[source_id][target_id] = alpha
                elif target_id in self.ip_ts_data_dict: # with collected machine data
                    if self.ip_ts_data_dict[target_id]:
                        source_ts_data = self.ts_data_dict[source_id]['MaxDuration']
                        correlation_list = []
                        for machine_metric in self.machine_metrics:
                            ip_ts_data = self.ip_ts_data_dict[target_id].get(machine_metric)
                            if ip_ts_data:
                                target_ts_data = [ip_ts_data[i] for i in range(-1,-len(source_ts_data)*2,-2)]      
                                pearcorr = self.pearson_correlation_function(source_ts_data[-self.time_window:], 
                                    target_ts_data[-self.time_window:])
                                correlation_list.append(pearcorr)
                        weighted_adjacency_table[source_id][target_id] = alpha * np.max(correlation_list)
                    else:
                        weighted_adjacency_table[source_id][target_id] = 0
                elif target_id in self.ts_data_dict: # with collected metric data
                    source_ts_data = self.ts_data_dict[source_id]['MaxDuration']
                    target_ts_data = self.ts_data_dict[target_id]['MaxDuration']
                    correlation = self.pearson_correlation_function(source_ts_data[-self.time_window:], 
                        target_ts_data[-self.time_window:])
                    weighted_adjacency_table[source_id][target_id] = correlation
                else:
                    LOG.debug(f"we do not have timeseries data for {target_id}")
                    weighted_adjacency_table[source_id][target_id] = 0

        return weighted_adjacency_table

    def localizing_root_cause(self, steps_threshold):
        # Personalized PageRank
        location = random.choice(list(self.anomalous_graph.keys()))
        steps_num = 0
        steps = []  #
        while steps_num < steps_threshold:
            rand = random.random()
            if location in self.anomalous_graph and rand > 0.15:
                next_nodes = list(self.anomalous_graph[location])
                next_nodes_weight = [self.edge_weight[location][node] for node in next_nodes]
                location = random.choices(next_nodes, weights=next_nodes_weight)[0]
                steps.append(location)
                steps_num += 1
            else:
                location = random.choice(list(self.anomalous_graph.keys()))
                steps.append(location)
                steps_num += 1

        frequency_dict = {}
        for source_id, target_id_list in self.anomalous_graph.items():
            frequency_dict[source_id] = 0
            for target_id in target_id_list:
                frequency_dict[target_id] = 0
        length_burned = int(steps_threshold / 2)
        for node in steps[length_burned:]:
            frequency_dict[node] += 1
        score = {}
        for node, frequency in frequency_dict.items():
            score[node] = frequency / length_burned

        score = dict(sorted(score.items(), key=lambda x: x[1], reverse=True))

        return score

    def run(self):
        self.anomalous_graph = self.anomalous_graph_extraction()
        if not self.anomalous_graph:
            print('MicroRCA\n', {})
            return {}
        self.edge_weight = self.edge_weighting(0.5)
        sorted_score = self.localizing_root_cause(10000)

        return sorted_score

if __name__ == '__main__':
    # from rca4tracing.anomalydetection.controller.monitor_keys_getter import ServiceGetter

    edges_ = [
        Edge(source_id='s1', target_id='s2', label='None'),
        Edge(source_id='s1', target_id='s3', label='None'),
        Edge(source_id='s2', target_id='s3', label='None'),
        Edge(source_id='s2', target_id='s4', label='None'),
        Edge(source_id='s3', target_id='s5', label='None'),
    ]

    nodes_id_ = ['s1','s2','s3','s4','s5']

    trace_data_dict_ = {
        's1': {'serverIp': ['h1']},
        's2': {'serverIp': ['h2']},
        's3': {'serverIp': ['h1']},
        's4': {'serverIp': ['h2']},
        's5': {'serverIp': ['h1']}
    }


    mrca = MicroRCA(edges_, nodes_id_, trace_data_dict_, time.time())

    print('localizing_root_cause\n', mrca.localizing_root_cause(1000))
