from pickle import TRUE
import time
from turtle import color
import numpy as np
import matplotlib.pyplot as plt
# from sqlalchemy import true

import rca4tracing.rca.baselines.utils as utils

from rca4tracing.graph.driver.elements import Edge

from rca4tracing.common.logger import setup_logger
LOG = setup_logger(__name__, module_name='rca')

class MicroHECL():

    def __init__(self, edges, nodes_id, trace_data_dict, request_timestamp):
        self.edges = edges
        self.nodes_id = nodes_id
        self.trace_data_dict = trace_data_dict
        self.request_timestamp = request_timestamp
        self.adjacency_node_table = self.get_adjacency_node_table()
        self.metric_list = ['MaxDuration']
        self.time_window = 15   # minutes


    def get_data(self, ts_data_dict=None, metrics_statistical_data=None, metrics_threshold=None):

        if ts_data_dict:
            self.ts_data_dict = ts_data_dict
        else:
            from rca4tracing.anomalydetection.data_query.influxdb_data_query import InfluxdbDataQuery
            self.idq = InfluxdbDataQuery(metrics=['Duration', 'QPS', 'MaxDuration'], dbname='dbpaas0826') 
            self.ts_data_dict = self.get_ts_data_dict()

        if metrics_statistical_data and metrics_threshold:
            self.metrics_statistical_data = metrics_statistical_data
            self.metrics_threshold = metrics_threshold
        else:
            from rca4tracing.datasources.redis.driver import RedisDriver
            from rca4tracing.api.api_get_threshold import ThresholdGetter
            self.redis_driver = RedisDriver()
            self.threshold_getter = ThresholdGetter()
            self.get_metrics_statistical_data()


    def get_adjacency_node_table(self):
        adjacency_table = {}
        for edge in self.edges:
            if edge.source_id not in adjacency_table:
                adjacency_table[edge.source_id] = {'in_nodes': set(), 'out_nodes': set()}
            if edge.target_id not in adjacency_table:
                adjacency_table[edge.target_id] = {'in_nodes': set(), 'out_nodes': set()}
            if not edge.source_id == edge.target_id:
                adjacency_table[edge.source_id]['out_nodes'].add(edge.target_id)
                adjacency_table[edge.target_id]['in_nodes'].add(edge.source_id)

        return adjacency_table

    
    def get_ts_data_dict(self, metrics=['MaxDuration', 'QPS'], lookback_second=60*60):
        to_timestamp = self.request_timestamp + 5 * 60 + 120 
        duration_ts_data_dict = {}
        for node_id in self.nodes_id:
            duration_ts_data_dict[node_id] = {}
            value_data = self.idq.get_data(node_id, to_timestamp - lookback_second, 
                                    to_timestamp, 'before')
            for metric in metrics:
                if value_data:
                        duration_ts_data = list(value_data[metric])
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
        ymean = float(statistic[0]) if statistic[0] is not None else np.mean(normal_data)
        ystd = float(statistic[1]) if statistic[1] is not None else np.std(normal_data)
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


    def pearson_correlation_function(self, a, b):
        return utils.similarity(a, b, default_value=0.01)
        # if a and b and np.var(a) and np.var(b):
        #     pearson_corr = abs(np.corrcoef(a, b)[0, 1])
        #     return pearson_corr
        # else:
        #     return 0.01



    def get_next_correlated_anomalous_nodes(self, current_node, metric_type):
        metric_type_key_dict = {
            'RT': 'MaxDuration',
            'EC': 'EC',
            'QPS': 'QPS'
        }

        # get next edges by metric
        next_nodes = []
        if current_node in self.adjacency_node_table:
            if metric_type in ['RT', 'EC']:
                next_nodes = self.adjacency_node_table[current_node]['out_nodes']
            if metric_type in ['QPS']:
                next_nodes = self.adjacency_node_table[current_node]['in_nodes']

        next_correlated_anomalous_nodes = []
        for node in next_nodes:
            current_node_ts_data = self.ts_data_dict[current_node][metric_type_key_dict[metric_type]][-self.time_window:]
            node_ts_data = self.ts_data_dict[node][metric_type_key_dict[metric_type]][-self.time_window:]
            if self.anomaly_detection(node, metric_type) and (
                self.pearson_correlation_function(current_node_ts_data, node_ts_data) > 0):
                next_correlated_anomalous_nodes.append(node)

        return next_correlated_anomalous_nodes

    def anomaly_propagation_analysis(self, initial_anomalous_node, metric_type):
        current_nodes = [initial_anomalous_node]
        end_nodes = []
        while current_nodes:
            # print(current_nodes)
            next_nodes = []
            for node in current_nodes:
                # get correlated anomalous nodes of current node
                next_correlated_anomalous_nodes = self.get_next_correlated_anomalous_nodes(node, metric_type)

                if next_correlated_anomalous_nodes:
                    next_nodes += next_correlated_anomalous_nodes
                else:
                    # get candidate root causes node
                    end_nodes.append(node)
            current_nodes = next_nodes

        return end_nodes


    def candidate_root_cause_ranking(self, candidate_list, entry_node):
        score = []
        for candidate in candidate_list:
            if candidate is None: # TODO: do not know why candidate is None
                continue
            candidate_ts_data = self.ts_data_dict[candidate]['MaxDuration'][-self.time_window:]
            entry_node_ts_data = self.ts_data_dict[entry_node]['MaxDuration'][-self.time_window:]
            score.append(self.pearson_correlation_function(candidate_ts_data, entry_node_ts_data))

        ranked_score = {}
        for i in np.argsort(score)[::-1]:
            ranked_score[candidate_list[i]] = score[i]

        return ranked_score


    def run(self, initial_anomalous_node, detect_metrics=['RT', 'EC', 'QPS']):
        candidate_list = []
        for metric in detect_metrics:
            if metric in ['RT', 'EC', 'QPS']:
                root_cause_candidate = self.anomaly_propagation_analysis(initial_anomalous_node, metric)
                candidate_list += root_cause_candidate
        
        if len(candidate_list) > 1 and initial_anomalous_node in candidate_list:
            candidate_list.remove(initial_anomalous_node)
        candidate_list = list(set(candidate_list))
        ranked_root_cause = self.candidate_root_cause_ranking(candidate_list, initial_anomalous_node)
        
        return(ranked_root_cause)


if __name__ == '__main__':

    # a synthetic graph in paper for test
    edges = [
        ('1', '3'),
        ('1', '4'),
        ('2', '4'),
        ('3', '5'),
        ('4', '5'),
        ('4', '6'),
        ('5', '7'),
        ('5', '8'),
        ('7', '9'),
        ('7', '10'),
        ]
    edges = [Edge(source_id=edge[0], target_id=edge[1], label='none') for edge in edges]


    # adjacency_table = get_adjacency_node_table(edges)


    # from rca4tracing.anomalydetection.data_query.influxdb_data_query import InfluxdbDataQuery
    # idq = InfluxdbDataQuery(metrics=['Duration', 'QPS'], dbname='dbpaas0826') 
    # node_list = list(adjacency_table.keys())
    # node_filter = ['RDS_SERVICE:GET']
    # node_ts_data_dict = {}
    # from_timestamp = int(time.time())-3600 * 24 * 7 # Minute-level data of last 7 days
    # for node_id in node_list:
    #     if node_id not in node_filter:
    #         value_data = idq.get_data_after(node_id, from_timestamp)
    #         node_ts_data_dict[node_id] = value_data

    microhecl = MicroHECL(edges)
    microhecl.run(initial_anomalous_node='5', detect_metrics=['RT', 'QPS'])
