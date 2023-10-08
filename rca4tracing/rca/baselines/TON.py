""" This implements the MultitierRCA: "Root Cause Analysis of Anomalies of Multitier Services in Public Clouds"
    Because it is publised in TON (Transactions on Networking), we name it TON.py
"""

from logging import root
import time
import random
import copy
import numpy as np

from rca4tracing.common.logger import setup_logger
import rca4tracing.rca.baselines.utils as utils
LOG = setup_logger(__name__, module_name='rca')

class TON():

    def __init__(self, edges, nodes_id, root_id, trace_data_dict, request_timestamp,
                 ip_mapping=None # the recored inner ip may not be the real IP
                ):
        self.edges = edges
        self.nodes_id = nodes_id
        self.root_id = root_id
        self.trace_data_dict = trace_data_dict
        self.request_timestamp = request_timestamp
        self.metric_list = ['MaxDuration']
        self.machine_metrics = [
            'node_cpu_utilization', 
            'memory_utilization', 
            'root_partition_utilization', 
            'node_sockstat_TCP_tw'
            ]
        self.ip_mapping = ip_mapping
        self.time_window = 15   # minutes
        
    def get_data(self, ts_data_dict=None, ip_ts_data_dict=None):
        self.operation_ip_dict = self.get_operation_ip_dict()
        self.ip_list = list(set(self.operation_ip_dict.values()))
        if self.ip_mapping is not None:
            self.ip_list = [self.ip_mapping[ip] for ip in self.ip_list]
        self.adjacency_dict = self.get_adjacency_dict()
        

        if ts_data_dict:
            self.ts_data_dict = ts_data_dict
        else:
            from rca4tracing.anomalydetection.data_query.influxdb_data_query import InfluxdbDataQuery
            self.idq = InfluxdbDataQuery(metrics=['MaxDuration'], dbname='dbpaas0826') 
            self.ts_data_dict = self.get_duration_ts_data_dict()
        
        self.ip_ts_data_dict = ip_ts_data_dict
        
        # we will not load machine data if not exists
        # if ip_ts_data_dict: 
        #     self.ip_ts_data_dict = ip_ts_data_dict
        # else:
        #     self.ip_ts_data_dict = self.get_host_ts_data_dict()

        self.all_nodes = self.nodes_id[:]
        if self.ip_ts_data_dict:
            self.all_nodes += self.ip_list        


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

    def get_operation_ip_dict(self):

        operation_ip_dict = {}
        for node_id in self.nodes_id:
            ip = self.trace_data_dict[node_id]['serverIp'][0]
            operation_ip_dict[node_id] = ip
        
        return operation_ip_dict

    def get_adjacency_dict(self):
        adjacency_dict = {}
        for node in self.nodes_id:
            adjacency_dict[node] = set()
        for ip in self.ip_list:
            adjacency_dict[ip] = set()
        for edge in self.edges:
            adjacency_dict[edge.source_id] = edge.target_id
        for operation, ip in self.operation_ip_dict.items():
            if self.ip_mapping is not None:
                ip = self.ip_mapping[ip]
            adjacency_dict[operation] = ip
            adjacency_dict[ip] = operation

        return adjacency_dict

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
    
    def get_similarity(self, a, b):
        # if a and b and np.var(a) and np.var(b):
        #     pearson_corr = abs(np.corrcoef(a, b)[0, 1])
        #     return pearson_corr
        # else:
        #     return 0.01
        return utils.similarity(a, b, default_value=0.01)

    def get_similarity_dict(self):
        similarity_dict = {}
        request_rt = self.ts_data_dict[self.root_id]['MaxDuration']

        for nodes in self.nodes_id:
            similarity_dict[nodes] = self.get_similarity(self.ts_data_dict[nodes]['MaxDuration'][-self.time_window:], 
                request_rt[-self.time_window:])
        
        for ip in self.ip_list:
            if ip not in self.ip_ts_data_dict: # added on 2022-3-26
                continue             
            similarity_list = []
            for metric in self.machine_metrics:
                
                if metric in self.ip_ts_data_dict[ip].keys():
                    ip_ts_data = self.ip_ts_data_dict[ip][metric]

                    # only for dbaas
                    if len(ip_ts_data) > len(request_rt)*2: # convert 30s to 1m
                        ip_ts_data = [ip_ts_data[i] for i in range(-1,-len(request_rt)*2,-2)]        

                    similarity_list.append(self.get_similarity(ip_ts_data[-self.time_window:], 
                        request_rt[-self.time_window:]))    
            if len(similarity_list) != 0:
                similarity_dict[ip] = max(similarity_list)
            else:
                similarity_dict[ip] = 0.01 # use the default value

        return similarity_dict

    def get_transferring_probability_matrix(self, rho=0.5):
        all_nodes = self.all_nodes
        transferring_probability_matrix = np.mat(np.zeros((self.service_num, self.service_num)))
        for i in all_nodes:
            for j in all_nodes:
                if i == j:
                    temp = []
                    for k in all_nodes:
                        if k in self.adjacency_dict: # added on 2022-3-26: only if existing
                            temp.append(self.similarity_dict[i] - self.similarity_dict[k])
                    if len(temp) > 0:
                        transferring_probability_matrix[all_nodes.index(i), all_nodes.index(j)] = max([0] + [min(temp)])
                    # transferring_probability_matrix[i, j] = max([0] + temp)
                elif j in self.adjacency_dict[i]:
                    transferring_probability_matrix[all_nodes.index(i), all_nodes.index(j)] = self.similarity_dict[j]
                elif i in self.adjacency_dict[j]:
                    transferring_probability_matrix[all_nodes.index(i), all_nodes.index(j)] = rho * self.similarity_dict[j]

        
        for i in range(self.service_num):
            row_sum = np.nansum(transferring_probability_matrix[i])
            if row_sum == 0: # added on 2022-3-28: to avoid zero division, fill in the default value
                transferring_probability_matrix[i] = [1/self.service_num] * self.service_num
            else:
                transferring_probability_matrix[i] /= row_sum

        return transferring_probability_matrix

    def iteration(self, degree_of_convergence=0.001):
        score = np.array([1 / self.service_num for _ in range(self.service_num)])
        for i in range(10000):
            score_next = score * self.transferring_matrix
            if np.linalg.norm(score_next - score) < degree_of_convergence:
                break
            score = score_next

        score_dict = {}
        for i in range(self.service_num):
            score_dict[self.all_nodes[i]] = score_next[0, i]
        score_dict = dict(sorted(score_dict.items(), key=lambda x: x[1], reverse=True))

        return score_dict

    def run(self):
        self.service_num = len(self.all_nodes)
        self.similarity_dict = self. get_similarity_dict()
        self.transferring_matrix = self.get_transferring_probability_matrix()
        score = self.iteration()

        return score



