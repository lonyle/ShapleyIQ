from os import PRIO_PGRP
from sqlite3 import Timestamp
import time
from tkinter import N
from tkinter.messagebox import NO
from turtle import up
import numpy as np
import copy
import json

from rca4tracing.graph.driver.elements import Edge
# from rca4tracing.rca.operation_level_rca import root_cause_analyze
import rca4tracing.web.edge_functions as edge_func

# from rca4tracing.api.api_get_threshold import ThresholdGetter
from rca4tracing.datasources.logstore.log_service_client import LogServiceClient
# from rca4tracing.periodic_task.error_trace_periodic_tasks import OperationAnomalyTrace

from rca4tracing.common.logger import setup_logger
LOG = setup_logger(__name__, module_name='rca')

# redis_driver = RedisDriver()
# oat = OperationAnomalyTrace()
# lsc = LogServiceClient()

class MicroRank():
    
    def __init__(self, root_id, request_timestamp, n_sigma=3):
        self.root_id = root_id
        self.request_timestamp = request_timestamp
        self.lsc = LogServiceClient()
        self.metric_list = ['MaxDuration']
        self.n_sigma = n_sigma


    def get_data(self, trace_dict=None, metrics_statistical_data=None):
        if metrics_statistical_data:
            self.metrics_statistical_data = metrics_statistical_data
        else:
            self.get_metrics_statistical_data()
        self.get_rt_threshold()

        if trace_dict:
            self.trace_dict = trace_dict
        else:
            self.get_root_trace_dict()

    def get_metrics_statistical_data(self):
        from rca4tracing.datasources.redis.driver import RedisDriver
        self.redis_driver = RedisDriver()
        self.metrics_statistical_data = {}
        self.metrics_statistical_data[self.root_id] = {}
        key = "__mean_std_" + 'MaxDuration' + "__::" + self.root_id
        self.metrics_statistical_data[self.root_id]['MaxDuration'] = \
            self.redis_driver.r.hmget(key, ['mean', 'std', 'count'])


    def get_root_trace_dict(self):
        timestamp = self.request_timestamp * 1000
        from_time = timestamp - 10 * 60 * 1000
        to_time = timestamp + 5 * 60 * 1000
        service_name, operation_name = self.root_id.split(':')
        trace_id_list = []
        trace_id_list += self.lsc.get_traceId_by_spanName(operation_name, from_time,
                            to_time, get_error=None, serviceName=service_name,
                            limit=20, timestamp=None, return_timestamp=False,
                            duration=None)

        trace_id_list += self.lsc.get_traceId_by_spanName(operation_name, from_time,
                            to_time, get_error=None, serviceName=service_name,
                            limit=20, timestamp=None, return_timestamp=False,
                            duration=self.rt_threshold)

        self.trace_dict = dict()
        for trace_id in trace_id_list:
            trace = self.lsc.get_trace_detail_by_id(trace_id, from_time,
                                                    to_time)    
            self.trace_dict[trace_id] = trace          

    def get_rt_threshold(self, threshold=3):
        mean, std, count = self.metrics_statistical_data[self.root_id]['MaxDuration']
        LOG.debug(f"mean: {mean}, std: {std}, count: {count}")

        if mean and std:
            self.rt_threshold = float(mean) + float(std) * self.n_sigma
        else:
            self.rt_threshold = 0


    def classify_trace(self):
        edges_list = [set(), set()]
        trace_operation_dict = [{}, {}]
        trace_count = [{}, {}]
        operation_trace_cover_dict = [{}, {}]
        operation_vector = [set(), set()]
        trace_vector = [set(), set()]
        
        edge_trace_list = [[], []]

        for trace_id, spans in self.trace_dict.items():
            if spans:
                root_id_list, spans_dict = self.lsc.build_span_tree(spans)
                root_duration = int(spans_dict[root_id_list[0]]['Duration'])
                edges = set(edge_func.get_edges_by_spans_dict(spans_dict, trace_id))
                operation_set = set(edge_func.get_edges_nodes_id(edges))
                if root_duration > self.rt_threshold:
                    LOG.debug(f'abnormal trace_id: {trace_id}')
                    index = 1 
                else:
                    LOG.debug(f'normal trace_id: {trace_id}')
                    index = 0
                edges_list[index].update(edges)
                operation_vector[index].update(operation_set)
                if edges not in edge_trace_list[0]:
                    edge_trace_list[0].append(list(edges))
                    edge_trace_list[1].append(trace_id)
                    trace_operation_dict[index][trace_id] = list(operation_set)
                    trace_count[index][trace_id] = 1
                    trace_vector[index].add(trace_id)
                else:
                    trace_id = edge_trace_list[1][edge_trace_list[0].index(list(edges))]
                    trace_count[index][trace_id] += 1
                for operation in operation_set:
                    if operation in operation_trace_cover_dict[index]:
                        operation_trace_cover_dict[index][operation] += 1
                    else:
                        operation_trace_cover_dict[index][operation] = 1
        
        edges_list = [[[edge.source_id, edge.target_id] for edge in edges] for edges in edges_list]
        operation_vector = [list(vector) for vector in operation_vector]
        trace_vector = [list(vector) for vector in trace_vector]

        return [edges_list, trace_operation_dict, trace_count, operation_trace_cover_dict, \
                operation_vector, trace_vector]


    def get_transition_matrix(self, edges, trace_operation_dict, operation_vector, trace_vector, omega=1):
        a_oo = np.zeros([len(operation_vector), len(operation_vector)])
        for edge in edges:
            source_id = edge[0]
            target_id = edge[1]
            a_oo[operation_vector.index(target_id), operation_vector.index(source_id)] = 1

        def column_normalize(array):
            for column in range(array.shape[1]):
                column_sum = sum(array[:,column])
                if column_sum:
                    array[:,column] /= column_sum
            return array

        a_oo = column_normalize(a_oo)

        a_ot = np.zeros([len(operation_vector), len(trace_vector)])
        for trace, operations in trace_operation_dict.items():
            for operation in operations:
                a_ot[operation_vector.index(operation), trace_vector.index(trace)] = 1
        a_to = copy.deepcopy(a_ot.T)
        a_ot = column_normalize(a_ot)
        a_to = column_normalize(a_to)
        a_tt = np.zeros([len(trace_vector), len(trace_vector)])

        a = np.vstack([np.hstack([omega * a_oo, a_ot]), np.hstack([a_to, a_tt])])
        
        return a


    def get_preference_vector(self, operation_vector, trace_operation_dict, trace_count, phi=0.5, anomalous=True):
        preference_vector = []
        for i in operation_vector:
            preference_vector.append([0])
        sum_k = sum([1 / count for count in trace_count.values()])
        sum_n = sum([( 1 / len(operations) if len(operations)>0 else 0) for operations in trace_operation_dict.values()])
        for trace, operations in trace_operation_dict.items():
            if len(operations) == 0 or sum_n == 0: # added on 2022-3-28: avoid zero division
                Theta = 0
            else:
                if anomalous:
                    Theta = phi * 1 / len(operations) / sum_n + (1 - phi) * 1 / trace_count[trace] / sum_k
                else:
                    Theta = 1 / len(operations) / sum_n
            preference_vector.append([Theta])

        return np.array(preference_vector)


    def get_init_vector(self, operation_vector, trace_vector):
        init_vector = []
        for i in operation_vector:
            init_vector.append([1 / len(operation_vector)])
        for i in trace_vector:
            init_vector.append([1 / len(trace_vector)])

        return np.array(init_vector)

    def get_pagerank_score(self, transition_matrix, init_vector, preference_vector, operation_vector,
                            damping_factor, convergence_threshold):
        pagerank_vector = copy.deepcopy(init_vector)
        for _ in range(10000):
            pagerank_vector_next = (1 - damping_factor) * transition_matrix.dot(pagerank_vector) + \
                damping_factor * preference_vector
            if np.linalg.norm(pagerank_vector_next - pagerank_vector) < convergence_threshold:
                break
            pagerank_vector = pagerank_vector_next

        score = {}
        for i in range(len(operation_vector)):
            score[operation_vector[i]] = pagerank_vector_next[i,0]

        return score


    def tarantula_spectrum_formulae(self, o_ep, o_np, o_ef, o_nf):
        fail_proportion = o_ef / (o_ef + o_nf)
        pass_proportion = o_ep / (o_ep + o_np)

        return fail_proportion / (fail_proportion + pass_proportion)

    def ochiai_spectrum_formulae(self, o_ep, o_np, o_ef, o_nf):
        return o_ef / ((o_ef + o_ep) * (o_ef + o_nf)) ** (1/2)


    def weighted_spectrum_ranker(self, normal_trace_cover_dict, anomalous_trace_cover_dict,
                                normal_pagerank_score, anomalous_pagerank_score,
                                normal_trace_count, anomalous_trace_count):
        operation_list = set(list(normal_trace_cover_dict.keys()) + (list(anomalous_trace_cover_dict.keys())))
        n_p = sum(list(normal_trace_count.values()))
        n_f = sum(list(anomalous_trace_count.values()))
        spectrum_score_dict = {}
        for operation in operation_list:
            if operation in normal_pagerank_score:
                p = normal_pagerank_score[operation]
                n_ep = normal_trace_cover_dict[operation]
            else:
                p = 0.0001
                n_ep = 0
            o_ep = p * n_ep
            o_np = p * (n_p - n_ep)

            if operation in anomalous_pagerank_score:
                f = anomalous_pagerank_score[operation]
                n_ef = anomalous_trace_cover_dict[operation]
            else:
                f = 0.0001
                n_ef = 0
            o_ef = f * n_ef
            o_nf = f * (n_f - n_ef)

            spectrum_score_dict[operation] = self.ochiai_spectrum_formulae(o_ep, o_np, o_ef, o_nf)
            
        spectrum_score_dict = dict(sorted(spectrum_score_dict.items(), key=lambda x: x[1], reverse=True))
        return spectrum_score_dict

    def run(self, phi = 0.5, omega = 0.01, d = 0.04):
        edges_list, trace_operation_dict, trace_count, operation_trace_cover_dict, \
        operation_vector, trace_vector = self.classify_trace()
        # assert trace_vector[0] 
        # assert trace_vector[1]
        if (not trace_vector[0]) or (not trace_vector[1]):
            return  {}
        
        
        preference_vector = []
        init_vector = []
        transition_matrix = []
        pagerank_score = []
        for i in range(2):
            preference_vector.append(self.get_preference_vector(operation_vector[i], trace_operation_dict[i], trace_count[i], phi, False))
            init_vector.append(self.get_init_vector(operation_vector[i], trace_vector[i]))
            transition_matrix.append(self.get_transition_matrix(edges_list[i], trace_operation_dict[i], operation_vector[i], trace_vector[i], omega))
            pagerank_score.append(self.get_pagerank_score(transition_matrix[i], init_vector[i], preference_vector[i], operation_vector[i], d, 0.001))
            # print(pagerank_score[i])
        spectrum_score = self.weighted_spectrum_ranker(operation_trace_cover_dict[0], operation_trace_cover_dict[1],
                                                pagerank_score[0], pagerank_score[1], 
                                                trace_count[0], trace_count[1])

        return spectrum_score


 
if __name__ == '__main__':

    # root_id = 'RDS_YAOCHI_ABS:AllocateInstancePublicConnection'
    # root_id = 'WORKFLOW_ENGINE:submit'
    root_id = 'DDS_YAOCHI_ABS:DescribeDBInstanceAttribute'
    timestamp = 1641797427
    microrank = MicroRank(root_id, timestamp)
    microrank.get_data()
    microrank.run()

    ''' get data from redis '''
    # key = root_id + 'MicroRank_data'
    # result = redis_driver.r.get(key)
    # trace_data = json.loads(result)

    ''' cache data to redis '''
    # key = root_id + 'MicroRank_data'
    # redis_driver.r.set(key, json.dumps(trace_data), ex=3600 * 24 * 60)     
    


    # operation_id = 'DDS_YAOCHI_ABS:DescribeDBInstanceAttribute'

    # timestamp = 1641797427000
    # service_name = operation_id.split(':')[0]
    # operation_name = operation_id.split(':')[1]
    # error_request = None
    # duration_threshold = None
    # trace_id_dict = lsc.get_traceId_by_spanName(operation_name,
    #                                             timestamp - 10 * 60 * 1000,
    #                                             timestamp + 10 * 60 * 1000,
    #                                             get_error=error_request,
    #                                             serviceName=service_name,
    #                                             limit=200,
    #                                             timestamp=None,
    #                                             return_timestamp=False,
    #                                             duration=duration_threshold)

    # print(len(trace_id_dict))

    
