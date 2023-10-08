from collections import defaultdict
import io
import numpy as np
import pandas as pd
from tigramite.pcmci import PCMCI
from tigramite import plotting as tp
from tigramite import data_processing as pp
from tigramite.independence_tests import ParCorr

from rca4tracing.rca.trace_metric_collector import TraceMetricCollector
from rca4tracing.rca.baselines.cause_infer_input_converter import CauseInferInputConverter, CauseInferData

from rca4tracing.common.logger import setup_logger
LOG = setup_logger(__name__, module_name='rca')


class CauseInfer:
    """
    CauseInfer is a RCA method base on causality graph learned by PC [1]

    Refereces:
    ----------
        [1] P. Chen, Y. Qi, P. Zheng, and D. Hou
            Causeinfer: Automatic and distributed performance diagnosis with hierarchical causality graph in large distributed systems
            IEEE INFOCOM 2014, 2014, pp. 1887–1895.
    """

    def __init__(self,
                 trace_id,
                 services_delay={},
                 n_sigma=1,                  # n-sigma for anomaly detection
                 max_lag=8,                  # maximumu lag
                 aggresive=False,            # aggresive use no prior knowledge
                 alpha_level=0.01,           # significant level
                 target_metric='MaxDuration',
                 anomaly_detector='three_sigma',
                 cond_ind_test='ParCorr',
                 data_source='dbaas'):
        self.max_lag = max_lag
        self.n_sigma = n_sigma
        self.trace_id = trace_id
        self.aggresive = aggresive
        self.alpha_level = alpha_level
        self.target_metric = target_metric
        self.anomaly_detector_name = anomaly_detector        

        if data_source is None:
            pass # do not initialize the data
        elif data_source == 'dbaas':
            self.metric_collector = TraceMetricCollector(trace_id=trace_id)
            self.data = CauseInferData()
            self.data.root_node_ids = self.metric_collector.root_node_ids
            self.data.calling_tree = self.metric_collector.calling_tree
            self.data.metric_time_series = self.metric_collector.get_snapshot_data()
            self.data.node_to_IPs = self.metric_collector.node_to_IPs
        elif data_source == 'jaeger':            
            self.data = CauseInferInputConverter().convert(trace_id=trace_id, 
                                                           services_delay=services_delay,
                                                           look_back_seconds=180)
        
        if cond_ind_test == 'ParCorr':
            self.cond_ind_test = ParCorr(significance='analytic')

        if self.target_metric != 'MaxDuration':
            LOG.error('Currently only support MaxDuration as the target metric!')
            exit()

    def set_data(self, data, data_source='jaeger'):
        self.data = CauseInferInputConverter().convert(data=data)


    def analyze(self):
        root_cause_nodes = dict()
        for node_id in self.data.root_node_ids:
            root_cause_nodes_ = self.analyze_single_node(node_id)
            root_cause_nodes.update(root_cause_nodes_)
        return root_cause_nodes

    def analyze_single_node(self, node_id):
        # print(node_id)
        root_cause_nodes = {}
        host_IP = self.anomaly_host_detection(node_id)
        graph, time_series_dict = self.build_causal_graph(node_id)
        root_cause_nodes_ = self.root_cause_localization(graph, time_series_dict, node_id, host_IP)
        root_cause_nodes.update(root_cause_nodes_)
        if node_id in self.data.calling_tree:
            for child in self.data.calling_tree[node_id]:
                root_cause_nodes_ = self.analyze_single_node(child)
                root_cause_nodes.update(root_cause_nodes_)
        return root_cause_nodes

    def build_causal_graph(self, node_id):
        ''' 
        '''
        time_series = []
        time_series_name = []    
        time_series_dict = {}
        for IP in self.data.node_to_IPs[node_id]:
            for metric in self.data.metric_time_series[IP]:
                if '::' not in metric or ('::' in metric and node_id in metric):
                    if np.std(self.data.metric_time_series[IP][metric]['time_series'][-self.max_lag:]) != 0:
                        time_series_name.append('{}::{}'.format(IP, metric))
                        time_series.append(self.data.metric_time_series[IP][metric]['time_series'])
                        time_series_dict[time_series_name[-1]] = time_series[-1]
        time_series = np.array(time_series).T
        dataframe = pp.DataFrame(time_series, 
                                datatime = np.arange(len(time_series)), 
                                var_names=time_series_name)
        pcmci = PCMCI(
            dataframe=dataframe, 
            cond_ind_test=self.cond_ind_test,
            verbosity=0)
        results = pcmci.run_pcmci(tau_max=self.max_lag, pc_alpha=None)

        p_matrix = results['p_matrix']
        # q_matrix = pcmci.get_corrected_pvalues(p_matrix=p_matrix, tau_max=self.max_lag, fdr_method='fdr_bh')
        val_matrix = results['val_matrix']
        time_series_graph = pcmci.get_graph_from_pmatrix(p_matrix=p_matrix, alpha_level=self.alpha_level, 
            tau_min=0, tau_max=self.max_lag, selected_links=None)        
        graph = self.get_pcmci_graph(pcmci, p_matrix, time_series_graph, val_matrix)

        return graph, time_series_dict

    def get_pcmci_graph(self, pcmci, p_matrix, time_series_graph, val_matrix):
        if time_series_graph is not None:
            sig_links = (time_series_graph != "")*(time_series_graph != "<--")
        else:
            sig_links = (p_matrix <= self.alpha_level)        
        
        graph = defaultdict(list)
        for j in range(pcmci.N):
            links = {(p[0], -p[1]): np.abs(val_matrix[p[0], j, abs(p[1])])
                     for p in zip(*np.where(sig_links[:, j, :]))}
            sorted_links = sorted(links, key=links.get, reverse=True)
            for p in sorted_links:
                if (pcmci.var_names[j] != pcmci.var_names[p[0]]) and (pcmci.var_names[p[0]] not in graph[pcmci.var_names[j]]):
                    if not self.aggresive and self.target_metric in pcmci.var_names[p[0]]:
                        pass
                    else:
                        graph[pcmci.var_names[j]].append(pcmci.var_names[p[0]])
        return graph

    def root_cause_localization(self, graph, time_series_dict, node_id, host_IP):
        root_node = '{}::{}::{}'.format(host_IP, node_id, self.target_metric)
        visit_nodes = list()
        visit_nodes.append(root_node)
        root_cause_nodes = dict()
        self.root_cause_localization_(node_id, root_node, graph, time_series_dict, visit_nodes, root_cause_nodes)
        return root_cause_nodes

    def root_cause_localization_(self, node_id, root_node, graph, time_series_dict, visit_nodes, root_cause_node, window_size=60, n_look_back_point=10, epsilon=0.001):
        children = graph[root_node]
        # DFS
        found_child_anomaly = False
        for child in children:
            if child not in visit_nodes:
                time_series = time_series_dict[child]
                found_anomaly = False
                max_z_score = 0
                for i in range(n_look_back_point):
                    j = - (i+1)
                    time_series_ = time_series[-window_size:j]
                    z_score = abs(time_series[j] - np.mean(time_series_)) / (np.std(time_series_)+epsilon)
                    if z_score > self.n_sigma:
                        found_anomaly = True
                        if z_score > max_z_score:
                            max_z_score = z_score
                if found_anomaly:
                    visit_nodes.append(child)
                    found_child_anomaly_ = self.root_cause_localization_(node_id, child, graph, time_series_dict, visit_nodes, root_cause_node)
                    if not found_child_anomaly_:
                        if node_id not in child:
                            strs = child.split("::")
                            root_cause_node['{}::{}::{}'.format(strs[0], node_id, strs[1])] = max_z_score
                        else:
                            root_cause_node[child] = max_z_score
                    else:
                        found_child_anomaly = True
        return found_child_anomaly

    def anomaly_host_detection(self, node_id):
        if len(self.data.node_to_IPs[node_id]) == 1:
            return list(self.data.node_to_IPs[node_id])[0]
        else:
            if self.target_metric == 'MaxDuration':
                pick_IP = None
                pick_IP_MaxDuration = 0
                for IP in self.data.node_to_IPs[node_id]:
                    metric_name = '{}::MaxDuration'.format(node_id)
                    if metric_name in self.data.metric_time_series[IP]:
                        max_time_series = max(self.data.metric_time_series[IP][metric_name]['time_series'])
                        if max_time_series > pick_IP_MaxDuration:
                            pick_IP_MaxDuration = max_time_series
                            pick_IP = IP
                return pick_IP

if __name__ == '__main__':
    # trace_id = '96900c2b156018c860bd65b48e2dcf40'
    # trace_id = 'c09cf81b32d89360ba8b4ab21d4fc830'
    # trace_id = 'cd4be049650d6207bcfaa81afe6444ae'

    trace_id = '5f01a51c296a4e774be333697cd4c2d5'
    cause_infer = CauseInfer(trace_id=trace_id)

    trace_id = 'b74a6b62f12248d4'
    services_delay = services_delay = {
        "ts-station-service": 500
    }
    cause_infer = CauseInfer(trace_id=trace_id, 
                             data_source='jaeger', 
                             services_delay=services_delay,
                             max_lag=2)
    # trace_id = 'b74a6b62f12248d4'
    # cause_infer = CauseInfer(trace_id=trace_id, data_source='jaeger')
    root_cause_nodes = cause_infer.analyze()
    print(root_cause_nodes)