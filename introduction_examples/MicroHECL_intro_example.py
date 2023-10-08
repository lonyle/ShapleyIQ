import numpy as np
import random


class GraphAM:
    def __init__(self, mat):
        self.mat = mat
        self.v_num = len(mat)

    def out_deg(self, vi):
        cnt = 0
        for i in self.mat[vi]:
            if i != 0:
                cnt += 1
        return cnt

    def out_neighbour(self, vi):
        out_neighbour = []
        for j in range(self.v_num):
            if self.mat[vi][j] != 0:
                out_neighbour.append(j)
        return out_neighbour

    def in_neighbour(self, vj):
        in_neighbour = []
        for i in range(self.v_num):
            if self.mat[i][vj] != 0:
                in_neighbour.append(i)
        return in_neighbour


def QPS_Anomaly_Detection(edge):
    # Using existing methods
    return QPS_Anomaly_matrix[edge[0]][edge[1]]


def RT_Anomaly_Detection(edge):
    # Using existing methods
    return RT_Anomaly_matrix[edge[0]][edge[1]]


def EC_Anomaly_Detection(edge):
    # Using existing methods
    return EC_Anomaly_matrix[edge[0]][edge[1]]


def pearson_correlation_function(a, b):
    # without data
    return 1


def get_next_anomalous_edge(node, anomaly):
    detection_func_dict = {'RT': RT_Anomaly_Detection,
                           "EC": EC_Anomaly_Detection,
                           'QPS': QPS_Anomaly_Detection}
    assert anomaly in detection_func_dict.keys()

    next_anomalous_edge = []
    if anomaly in ['RT', 'EC']:
        next_nodes = call_graph.out_neighbour(node)
        next_edges = [(node, i) for i in next_nodes]
    else:
        next_nodes = call_graph.in_neighbour(node)
        next_edges = [(i, node) for i in next_nodes]

    for i in next_edges:
        if detection_func_dict.get(anomaly)(i):
            next_anomalous_edge.append(i)

    return next_anomalous_edge


def get_correlated_anomalous_edges(edge, anomaly, direction):
    # Pruning Strategy
    correlated_anomalous_edges = []
    current_node = edge[direction]
    next_anomalous_edge = get_next_anomalous_edge(current_node, anomaly)
    for i in next_anomalous_edge:
        if pearson_correlation_function(i, edge):
            correlated_anomalous_edges.append(i)

    return correlated_anomalous_edges


def anomaly_propagation_analysis(entry_node, anomaly, direction):
    current_edges = get_next_anomalous_edge(entry_node, anomaly)
    end_nodes = []
    while current_edges:
        next_edges = []
        for i in current_edges:
            correlated_anomalous_edges = get_correlated_anomalous_edges(i, anomaly, direction)
            if not get_correlated_anomalous_edges(i, anomaly, direction):
                end_nodes.append(i[direction])
            else:
                next_edges += correlated_anomalous_edges
        current_edges = next_edges

    return end_nodes


def candidate_root_cause_ranking(candidate_list, entry_node):
    score = [pearson_correlation_function(i, entry_node) for i in candidate_list]
    return [candidate_list[i] for i in np.argsort(score)]


def test():
    RT_root_cause_candidate = anomaly_propagation_analysis(initial_anomalous_service, 'RT', direction=1)
    print(RT_root_cause_candidate)
    # EC_root_cause_candidate = anomaly_propagation_analysis(initial_anomalous_service, 'EC', direction=1)
    # print(EC_root_cause_candidate)
    # QPS_root_cause_candidate = anomaly_propagation_analysis(initial_anomalous_service, 'QPS', direction=0)
    # print(QPS_root_cause_candidate)
    # candidate_list = RT_root_cause_candidate + EC_root_cause_candidate + QPS_root_cause_candidate
    # print(candidate_root_cause_ranking(candidate_list, initial_anomalous_service))


if __name__ == '__main__':
    # a synthetic graph in paper for test
    # adjacency_matrix = [[0, 0, 1, 1, 0, 0, 0, 0, 0, 0],
    #                     [0, 0, 0, 1, 0, 0, 0, 0, 0, 0],
    #                     [0, 0, 0, 0, 1, 0, 0, 0, 0, 0],
    #                     [0, 0, 0, 0, 1, 1, 0, 0, 0, 0],
    #                     [0, 0, 0, 0, 0, 0, 1, 1, 0, 0],
    #                     [0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
    #                     [0, 0, 0, 0, 0, 0, 0, 0, 1, 1],
    #                     [0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
    #                     [0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
    #                     [0, 0, 0, 0, 0, 0, 0, 0, 0, 0]]
    # call_graph = GraphAM(adjacency_matrix)

    # initial_anomalous_service = 4

    # QPS_Anomaly_edge = [(0, 3), (3, 4)]
    # QPS_Anomaly_matrix = [[0, 0, 0, 1, 0, 0, 0, 0, 0, 0],
    #                       [0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
    #                       [0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
    #                       [0, 0, 0, 0, 1, 0, 0, 0, 0, 0],
    #                       [0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
    #                       [0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
    #                       [0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
    #                       [0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
    #                       [0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
    #                       [0, 0, 0, 0, 0, 0, 0, 0, 0, 0]]

    # RT_Anomaly_edge = [(4, 6), (6, 8), (6, 9)]
    # RT_Anomaly_matrix = [[0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
    #                      [0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
    #                      [0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
    #                      [0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
    #                      [0, 0, 0, 0, 0, 0, 1, 0, 0, 0],
    #                      [0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
    #                      [0, 0, 0, 0, 0, 0, 0, 0, 1, 1],
    #                      [0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
    #                      [0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
    #                      [0, 0, 0, 0, 0, 0, 0, 0, 0, 0]]

    # EC_Anomaly_edge = [(4, 6), (6, 8), (6, 9)]
    # EC_Anomaly_matrix = [[0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
    #                      [0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
    #                      [0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
    #                      [0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
    #                      [0, 0, 0, 0, 0, 0, 0, 1, 0, 0],
    #                      [0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
    #                      [0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
    #                      [0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
    #                      [0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
    #                      [0, 0, 0, 0, 0, 0, 0, 0, 0, 0]]

    adjacency_matrix = [[0, 1, 1, 0],                 # adjacency matrix
                        [0, 0, 0, 0],
                        [0, 0, 0, 1],
                        [0, 0, 0, 0]]
    call_graph = GraphAM(adjacency_matrix)

    initial_anomalous_service = 0

    QPS_Anomaly_edge = []
    QPS_Anomaly_matrix = []

    RT_Anomaly_edge = [(0, 1), (0, 2), (2, 3)]
    RT_Anomaly_matrix = [[0, 1, 1, 0],                 
                        [0, 0, 0, 0],
                        [0, 0, 0, 1],
                        [0, 0, 0, 0]]

    EC_Anomaly_edge = []
    EC_Anomaly_matrix = []


    test()
