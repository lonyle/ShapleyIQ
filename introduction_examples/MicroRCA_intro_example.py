import numpy as np
import random


def get_anomalous_nodes(anomalous_edges):
    anomalous_nodes = []
    for edge in anomalous_edges:
        anomalous_nodes.append(edge[1])

    return anomalous_nodes


def anomalous_graph_extraction(graph, anomalous_nodes):
    anomalous_graph = graph

    for i in range(len(graph)):
        for j in range(len(graph)):
            if i not in anomalous_nodes and j not in anomalous_nodes:
                anomalous_graph[i, j] = 0

    return anomalous_graph


def pearson_correlation_function(a, b):
    return np.corrcoef(a, b)[1, 0]


def edge_weighing(anomalous_graph, anomalous_edges, anomalous_nodes, alpha):
    weighted_edge_mat = np.zeros((num_node, num_node))
    for j in anomalous_nodes:
        for i in range(len(anomalous_graph)):
            if anomalous_graph[i, j] == 1:
                if (i, j) in anomalous_edges:
                    weighted_edge_mat[i, j] = alpha
                else:
                    # rt_a: the average of anomalous response times for each anomalous service node
                    # rt: response times of edge
                    weighted_edge_mat[i, j] = pearson_correlation_function(rt_a[j], rt[i][j])

        for k in range(len(anomalous_graph)):
            if anomalous_graph[j, k] == 1:
                if k < num_service:
                    weighted_edge_mat[j, k] = pearson_correlation_function(rt_a[j], rt[j][k])
                else:
                    avg_win = np.sum(weighted_edge_mat[:, j]) / np.sum(anomalous_graph[:, j])
                    max_cor = 0
                    for u in uh[k - num_service]:
                        cor = pearson_correlation_function(rt_a[j], u)
                        if cor > max_cor:
                            max_cor = cor
                    weighted_edge_mat[j, k] = avg_win * max_cor

    for i in range(num_node):
        for j in range(num_node):
            if weighted_edge_mat[i, j] < 0:
                weighted_edge_mat[i, j] = -weighted_edge_mat[i, j]

    return weighted_edge_mat


def service_anomaly_score():    # container resource utilization is unclear
    return [0.25, 0.25, 0.25, 0.25]      # random


def localizing_faulty_services(edge_weight, anomalous_nodes_weight, length):
    # Personalized PageRank
    location = 0
    steps_num = 0
    steps = []  #
    while steps_num < length:
        rand = random.random()
        if np.sum(edge_weight[location]) > 0 and rand > 0.15:
            location = random.choices(range(num_node), weights=edge_weight[location])[0]
            steps.append(location)
            steps_num += 1
        else:
            location = random.choices(range(num_node), weights=anomalous_nodes_weight)[0]
            steps.append(location)
            steps_num += 1

    frequency = [0 for _ in range(num_node)]
    length_burned = int(length / 2)
    for i in steps[length_burned:]:
        frequency[i] += 1
    score = [frequency[i]/length_burned for i in range(len(frequency))]

    return score


def rank_service(score):
    return np.argsort(score[:num_service])[::-1]


def test():
    # 
    for alpha_input in [i / 10 for i in range(11)]:
        anomalous_nodes_ = get_anomalous_nodes(anomalous_edges1)
        anomalous_graph_ = anomalous_graph_extraction(graph2, anomalous_nodes_)
        edge_weight_ = edge_weighing(anomalous_graph_, anomalous_edges1, anomalous_nodes_, alpha=alpha_input)
        anomalous_nodes_weight_ = service_anomaly_score()
        score_ = localizing_faulty_services(edge_weight_, anomalous_nodes_weight_, length=10000)
        rank_ = rank_service(score_)
        # print('anomalous nodes:')
        # print(anomalous_nodes_)
        # print('anomalous graph:')
        # print(anomalous_graph_)
        # print('edge weight:')
        # print(edge_weight_)
        # print('score:')
        # print(score_)
        # print('rank:')
        # print(rank_)
        print('\nalpha=' + str(alpha_input))
        print([score_[i] for i in rank_])
        nodes_id = ['A', 'B', 'C', 'D']
        print(', '.join([nodes_id[i] for i in rank_]))


if __name__ == '__main__':
    # a synthetic graph in paper for test
    # num_node = 7s
    # num_service = 5
    # num_host = 2

    # # graph1 = [[1, 2, 5],  # s1      # Adjacency List
    # #           [2, 3, 6],  # s2
    # #           [4, 5],  # s3
    # #           [6],  # s4
    # #           [5],  # s5
    # #           [],  # h1
    # #           []]  # h2

    # graph2 = np.mat([[0, 1, 1, 0, 0, 1, 0],                 # adjacency matrix
    #                  [0, 0, 1, 1, 0, 0, 1],
    #                  [0, 0, 0, 0, 1, 1, 0],
    #                  [0, 0, 0, 0, 0, 0, 1],
    #                  [0, 0, 0, 0, 0, 1, 0],
    #                  [0, 0, 0, 0, 0, 0, 0],
    #                  [0, 0, 0, 0, 0, 0, 0]])

    # anomalous_edges1 = [(0, 1), (1, 2), (1, 3)]

    # rt_a = [[],                                             # anomalous response times
    #         [1, 2, 3],
    #         [2, 3, 4],
    #         [3, 4, 5],
    #         [],
    #         [],
    #         []]

    # rt = [[0, [1, 2, 3], [2, 3, 1], 0, 0, 0, 0],            # response times
    #       [0, 0, [2, 3, 2], [1, 2, 3], 0, 0, [3, 2, 1]],
    #       [0, 0, 0, 0, [3, 2, 1], [2, 3, 2], 0],
    #       [0, 0, 0, 0, 0, 0, [1, 2, 3]],
    #       [0, 0, 0, 0, 0, 0, 0],
    #       [0, 0, 0, 0, 0, 0, 0],
    #       [0, 0, 0, 0, 0, 0, 0]]

    # uh = [[[1, 3, 4], [1, 3, 3]],  # utilization metrics of host 1
    #       [[2, 3, 4], [3, 2, 1]]]  # utilization metrics of host 2

    
    num_node = 4
    num_service = 4
    num_host = 0

    # graph1 = [[1, 2, 5],  # s1      # Adjacency List
    #           [2, 3, 6],  # s2
    #           [4, 5],  # s3
    #           [6],  # s4
    #           [5],  # s5
    #           [],  # h1
    #           []]  # h2

    graph2 = np.mat([[0, 1, 1, 0],                 # adjacency matrix
                     [0, 0, 0, 0],
                     [0, 0, 0, 1],
                     [0, 0, 0, 0]])

    anomalous_edges1 = [(0, 1), (0, 2), (2, 3)]

    rt_a = [[3, 3, 3, 3, 6, 6],
            [1, 1, 1, 1, 2, 2],
            [2, 2, 2, 2, 4, 4],
            [2, 2, 2, 2, 4, 4]]                          # anomalous response times

    rt = [[0, [1, 1, 1, 1, 2, 2], [2, 2, 2, 2, 4, 4], 0],            # response times
          [0, 0, 0, 0],
          [0, 0, 0, [2, 2, 2, 2, 4, 4]],
          [0, 0, 0, 0]]

    uh = []  

    test()
