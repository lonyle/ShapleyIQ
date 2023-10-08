import numpy as np
import random


def similarity(a, b):
    if np.var(a) and np.var(b):
        return abs(np.corrcoef(a, b)[1, 0])
    else:
        return 0


def get_similarity_list():
    similarity_list = [0] * service_num
    for i in component_services:
        similarity_list[i] = similarity(RT[i], RT_request)
    for i in collocated_services:
        similarity_list[i] = max([similarity(j[i], RT_request) for j in [CPU, memory, IO, network]])

    return similarity_list


def get_transferring_probability_matrix(similarity_list, rho=0.5):
    transferring_probability_matrix = np.mat(np.zeros((service_num, service_num)))
    for i in range(service_num):
        for j in range(service_num):
            if i == j:
                temp = [1]
                for k in range(service_num):
                    if adjacency_matrix[i, k] == 1:
                        temp.append(similarity_list[i] - similarity_list[k])
                transferring_probability_matrix[i, j] = max([0] + [min(temp)])
                # transferring_probability_matrix[i, j] = max([0] + temp)
            elif adjacency_matrix[i][j]:
                transferring_probability_matrix[i, j] = similarity_list[j]
            elif adjacency_matrix[j][i]:
                transferring_probability_matrix[i, j] = rho * similarity_list[j]

    for i in range(service_num):
        row_sum = np.sum(transferring_probability_matrix[i])
        transferring_probability_matrix[i] /= row_sum

    return transferring_probability_matrix


def iteration(transferring_probability_matrix, degree_of_convergence):
    score = np.array([1 / service_num for _ in range(service_num)])
    while True:
        score_next = score * transferring_probability_matrix
        if np.linalg.norm(score_next - score) < degree_of_convergence:
            break
        score = score_next

    return score_next



if __name__ == '__main__':
    # a synthetic graph in paper for test
    # service_num = 6
    # component_services = [0, 1, 2]
    # collocated_services = [3, 4, 5]
    # collocation_dependency_edges = [(0, 3), (3, 0), (1, 4), (4, 1), (2, 5), (5, 2)]
    # service_call_edges = [(0, 1), (1, 2)]

    # adjacency_matrix = np.zeros((service_num, service_num))
    # for edge in collocation_dependency_edges + service_call_edges:
    #     adjacency_matrix[edge[0]][edge[1]] = 1

    # # synthetic metrics
    # RT_request = [1, 2, 1, 10, 1, 2, 1]

    # RT = [[1, 2, 1, 2, 1, 2, 1],
    #       [1, 2, 1, 2, 1, 2, 1],
    #       [1, 2, 1, 2, 1, 2, 1],
    #       [1, 2, 1, 2, 1, 2, 1],
    #       [1, 2, 1, 2, 1, 2, 1],
    #       [1, 2, 1, 2, 1, 2, 1]]

    # CPU = [[1, 2, 1, 2, 1, 2, 1],
    #        [1, 2, 1, 2, 1, 2, 1],
    #        [1, 2, 1, 2, 1, 2, 1],
    #        [1, 2, 1, 2, 1, 2, 1],
    #        [1, 2, 1, 2, 1, 2, 1],
    #        [1, 2, 1, 10, 1, 2, 1]]

    # memory = [[1, 2, 1, 2, 1, 2, 1],
    #           [1, 2, 1, 2, 1, 2, 1],
    #           [1, 2, 1, 2, 1, 2, 1],
    #           [1, 2, 1, 2, 1, 2, 1],
    #           [1, 2, 1, 2, 1, 2, 1],
    #           [1, 2, 1, 2, 1, 2, 1]]

    # IO = [[1, 2, 1, 2, 1, 2, 1],
    #       [1, 2, 1, 2, 1, 2, 1],
    #       [1, 2, 1, 2, 1, 2, 1],
    #       [1, 2, 1, 2, 1, 2, 1],
    #       [1, 2, 1, 2, 1, 2, 1],
    #       [1, 2, 1, 2, 1, 2, 1]]

    # network = [[1, 2, 1, 2, 1, 2, 1],
    #            [1, 2, 1, 2, 1, 2, 1],
    #            [1, 2, 1, 2, 1, 2, 1],
    #            [1, 2, 1, 2, 1, 2, 1],
    #            [1, 2, 1, 2, 1, 2, 1],
    #            [1, 2, 1, 2, 1, 2, 1]]

    # similarity_list = get_similarity_list()
    # print(similarity_list)
    # Q = get_transferring_probability_matrix(similarity_list)
    # print(Q)
    # root_cause_score = iteration(Q, 0.001)
    # print(root_cause_score)


    service_num = 4
    component_services = [0, 1, 2, 3]
    collocated_services = []
    collocation_dependency_edges = []
    service_call_edges = [(0, 1), (0, 2), (2, 3)]

    adjacency_matrix = np.zeros((service_num, service_num))
    for edge in collocation_dependency_edges + service_call_edges:
        adjacency_matrix[edge[0]][edge[1]] = 1

    # synthetic metrics
    RT_request = [3, 3, 3, 3, 6, 6]

    RT = [[3, 3, 3, 3, 6, 6],
          [1, 1, 1, 1, 2, 2],
          [2, 2, 2, 2, 4, 4],
          [2, 2, 2, 2, 4, 4]]

    CPU = [[1, 2, 1, 2, 1, 2, 1],
           [1, 2, 1, 2, 1, 2, 1],
           [1, 2, 1, 2, 1, 2, 1],
           [1, 2, 1, 2, 1, 2, 1],
           [1, 2, 1, 2, 1, 2, 1],
           [1, 2, 1, 10, 1, 2, 1]]

    memory = [[1, 2, 1, 2, 1, 2, 1],
              [1, 2, 1, 2, 1, 2, 1],
              [1, 2, 1, 2, 1, 2, 1],
              [1, 2, 1, 2, 1, 2, 1],
              [1, 2, 1, 2, 1, 2, 1],
              [1, 2, 1, 2, 1, 2, 1]]

    IO = [[1, 2, 1, 2, 1, 2, 1],
          [1, 2, 1, 2, 1, 2, 1],
          [1, 2, 1, 2, 1, 2, 1],
          [1, 2, 1, 2, 1, 2, 1],
          [1, 2, 1, 2, 1, 2, 1],
          [1, 2, 1, 2, 1, 2, 1]]

    network = []

    similarity_list = get_similarity_list()
    print(similarity_list)
    for rho_input in [i / 10 for i in range(11)]:
        Q = get_transferring_probability_matrix(similarity_list, rho=rho_input)
        # print(Q)
        root_cause_score = iteration(Q, 0.001)
        print('\nrho=' + str(rho_input))
        print(root_cause_score)