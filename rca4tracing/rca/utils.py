import numpy as np

def pearson_corr_func(a, b):
    if np.var(a) and np.var(b):
        pearson_corr = abs(np.corrcoef(a, b)[0, 1])
        # print('pearson_corr', pearson_corr)
        return pearson_corr
    else:
        return 0

def contribution_to_prob(adjusted_contribution_dict):
    for key in adjusted_contribution_dict:
        if adjusted_contribution_dict[key] < 0:
            adjusted_contribution_dict[key] = 0
            
    all_contribution = sum(adjusted_contribution_dict.values())
    contribution_percentage_dict = {}
    for operation_id, contribution in adjusted_contribution_dict.items():
        contribution_percentage_dict[operation_id] = contribution / all_contribution
    return contribution_percentage_dict