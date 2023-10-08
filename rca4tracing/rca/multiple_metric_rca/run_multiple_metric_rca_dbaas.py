''' input: case id, output: distribution 
'''
import json

from rca4tracing.rca.multiple_metric_rca.gamma_distributor import GammaDistributor

filename = 'rca4tracing/rca/multiple_metric_rca/data/dbaas/data.json'

def run(case_idx, user_fatcor_time=0):    
    data = json.load(open(filename))

    mean_vec = [data['est_delay_list'][case_idx], data['gc_time'][case_idx], user_fatcor_time]
    delta_RT = data['RT_average_list'][case_idx]

    k_vec = [2, 10, 2]

    print ('before gamma distribution:', mean_vec, 'delta_RT:', delta_RT)
    gamma_distributor = GammaDistributor(mean_vec, k_vec, delta_RT)
    ret = gamma_distributor.distribute()
    print ('after gamma distribution:', ret)

    return ret

def run_for_all(num_cases):
    data = json.load(open(filename))
    adjusted_delay_list = []
    for idx in range(num_cases):
        ret = run(idx)
        adjusted_delay_list.append(ret[0])
    
    data['adjusted_delay_list'] = adjusted_delay_list
    with open(filename, 'w') as outfile:
        json.dump(data, outfile, indent=4)

if __name__ == '__main__':
    run_for_all(7)

