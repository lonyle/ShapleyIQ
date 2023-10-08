''' the first experiment
'''
import os
import subprocess
import pandas as pd

from rca4tracing.rca.experiment.evaluator import Evaluator

folder_name = 'experiment_dbaas'

def do_clear():
    input_path = f'rca4tracing/rca/experiment/input_data/{folder_name}/'
    error_trace = pd.read_csv(input_path + 'error_trace.csv')  
    clear_input_data(input_path, error_trace)

def clear_input_data(input_path, error_trace):
    for index, row in error_trace.iterrows():
        trace_id = row.trace_id
        origin_path = input_path + '{}/data1/.'.format(trace_id)
        target_folder = input_path + 'tmp/' + trace_id
        if os.path.exists(origin_path):
            subprocess.check_output(['mkdir', '-p', target_folder])
            subprocess.check_output(['cp', '-R', origin_path, target_folder])

def df2dict(anomaly_traces_df):
    anomaly_traces_with_root_cause = dict()
    for index, row in anomaly_traces_df.iterrows():
        trace_id = row.trace_id
        root_cause = row.root_cause
        root_cause_list = root_cause.split(',')
        anomaly_traces_with_root_cause[trace_id] = root_cause_list
    return anomaly_traces_with_root_cause

def run():
    input_path = f'rca4tracing/rca/experiment/input_data/{folder_name}/'
    output_path= f'rca4tracing/rca/experiment/output_data/{folder_name}/'

    anomaly_traces_df = pd.read_csv(input_path + 'error_trace.csv')    

    algorithm_with_params={
        'MicroHECL': {}, 
        'MicroRCA': {'operation_only': True}, 
        'TON': {'operation_only': True}, 
        'MicroRank': {}, 
        'ShapleyValueRCA': {'using_cache': False}
    }
    top_k_list=[1, 2,3]
    
    evaluator = Evaluator(input_path, output_path,
                          algorithm_with_params=algorithm_with_params,
                          top_k_list=top_k_list, 
                          data_source='dbaas')
    top_k = evaluator.get_precision_rate( df2dict(anomaly_traces_df) )

    evaluator.save_result(top_k, evaluator.output_folder+'/')
    evaluator.get_bar_graph(top_k, evaluator.output_folder+'/',
                           algorithms=list(algorithm_with_params.keys()),
                           top_k_list=top_k_list)

if __name__ == '__main__':
    run()