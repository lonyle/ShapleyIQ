# similar to rca4tracing/fault_injection/run_experiment.py and experiment1.py
''' the experiment is now in rca4tracing/fault_injection/run_experiment.py
'''

from rca4tracing.rca.experiment.evaluator import Evaluator

def run_one_trace(anomaly_traces_with_root_cause,
                  services_delay):
    input_path = 'rca4tracing/rca/experiment/input_data/experiment_jaeger/'
    output_path='rca4tracing/rca/experiment/output_data/experiment_jaeger/'

    top_k_list=[1,2,3]

    algorithm_with_params={
        # 'MicroHECL': {}, 
        # 'MicroRCA': {'operation_only': True}, 
        'TON': {'operation_only': True}, 
        # 'MicroRank': {'n_sigma': 1}, 
        # 'ShapleyValueRCA': {'multiple_metric': False},
        # 'CauseInfer': {}
    }

    evaluator = Evaluator(input_path, output_path,
                          algorithm_with_params=algorithm_with_params,
                          top_k_list=top_k_list,
                          data_source='jaeger')
    evaluator.evaluate(anomaly_traces_with_root_cause,
                       anomaly_conditions=services_delay,
                       mode='trace')

if __name__ == '__main__':
    anomaly_traces_with_root_cause = {
        # "b74a6b62f12248d4": ["ts-basic-service"]
        "171e72432179f21a": ['ts-station-service']
    }

    services_delay = {
        # "ts-station-service": 500
        # "ts-ticketinfo-service": 500
        "ts-station-service": 500
    }
    run_one_trace(anomaly_traces_with_root_cause,
                  services_delay)