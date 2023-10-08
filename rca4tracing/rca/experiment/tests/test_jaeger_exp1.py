from rca4tracing.rca.experiment.experiment_jaeger import run_one_experiment, get_evaluator

def run_experiment1():
    services_delay = {
        # "ts-ticketinfo-service": 500
        "ts-station-service": 500
    }
    evaluator = get_evaluator()
    normal_seconds = 15
    anomaly_seconds = 30
    run_one_experiment(services_delay, 
                       normal_seconds=normal_seconds, 
                       anomaly_seconds=anomaly_seconds,
                       evaluator=evaluator,
                       mode='trace') 

def run_experiment1_mutiple_faults():
    services_delay = {
        "ts-ticketinfo-service": 200,
        "ts-station-service": 200
    }
    evaluator = get_evaluator(annotation='test_multiple', select_root_cause_num=2, top_k_list=[2,3,4])
    normal_seconds = 15
    anomaly_seconds = 20
    run_one_experiment(services_delay, 
                       normal_seconds=normal_seconds, 
                       anomaly_seconds=anomaly_seconds,
                       evaluator=evaluator,
                       mode='trace',
                       sleep_seconds=0) 


if __name__ == '__main__':
    # run_experiment1()
    run_experiment1_mutiple_faults()