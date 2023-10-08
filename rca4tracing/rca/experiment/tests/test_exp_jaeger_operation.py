from rca4tracing.rca.experiment.experiment_jaeger import run_one_experiment, get_evaluator


def run_experiment_operations():
    services_delay = {
        # "ts-ticketinfo-service": 500
        "ts-station-service": 500 # this has the unit of ms
    }
    evaluator = get_evaluator()
    normal_seconds = 15
    anomaly_seconds = 30

    run_one_experiment(services_delay,
                       normal_seconds=normal_seconds, 
                       anomaly_seconds=anomaly_seconds,
                       evaluator=evaluator,
                       mode='operation')

def run_experiment_global():
    services_delay = {
        # "ts-ticketinfo-service": 500
        "ts-station-service": 500 # this has the unit of ms
    }
    evaluator = get_evaluator()
    normal_seconds = 15
    anomaly_seconds = 30

    run_one_experiment(services_delay,
                       normal_seconds=normal_seconds, 
                       anomaly_seconds=anomaly_seconds,
                       evaluator=evaluator,
                       mode='global')
if __name__ == '__main__':
    # run_experiment_operations()
    run_experiment_global()