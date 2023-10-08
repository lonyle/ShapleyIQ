''' single operation and global anomalies
'''

from rca4tracing.rca.experiment.experiment_jaeger import get_all_services, run_one_experiment, get_evaluator


def run_all_experiment_operations(sleep_seconds=90):
    service_list = get_all_services()
    # service_list = ['ts-station-service']
    delay_list = [100] #[100, 200, 500, 1000]
    users_list = [5] #[1, 5, 10]
    spawn_rate_list = [5] #[1, 5, 10]

    params_to_str = f"{delay_list}{users_list}{spawn_rate_list}"

    evaluator = get_evaluator(annotation=params_to_str)
    normal_seconds = 15
    anomaly_seconds = 30

    for service in service_list:   
        for delay in delay_list:
            for users in users_list:
                for spawn_rate in spawn_rate_list:
                    services_delay = {
                        service: delay
                    }
                    run_one_experiment(services_delay,
                        users=users,
                        spawn_rate=spawn_rate,
                        normal_seconds=normal_seconds, 
                        anomaly_seconds=anomaly_seconds,
                        evaluator=evaluator,
                        mode='operation',
                        donot_collect_data=True,
                        sleep_seconds=sleep_seconds)


def run_all_experiment_global(sleep_seconds=90):
    service_list = get_all_services()
    delay_list = [100, 200, 500] #[100, 200, 500, 1000]
    users_list = [1, 5, 10] #[1, 5, 10]
    spawn_rate_list = [1, 5] #[1, 5, 10]

    params_to_str = f"{delay_list}{users_list}{spawn_rate_list}"

    evaluator = get_evaluator(annotation=params_to_str)

    normal_seconds = 15
    anomaly_seconds = 30
    for service in service_list:   
        for delay in delay_list:
            for users in users_list:
                for spawn_rate in spawn_rate_list:
                    services_delay = {
                        service: delay
                    }
                    run_one_experiment(services_delay,
                        users=users,
                        spawn_rate=spawn_rate,
                        normal_seconds=normal_seconds, 
                        anomaly_seconds=anomaly_seconds,
                        evaluator=evaluator,
                        mode='global',
                        donot_collect_data=True,
                        sleep_seconds=sleep_seconds)

if __name__ == '__main__':
    # run_all_experiment_operations()
    run_all_experiment_global()