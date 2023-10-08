import logging
import sys
import argparse
import os

from utils.tools import run_external_applicaton

from rca4tracing.common.utils import get_proj_dir

proj_dir = get_proj_dir()
folder_name = os.path.join(proj_dir, 'rca4tracing/fault_injection/workload_generator')


def run(test_id, users=2, run_time=240, spawn_rate=50, host="http://localhost:8080"):
    driver = folder_name+"/locustfile.py"
    # print(driver)
    # host = "http://localhost:8080"  # current_configuration["locust_host_url"]
    load = users  # current_configuration["load"]
    # spawn_rate = 50  # current_configuration["spawn_rate_per_second"] user spawn / second
    # run_time = 240  # current_configuration["run_time_in_seconds"]
    log_file = folder_name+"/output/locust_test.log"  # os.path.splitext(driver)[0] + ".log"
    # print ('folder_name:', folder_name)
    out_file = folder_name+"/output/locust_test.out"  # os.path.splitext(driver)[0] + ".out"
    csv_prefix = folder_name+"/output/result"  # os.path.join(os.path.dirname(driver), "result")
    logging.info(f"Running the load test for {test_id}, with {load} users, running for {run_time} seconds.")

    print(f'test_id:{test_id}, load:{load}, spawn_rate:{spawn_rate}')
    run_external_applicaton(
        f'locust --locustfile {driver} --host {host} --users {load} --spawn-rate {spawn_rate} --run-time {run_time}s '
        f'--headless --only-summary --csv {csv_prefix} --csv-full-history --logfile "{log_file}" --loglevel DEBUG >> '
        f'{out_file} 2> {out_file}',
        False)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    parser.add_argument('--run_time',
                        help="run time of the test",
                        default=200,
                        type=int)
    parser.add_argument('--users',
                        default=20,
                        type=int)
    parser.add_argument('--spawn_rate',
                        default=20,
                        type=int)
    parser.add_argument('--host',
                        default='http://localhost:8080',
                        type=str)

    args = parser.parse_args()
    test_id = 1
    run(test_id, 
        users=args.users, 
        run_time=args.run_time, 
        spawn_rate=args.spawn_rate,
        host=args.host)
    #blade create network delay --time 3000 --interface eth0 --local-port 12345 --timeout 20