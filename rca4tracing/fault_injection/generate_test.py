'''
first, run the workload generator
second, inject the time delay
Note: this program could be executed remotely, e.g. on a local machine
'''

from rca4tracing.fault_injection.data_collector import DataCollector #collect, collect_all_services
from rca4tracing.fault_injection.ssh_controller_docker import SshControlerDocker
from rca4tracing.fault_injection.ssh_controller_k8s import SshControlerK8s

# remote_prefix = "ssh root@8.136.136.64"
# remote_prefix = ""

import os
import time
import json
import subprocess
import numpy as np

from rca4tracing.common.utils import get_proj_dir
from rca4tracing.fault_injection.trace_to_metric import Trace2Metric

from rca4tracing.common.logger import setup_logger
LOG = setup_logger(__name__, module_name='rca')

class CaseGenerator:
    ''' generate test and store the data to jaeger and influxdb
    '''
    def __init__(self, 
                 services_delay={},
                 users=0,
                 spawn_rate=0,
                 normal_seconds=0, 
                 anomaly_seconds=0,
                 system_type='k8s',
                 web_host=None,
                 jaeger_host=None):
        self.users = users 
        self.spawn_rate = spawn_rate

        self.web_host = web_host 
        self.jaeger_host = jaeger_host

        self.services_delay = services_delay
        self.normal_seconds = normal_seconds
        self.anomaly_seconds = anomaly_seconds

        if system_type == 'docker':
            self.ssh_controller = SshControlerDocker(remote_prefix='ssh root@8.136.136.64')
        elif system_type == 'k8s':
            self.ssh_controller = SshControlerK8s(remote_prefix='ssh root@118.31.76.75')

    def exec_workload_generator(self, workload_second, users, spawn_rate):
        ''' this is run locally
        '''
        proj_dir = get_proj_dir()
        python_filename = os.path.join(proj_dir, 'rca4tracing/fault_injection/workload_generator', 'run_load_test.py')
        p = subprocess.Popen(['python3', python_filename, '--run_time', str(workload_second),
                '--users', str(users), '--spawn_rate', str(spawn_rate), '--host', str(self.web_host)])
        return p

    def inject_for_services(self, 
                            services_delay, 
                            users=1, spawn_rate=1,
                            normal_seconds=5, anomaly_seconds=15):
        '''
            the input is a dictionary like {"traininfo": 200}
        '''
        workload_second = int((normal_seconds + anomaly_seconds) + len(services_delay)*30)
        p = self.exec_workload_generator(workload_second, users, spawn_rate)

        # do the injection
        
        fault_id_list = []
        for service in services_delay:
            container_id, local_port = self.ssh_controller.get_container_id_for_name(service)
            fault_id = self.ssh_controller.exec_network_delay(container_id, local_port, services_delay[service])
            fault_id_list.append(fault_id)

        start_time = time.time()
        time.sleep(anomaly_seconds)

        for fault_id in fault_id_list:
            self.ssh_controller.exec_destroy_fault(fault_id)

        time.sleep(normal_seconds+10)

        p.terminate()
        
        return workload_second, start_time

    def generate(self):
        workload_second, start_time = self.inject_for_services(self.services_delay,
                                 users=self.users,
                                 spawn_rate=self.spawn_rate,
                                 normal_seconds=self.normal_seconds,
                                 anomaly_seconds=self.anomaly_seconds)

        look_back_seconds = workload_second + 30 # workload second + 30, because we have a 30s pause between two exp

        # look_back_seconds = 1800
        self.update_metrics(look_back_seconds)

        return start_time

    def update_metrics(self, look_back_seconds):
        ''' insert metrics after injecting the fault
        '''
        data_collector = DataCollector(host=self.jaeger_host)
        traces, trace_ids = data_collector.collect_all_services(
                                        look_back_seconds=look_back_seconds, 
                                        limit=2000, # almost get all the traces
                                        minDuration_us=0, 
                                        maxDuration_us=np.inf)
        trace2metric = Trace2Metric()
        start_time, end_time = trace2metric.insert_traces(traces)
        LOG.info(f'after inserting to influxdb, start_time: {start_time}, end_time: {end_time}')

if __name__ == '__main__':
    services_delay = {
        # "ts-ticketinfo-service": 500
        "ts-station-service": 100
    }
    normal_seconds = 5
    anomaly_seconds = 15
    users = 1
    spawn_rate = 1
    web_host = 'http://localhost:61416'
    jaeger_host = 'http://localhost:61420'
    case_generator = CaseGenerator(services_delay=services_delay,
                                   normal_seconds=normal_seconds,
                                   anomaly_seconds=anomaly_seconds,
                                   users=users,
                                   spawn_rate=spawn_rate,
                                   web_host=web_host,
                                   jaeger_host=jaeger_host)
    # case_generator.inject_for_services(services_delay, normal_seconds=normal_seconds, anomaly_seconds=anomaly_seconds)
    case_generator.update_metrics(look_back_seconds=1800)
    # case_generator.generate()
    
