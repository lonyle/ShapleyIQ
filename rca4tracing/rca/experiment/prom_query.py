''' similar to rca4tracing/datasources/prometheus/driver.py
'''
import time
from prometheus_api_client.utils import parse_datetime
from datetime import datetime

from rca4tracing.anomalydetection.data_query.machine_prom_data import get_metrics, extract_value_vec

class PromQueryLocal:
    def __init__(self):
        self.url='http://localhost:9090'
        self.prom_port = 9100
        self.metric_types = ['cpu', 'memory', 'network']
        


    def get_machine_perf(self, ip, timestamp, look_back_seconds=180):
        from rca4tracing.datasources.prometheus.machine_metric_collector import MachineMetricCollector
        self.mmc = MachineMetricCollector(url=self.url)

        end_time = datetime.fromtimestamp(timestamp)
        start_time = datetime.fromtimestamp(timestamp - look_back_seconds)
        
        value_dict = dict()
        for metric_type in self.metric_types:
            res = get_metrics(self.mmc, [f"{ip}:{self.prom_port}"], metric_type, \
                start_time=start_time, end_time=end_time)
            tmp_value_dict = extract_value_vec(res, metric_type)
            value_dict.update(tmp_value_dict)
        
        return value_dict

if __name__ == '__main__':
    prom_query = PromQueryLocal()
    ip = '172.20.82.224' # 224 (master), 226 (node1), 227 (node2)
    timestamp = int(time.time())
    value_dict = prom_query.get_machine_perf(ip, timestamp)
    print (value_dict)
    print (len(value_dict['node_cpu_utilization']))

# blade create cpu load --cpu-percent 60 --cpu-count 2

