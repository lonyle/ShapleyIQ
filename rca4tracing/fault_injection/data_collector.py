''' collect the trace that has high latency, and analyze them
    similar to test_jaeger_driver
'''

import json
import time
import numpy as np 

from rca4tracing.datasources.jaeger.driver import JaegerDriver
from rca4tracing.rca.shapley_value_rca import ShapleyValueRCA

from rca4tracing.common.logger import setup_logger
LOG = setup_logger(__name__, module_name='rca')

class DataCollector:
    def __init__(self, host=None):
        self.driver = JaegerDriver(base_url=host)

    def collect(self,
                look_back_seconds=200, 
                end_time_seconds=None,
                limit=200, 
                minDuration_us=0, 
                maxDuration_us=np.inf, 
                service='',
                anomaly_condition_ms={}):
        ''' get the data and turn it into data that our algorithm can recognize
        '''
        if end_time_seconds is not None:
            end_time = end_time_seconds
        else:
            end_time = int(time.time())
        start_time = end_time - look_back_seconds
        # driver = JaegerDriver()
        params = {
            'end': end_time * (10**6),
            'start': start_time * (10**6),
            'limit': limit,
            'lookback': 'custom',
            'maxDuration': '',
            # 'minDuration': '',
            'minDuration': f"{minDuration_us*0.7//1000}ms", # above 70%
            'service': service
        }

        anomaly_condition_us = {}
        for service in anomaly_condition_ms:
            anomaly_condition_us[service] = anomaly_condition_ms[service] * 1000*0.7

        raw_traces = self.driver.get_traces_by_params(**params)
        # print (raw_traces)
        # the minDuration should apply to the root node
        traces, trace_ids = self.driver.process_traces(raw_traces, 
                                                minDuration_us=minDuration_us,
                                                maxDuration_us=maxDuration_us,
                                                anomaly_condition_us=anomaly_condition_us,
                                                return_trace_id=True)
        if len(traces) != len(trace_ids):
            LOG.warning("len(traces) != len(trace_ids)")
        return traces, trace_ids

    def collect_by_trace_id(self,
                            trace_id):
        # driver = JaegerDriver()
        raw_traces = self.driver.get_trace_detail_by_id(trace_id)
        traces, trace_ids = self.driver.process_traces(raw_traces, return_trace_id=True)
        return traces, trace_ids

    def collect_all_services(self,
                            look_back_seconds=200, 
                            limit=200, 
                            minDuration_us=0, 
                            maxDuration_us=np.inf,
                            anomaly_condition_ms={}):
        # get all services in recent experiment
        services = self.driver.get_all_services(lookback_seconds=look_back_seconds*2) # here, we multiply by 2

        # because jaeger does not support collecting traces of all services, we get all services and remove the duplicate
        dedup_traces = []
        dedup_trace_ids = []
        trace_ids_set = set()
        for service in services:
            traces, trace_ids = self.collect(look_back_seconds=look_back_seconds, 
                                            limit=limit, 
                                            minDuration_us=minDuration_us, 
                                            maxDuration_us=maxDuration_us,
                                            service=service,
                                            anomaly_condition_ms=anomaly_condition_ms)
            for idx in range(len(trace_ids)):
                trace_id = trace_ids[idx]
                trace = traces[idx]
                if trace_id not in trace_ids_set:
                    dedup_traces.append(trace)
                    dedup_trace_ids.append(trace_id)
                    trace_ids_set.add(trace_id)

        return dedup_traces, dedup_trace_ids

if __name__ == '__main__':
    data_collector = DataCollector()
    data_collector.collect(look_back_seconds=1800, limit=200, minDuration_ms=1000, service='ts-station-service')

