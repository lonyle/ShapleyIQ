''' insert the trace data to influxdb and then get out the metric
    ref to : rca4tracing/datasources/logstore/log_service_ray_driver.py
'''
import numpy as np
import time 

from rca4tracing.timeseries.influxdb import InfluxDB
from rca4tracing.fault_injection.data_collector import DataCollector #collect

from rca4tracing.common.logger import setup_logger
LOG = setup_logger(__name__, module_name='rca')

def get_tags(key='ServiceName'):
    influxdb = InfluxDB()
    q = f"show tag values with key={key}"
    res = influxdb.query(dbname, q)
    print (res)

class Trace2Metric:
    def __init__(self):
        self.dbname = 'rca_experiment'
        self.metric_to_select_field = {
            "QPS": "COUNT",
            "MaxDuration": "Max",
            "Duration": "MEAN"
        }
        from rca4tracing.anomalydetection.data_query.influxdb_trace_query import InfluxDBTraceQuery
        self.trace_query = InfluxDBTraceQuery(dbname=self.dbname)
        self.influxdb = InfluxDB()

    def traces_to_metric(self, traces):
        # this function specifies one service. We will divide into two functions
        start_time, end_time = self.insert_traces(traces)
        return self.get_metrics(start_time, end_time, 'ts-travel-service')

    def get_metrics(self, start_time, end_time, 
                    service_name=None, 
                    operation_name=None,
                    metrics=['Duration', 'QPS','MaxDuration'],
                    group_by_seconds=5):
        ''' ref: rca4tracing/anomalydetection/data_query/influxdb_data_query.py
            because the injection is at some service, so we get the metric for the service
        '''
        LOG.debug(f'start_time: {start_time}, end_time: {end_time}')
        query_start_time = start_time//1000 - group_by_seconds
        query_end_time = end_time//1000 + group_by_seconds

        select_fields = []
        for metric in metrics:
            select_fields.append(f"{self.metric_to_select_field[metric]}(\"Duration\")")

        cond_str_list = []
        if service_name is not None:
            cond_str_list.append(f"\"ServiceName\"='{service_name}'")
        if operation_name is not None:
            cond_str_list.append(f"\"OperationName\"='{operation_name}'")
        q = f"SELECT {','.join(select_fields)} FROM \"trace\" WHERE {' AND '.join(cond_str_list)}" + \
            f" AND time>{query_start_time}s AND time<{query_end_time}s group by time({group_by_seconds}s)"

        LOG.debug (q)
        res = self.influxdb.query(self.dbname, data=q)
        value_dict, timestamps = self.trace_query.process_data_multi_aggregator(res, 
            return_result_code=False, metric_list=metrics)
        LOG.debug (res)
        
        return value_dict, timestamps

    def insert_traces(self, traces):
        start_time = np.inf
        end_time = -np.inf
        
        # the trace is of the processed trace
        data_list = []
        for trace in traces:
            for span in trace:
                timestamp = span['timestamp'] # //1000 # turn it to 13-digit format, edited on 2022-3-25: do not change it into 13 digits
                if timestamp < start_time:
                    start_time = timestamp
                if timestamp > end_time:
                    end_time = timestamp

                LOG.debug (f'timestamp: {timestamp}')
                data = {
                    "metric": "trace",
                    "timestamp": timestamp,
                    "fields": {
                        "Duration": span['duration'],
                    },
                    "tags": {
                        "ServiceName": span['serviceName'],
                        "OperationName": span['operationName']
                    }
                }
                data_list.append(data)
        
        response = self.influxdb.put_list(self.dbname, data_list)
        LOG.info(f'response of trace insertion: {response.text}')
        LOG.info(f"{len(data_list)} of traces are inserted")
        return start_time, end_time

if __name__ == '__main__':
    data_collector = DataCollector(host='http://localhost:61420')
    traces, trace_ids = data_collector.collect(look_back_seconds=1800, limit=200, minDuration_ms=0, service='ts-station-service')
    
    value_dict, timestamps = Trace2Metric().traces_to_metric(traces)
    print (value_dict, timestamps)

    # get_metrics(int(time.time())-3600, 0)
    # get_tags(key='ServiceName')