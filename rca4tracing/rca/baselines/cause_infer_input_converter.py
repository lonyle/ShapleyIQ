''' we need two input data:
        self.metric_collector
            self.metric_collector.root_node_ids
            self.metric_collector.calling_tree
            node_to_IPs
        self.metric_time_series
    we only need this converter for the jaeger data
'''

from rca4tracing.rca.trace_metric_collector import TraceMetricCollector
from rca4tracing.rca.experiment.get_rca_data_jaeger import RCADataJaeger

class CauseInferData:
    def __init__(self):
        self.root_node_ids = None
        self.calling_tree = None 
        self.node_to_IPs = None 
        self.metric_time_series = None

class CauseInferInputConverter:
    def __init__(self):
        trace_id = None
        self.trace_metric_collector = TraceMetricCollector(trace_id, donot_init=True)

    def get_jaeger_data(self,
                 input_path='', 
                 trace_id=None, 
                 look_back_seconds=None,
                 services_delay={}): 
        ''' this will be called for testing, otherwise, we will pass the data from convert()
        '''
        jaeger_data = RCADataJaeger(input_path, 
                                  trace_id=trace_id, 
                                  root_causes=None,
                                  look_back_seconds=look_back_seconds,
                                  services_delay=services_delay)

        
        jaeger_data.load_all_data()
        return jaeger_data

    def convert(self, data=None, **kwargs):
        if data is None:
            self.data = self.get_jaeger_data(**kwargs)        
        else:
            self.data = data

        if self.data.traces is None:
            self.data.traces = [self.data.spans]

        output_data = CauseInferData()

        
        output_data.root_node_ids, output_data.calling_tree = \
            self.trace_metric_collector.span_to_tree(self.data.traces[0])

        output_data.node_to_IPs, machine_ids, vm_ids = \
            self.trace_metric_collector.find_node2IP_and_machines_and_jvms(self.data.traces[0])

        output_data.metric_time_series = self.get_metric_time_series(output_data.node_to_IPs)        

        return output_data

    def get_metric_time_series(self, node_to_IPs):
        ''' metric_time_series is a dictionary by IP, currently we will fill
        '''
        metric_time_series = dict()
        metric_time_series.update(self.data.ip_ts_data_dict) # now empty

        for node_id in self.data.ts_data_dict:
            for IP in node_to_IPs[node_id]:
                if IP not in metric_time_series:
                    metric_time_series[IP] = dict()
                # metric_time_series[IP][node_id] = dict()
                for metric_name in self.data.ts_data_dict[node_id]:
                    time_series = self.data.ts_data_dict[node_id][metric_name]
                    
                    time_series = [float(value) for value in time_series]
                    metric_time_series[IP][metric_name] = {'time_series': time_series}
                    # metric_time_series[IP][metric_name+'.1'] = {'time_series': time_series}
        return metric_time_series

        