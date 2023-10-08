''' the client to process the traces from jaeger
'''
import requests
import json
import urllib.parse

mapping = { # dbaas: jaeger
            'timestamp': 'startTime',
            'TraceID': 'traceID',
            'elapsed': 'duration',
            'spanId': 'spanID',
            'rpc': 'operationName'
        }

class JaegerDriver:
    def __init__(self, base_url=None):
        if base_url is None:
            # self.base_url = 'http://localhost:16686'
            # self.base_url = 'http://localhost:61420'
            from rca4tracing.fault_injection.config import jaeger_host
            self.base_url = jaeger_host
        else:
            self.base_url = base_url
        self.url_prefix = self.base_url + '/api/traces'

    def get_traces_by_params(self, **kwargs):
        ''' example of params:
        limit=2&maxDuration&minDuration&service=ts-basic-service&prettyPring=true
        '''               
        encoded_params = urllib.parse.urlencode(kwargs)
        url = self.url_prefix + '?' + encoded_params
        print (f'url: {url}')
        response = requests.get(url)
        return json.loads(response.text)

    def get_trace_detail_by_id(self, traceID):
        url = self.url_prefix + '/' + traceID
        response = requests.get(url)
        return json.loads(response.text)

    def get_dependency_graph(self, lookback_seconds=3600*24*15): # recent 15 days
        import time 
        endTs = int(time.time())*1000
        url = self.base_url + '/api/dependencies?endTs={}&lookback={}'.format(endTs, lookback_seconds*1000)
        response = requests.get(url)
        return json.loads(response.text)

    def get_all_services(self, lookback_seconds=3600*24*15):
        dependency_graph = self.get_dependency_graph(lookback_seconds=lookback_seconds)
        dependency_graph = dependency_graph["data"]
        
        services_set = set()
        for pair in dependency_graph:
            if pair["parent"] not in services_set:
                services_set.add(pair["parent"])
            if pair['child'] not in services_set:
                services_set.add(pair['child'])
        return list(services_set)

    def process_traces(self, raw_traces_data, 
                       anomaly_condition_us = {}, # should also satisfy the condition for the services
                       minDuration_us=None, maxDuration_us=None, return_trace_id=False):
        ''' each trace is a list of spans
            to be used by ShapleyRCA, each span should contain 
            ['ParentSpanId', 'Duration', 'SpanId', 'ServiceName', 'OperationName'
            'TraceID', 'TimeStamp']
        '''        
        traces = []      
        trace_ids = []  
        # print (raw_traces_data)        
        for raw_trace_data in raw_traces_data['data']:
            anomaly_flag_set = set(anomaly_condition_us.keys()) # whether an anomaly is found

            duration = 0 # the max duration of all spans is the duration for this request
            processes = raw_trace_data['processes']
            trace = []
            for span in raw_trace_data['spans']:
                
                if span['duration'] > duration:
                    duration = span['duration']

                # keep a copy in required name
                for key in mapping:
                    span[key] = span[mapping[key]]

                span['serviceName'] = ""
                if 'processID' in span:
                    if span['processID'] in processes:
                        span['serviceName'] = processes[ span['processID'] ]['serviceName']
                        for tag in processes[ span['processID'] ]['tags']: # added on 2022-3-26
                            if tag['key'] == 'ip':
                                span['serverIp'] = tag['value']
                if 'references' in span:
                    for reference in span['references']:
                        if 'refType' in reference and reference['refType'] == 'CHILD_OF':
                            span['parentSpanId'] = reference['spanID']

                if span['serviceName'] in anomaly_flag_set:
                    if span['duration'] > anomaly_condition_us[span['serviceName']]:
                        anomaly_flag_set.remove( span['serviceName'] )
               
                trace.append(span)

            if len(anomaly_flag_set) > 0:
                continue 

            if (minDuration_us is None or duration > minDuration_us) \
                    and (maxDuration_us is None or duration < maxDuration_us):
                traces.append(trace)
                trace_ids.append(raw_trace_data['traceID'])
        if return_trace_id:
            return traces, trace_ids
        else:
            return traces
        
# http://localhost:16686/api/traces/a79909039d742ba0

# http://localhost:16686/api/traces?end=1643898196772000&limit=20&lookback=1h&maxDuration&minDuration&service=ts-basic-service&start=1643894596772000 # the start and end has their meanings, the lookback might only works for the UI