''' convert the performance event node to trace data
'''
import numpy as np

from rca4tracing.common.logger import setup_logger
LOG = setup_logger(__name__, module_name='rca')

add_children_visited = set()

def ps2trace(perf_schema_data, event_node_id, event_type='Transaction'):
    # prepare data to analyze why the event with event_node_id becomes slow
    root_node = perf_schema_data.nodes[event_type][event_node_id]
    span_dict = dict()
    span = node2span(root_node)

    add_span(span_dict, span)

    add_children_visited = set()

    # DFS to search the whole trace
    add_children(root_node, span_dict)
    trace = list(span_dict.values())
    return trace

def calibrate_wait_timestamp(span, parent_span):
    ''' the parent span should record the earliest time of its child span
        we assume the start time of the parent_span = the start time of its earliest child
    '''
    if 'child_timestamp' not in parent_span:
        parent_span['child_timestamp'] = span['timestamp']
    else:
        if span['timestamp'] < parent_span['child_timestamp']:
            parent_span['child_timestamp'] = span['timestamp']

    # update the timestamp of the span
    if span['timestamp'] < parent_span['timestamp']:
        # the start time cannot be before the parent span
        span['timestamp'] = parent_span['timestamp'] + (span['timestamp']-parent_span['child_timestamp'])
    
    if span['timestamp'] + span['elapsed'] > parent_span['timestamp']:
        if span['elapsed'] < parent_span['elapsed']*1.05: # only consider the case when the parent last longer, 1.05 is tolerence
            span['timestamp'] = parent_span['timestamp'] + (span['timestamp']-parent_span['child_timestamp'])

    return span

def add_span(span_dict, span):
    # for wait event, currently we do not consider
    if span['spanId'].split(':')[0] == 'Wait':
        # calibrate the wait event to its stage event
        for parentSpanId in span['parentSpanId']:
            if parentSpanId.split(':')[0] == 'Stage':
                span = calibrate_wait_timestamp(span, span_dict[parentSpanId])
                break
        # return # the old version, directly ignore

    if span is None:
        return
    if span['spanId'] not in span_dict:
        span_dict[span['spanId']] = span
    else:
        # span_dict[span['spanId']+'1'] = span
        if 'parentSpanId' not in span_dict[span['spanId']]:
            span_dict[span['spanId']]['parentSpanId'] = []
        if 'parentSpanId' in span:
            span_dict[span['spanId']]['parentSpanId'] += (span['parentSpanId'])

def find_blocking_parent(node):
    blocking_parent = None
    if node.blocking_parent is not None:
        blocking_parent = node.blocking_parent
    else:
        parent = node.parent 
        while parent is not None:
            current_node = parent 
            if current_node.blocking_parent is not None:
                blocking_parent = current_node.blocking_parent
                break
            parent = current_node.parent

    # if the blocking parent is contained in a transaction, use that transaction
    if blocking_parent is not None:
        parent = blocking_parent.parent
        while parent is not None:
            if parent.node_type == 'Transaction':
                return parent 
            parent = parent.parent
        return blocking_parent
    return None


def add_children(node, span_dict):
    ''' add a node's children recursively
        Args:
            span_dict: passed by reference, so we can update it 
                       updated on 2022-3-14: if two spans has the same span id, we only update the reference Type 
    '''    
    # print (node.node_id)
    if node.node_id == 'Statement:ThreadID:135657-EventID:15367519':
        verbose = True
    else:
        verbose = False

    if node.node_id in add_children_visited: # if this node has appeared in the span_dict, we've already added its children
        if verbose:
            print ("node.node_id in add_children_visited")
        return
    add_children_visited.add(node.node_id)

    # if len(node.children) == 0 and node.blocking_parent is None:
    #     return # quit the DFS

    blocking_parent = find_blocking_parent(node)
    if blocking_parent is not None:
        span = node2span(blocking_parent, parent_node=node,
                        #  min_start_time=node.data['TIMER_START'],
                        #  max_end_time=node.data['TIMER_END']
                        )
        add_span(span_dict, span)
        add_children(blocking_parent, span_dict) 

    for child_node_id in node.children:
        child = node.children[child_node_id]
        span = node2span(child, parent_node=node,
                        #  min_start_time=node.data['TIMER_START'],
                        #  max_end_time=node.data['TIMER_END']
                        )   
        add_span(span_dict, span)
        add_children(child, span_dict)

def node2span(node, 
              parent_node=None, 
              min_start_time=-np.inf, 
              max_end_time=np.inf):
    span = dict()
    span['spanId'] = node.node_id
    if (parent_node is not None) and (parent_node.node_id is not None):
        span['parentSpanId'] = [parent_node.node_id]
    
    if node.data['TIMER_END'] is None:
        return None
    start_time = max(min_start_time, node.data['TIMER_START'])
    end_time = min(max_end_time, node.data['TIMER_END'])
    span['timestamp'] = (start_time)//(10**4)
    span['elapsed'] = (end_time - start_time)//(10**4)

    if span['elapsed'] == 0:
        return None
    span['rpc'] = node.node_id
    span['serviceName'] = node.data['EVENT_NAME']
    if 'SQL_TEXT' in node.data:
        span['excepInfo'] = node.data['SQL_TEXT']
    return span

def trace2jaeger(trace, trace_id=None):
    # convert the trace format defined in the dbaas to the standard jaeger format, so that we can visualize it
    # a jaeger trace is not just a list of spans, but has "data" and more fields

    # the following mapping should be synced with rca4tracing/datasources/jaeger/driver.py
    mapping = { # jaeger: dbaas
        'startTime': 'timestamp',
        'traceID': 'TraceID',
        'duration': 'elapsed',
        'spanID': 'spanId',
        'operationName': 'rpc'
    }
    span_ids = set()
    jaeger_data = dict()

    if trace_id is None:
        trace_id = trace[0]['TraceID']
    
    one_trace = dict()
    one_trace['traceID'] = trace_id
    jaeger_spans = []
    processes = dict()
    service_to_process_id = dict()
    for span in trace:
        jaeger_span = dict()
        
        if 'traceID' not in jaeger_span:
            jaeger_span['traceID'] = trace_id
        for key in mapping:
            if key not in jaeger_span:
                jaeger_span[key] = span[mapping[key]]
        
        if jaeger_span['spanID'] in span_ids:
            continue # after our processing, we may have multiple spans with one span ID (usually blocking relationship)
        span_ids.add(jaeger_span['spanID'])

        # jaeger_span['duration'] = max(0, jaeger_span['duration'])
        jaeger_span['references'] = []
        if 'parentSpanId' in span:
            if type(span['parentSpanId']) != list:
                parentSpanIds = [span['parentSpanId']]
            else:
                parentSpanIds = span['parentSpanId']
            for parentSpanId in parentSpanIds:
                jaeger_span['references'].append({
                    "refType": "CHILD_OF",
                    "traceID": trace_id,
                    "spanID": parentSpanId
                })
        # fill the fields that might be necessary
        jaeger_span['tags'] = []
        jaeger_span['logs'] = []
        if 'excepInfo' in span:
            log = {
                    "timestamp": jaeger_span['startTime'],
                    "fields": [
                        {
                            "key": "SQL_TEXT",
                            "type": "string",
                            "value": span['excepInfo']
                        }
                    ]
                }
            jaeger_span['logs'].append(log)
        
        jaeger_span['flags'] = 1

        if span['serviceName'] not in service_to_process_id:
            processID = 'p' + str(len(processes) + 1)
            processes[processID] = {"serviceName": span['serviceName'], "tags":[]}
            service_to_process_id[ span['serviceName'] ] = processID
        else:
            processID = service_to_process_id[span['serviceName']]
        jaeger_span['processID'] = processID
        
        jaeger_span["warnings"] = None

        jaeger_spans.append(jaeger_span)
    one_trace['spans'] = jaeger_spans
    one_trace["processes"] = processes
    one_trace["warnings"] = None    
    
    jaeger_data['data'] = [one_trace]    
    jaeger_data["total"] = 0
    jaeger_data["limit"] = 0
    jaeger_data["offset"] = 0
    jaeger_data["errors"] = None
    return jaeger_data

def ps_data_to_traces_(data):
    ''' each event corresponds to a span, and we need to record the following fields:
        timestamp: 13 digits timer_start
        rpc: event name
        spanId: event id
        parentSpanId: nesting event id
        elapsed: timer_end - timer_start
        excepInfo: SQL_TEXT

        the challenge is how to split the histories to traces (grouped by statement or transaction)
        trace id: the event id of the statement or transaction
    '''
    
    ''' we have a two-pass approach, where the first pass process the spans and the second pass add the trace id
    '''
    # the first pass
    span_dict = dict()
    for key in data:
        for row in data[key]: # a list of records
            span = dict()
            span['rpc'] = row['EVENT_NAME']
            span['timestamp'] = row['TIMER_START']
            spanId = "{}:{}".format(key, row['EVENT_ID'])
            span['spanId'] = spanId
            if row['NESTING_EVENT_ID'] is None:
                span['parentSpanId'] = None
            else:
                span['parentSpanId'] = "{}:{}".format(row['NESTING_EVENT_TYPE'], row['NESTING_EVENT_ID'])
            if 'SQL_TEXT' in row:
                span['excepInfo'] = row['SQL_TEXT']
            
            if row['TIMER_END'] is None:
                # print (row)
                continue
            span['elapsed'] = row['TIMER_END'] - row['TIMER_START']   
            span_dict[spanId] = span

    # the second pass
    trace_dict = dict()
    not_visited_keys_set = set(span_dict.keys())

    while len(not_visited_keys_set) > 0:
        # current_key = not_visited_keys_set[0]   
        current_key = not_visited_keys_set.pop()
        visited_keys = []
        while True: # find the trace id                  
            # it is possible that the current key is not in the history
            if (current_key is None) or (current_key not in span_dict):
                trace_id = visited_keys[-1]
                break          

            visited_keys.append(current_key)   
            if 'traceId' in span_dict[current_key]:
                trace_id = span_dict[current_key]['traceId']
                break                

            # TODO: if some span waits for some lock to release, add the lock holder to this trace

            parentSpanId = span_dict[current_key]['parentSpanId']  
            current_key = parentSpanId                  
        
        # now we know the trace_id
        for key in visited_keys:
            span_dict[key]['traceId'] = trace_id
            # not_visited_keys_set.remove(key)
            if trace_id not in trace_dict:
                trace_dict[trace_id] = []
                
            trace_dict[trace_id].append(span_dict[key])
    return trace_dict           