''' using Shapley influence quantification to locate root causes
'''
import time

from rca4tracing.datasources.logstore.log_service_client import LogServiceClient
# from rca4tracing.api.log_api import get_trace_detail_by_id
# from rca4tracing.datasources.redis.driver import RedisDriver
from rca4tracing.rca.utils import contribution_to_prob

from rca4tracing.common.logger import setup_logger
LOG = setup_logger(__name__, module_name='rca')


class ShapleyValueRCA:
    ''' a timeline is a three-tuple, (start_time, end_time, node_id [, diff])
        diff = (end_time-start_time) - normal_duration
    '''
    def __init__(self, using_cache=False):
        self.sync_overlap_threshold = 0.05
        self.normal_duration_dict = dict()
        self.using_cache = using_cache
        # if self.using_cache:
        #     self.redis_driver = RedisDriver()   

    def analyze_traces(self, traces, 
                       strategy='avg_by_contribution', # or 'avg_by_prob'
                       with_anomaly_nodes=False,
                       sort_result=False):
        ''' the input is a list of traces, the output is the root causes
            aggregate the root causes in the traces
        '''
        impacted_nodes = dict()
        aggregated_contribution = dict()
        for trace in traces:
            real_trace = trace['trace'] if with_anomaly_nodes else trace
            contribution_dict = self.analyze_trace(real_trace)
            if with_anomaly_nodes:
                for root_cause_node in contribution_dict:
                    if root_cause_node not in impacted_nodes:
                        impacted_nodes[root_cause_node] = set()
                    impacted_nodes[root_cause_node].update( trace['anomaly_nodes'] )
            if strategy == 'avg_by_prob':
                contribution_dict = contribution_to_prob(contribution_dict)
                
            for key in contribution_dict:
                if key not in aggregated_contribution:
                    aggregated_contribution[key] = 0
                aggregated_contribution[key] += contribution_dict[key]

        k = len(traces)
        for key in aggregated_contribution:
            aggregated_contribution[key] /= k 

        if sort_result:
            aggregated_contribution = dict(sorted(aggregated_contribution.items(), key=lambda x: x[1], reverse=True))
        
        if with_anomaly_nodes:
            return aggregated_contribution, impacted_nodes
        return aggregated_contribution
        
    def analyze_by_trace_id(self, trace_id):
        trace = get_trace_detail_by_id(trace_id,
                                   int((time.time() - 3600 * 24 * 15) * 1000),
                                   int(time.time() * 1000))
        # print (trace)
        return self.analyze_trace(trace)

    def get_normal_duration(self, node_id):
        # we store the normal duraiton in redis
        if not self.using_cache:
            return None
        # if we have cached normal duration, use it
        LOG.debug ('node_id: {}'.format(node_id))
        if node_id in self.normal_duration_dict:
            return self.normal_duration_dict[node_id]

        key = "__mean_std_Duration__::" + node_id # in micro-seconds
        redis_value = self.redis_driver.r.hget(key, 'mean')
        normal_duration = None if redis_value is None else float(redis_value)
        # print (node_id, normal_duration)
        return normal_duration

    def analyze_trace(self, trace):
        ''' analyze one trace
        '''
        # start_time = time.time()

        timeline_dict = self.trace_to_timelines(trace)
        LOG.debug('sorted timeline_dict: {}'.format(sorted(timeline_dict.items(), key=lambda x:x[1])))
        calling_tree = self.trace_to_tree(trace)
        LOG.debug (calling_tree)

        # print (f"running_time at 0: {time.time()-start_time}")

        # 1. first step: split the timelines of the callers'
        all_timeline_segments = []
        for caller in calling_tree:
            callees = calling_tree[caller]
            callee_timelines = [] # one operation can be called for multiple times, so we need to rename the operations by adding the squence number
            for callee in callees:
                callee_timelines.append(timeline_dict[callee])
            # if a timeline cannot be splited, return itself
            
            timeline_segments = self.split_timelines(caller, timeline_dict[caller], callee_timelines)
            all_timeline_segments += timeline_segments
        
        # print (f"running_time at 1: {time.time()-start_time}")

        # add the callees that does not occur in the callers
        for key in timeline_dict:
            if key not in calling_tree: # if not caller
                all_timeline_segments += [timeline_dict[key]]
        LOG.debug('all_timeline_segments after splitting: {}'.format(sorted(all_timeline_segments, key=lambda x:x[3], reverse=True)))
        
        # print (f"running_time at 2: {time.time()-start_time}")

        # 2. second step: merge the sync timelines
        merged_timelines = self.merge_timelines(all_timeline_segments)
        LOG.debug('merged_timelines: {}'.format(merged_timelines))

        # print (f"running_time at 3: {time.time()-start_time}")

        # 3. third step: calculate the shapley value for the merged timelines
        contribution_dict = self.shapley_value_for_timelines(merged_timelines)
        LOG.debug('contribution_dict: {}'.format(contribution_dict))

        # print (f"running_time at 4: {time.time()-start_time}")

        # 4. fourth step: consider the merged or splited nodes
        adjusted_contribution_dict = self.distribute_contribution_to_nodes(contribution_dict)

        # print (f"running_time at 5: {time.time()-start_time}")

        # 5. post filter those < 0
        for key in adjusted_contribution_dict:
            if adjusted_contribution_dict[key] < 0:
                adjusted_contribution_dict[key] = 0
        return adjusted_contribution_dict

    def rename_spans_by_count(self, trace):
        ''' rename the service name in spans
        '''
        operation_count = dict()

        # filter out specific nodes, for debugging
        # new_trace = []
        # for span in trace:
        #     node_id = span['serviceName'] + ':' + span['rpc']
        #     if not node_id.startswith('COMMON_ACTIVITY_PROVIDER:Request /api/v1.0'):
        #         new_trace.append(span.copy())

        new_trace = [item.copy() for item in trace]
        for span in new_trace:
            node_id = span['serviceName'] + ':' + span['rpc']
            if node_id not in operation_count:
                operation_count[node_id] = 0
            operation_count[node_id] += 1
            span['rpc'] = span['rpc'] + ',' + str(operation_count[node_id])
        return new_trace

    def trace_to_tree(self, old_trace):
        ''' Output: calling_tree, which is a dictionary where the keys are the callers and the values are lists of callees
            we refer to process_traces_v2 in datasources/logstore/driver.py on how to process the traces
        '''
        # a trace is a list of spans
        # an operation may occur for many times. we use number like operation_name.1 to distinguish
        calling_tree = dict()
        trace = self.rename_spans_by_count(old_trace)
        root_id_list, spans_dict = LogServiceClient().build_span_tree(trace)
        for key, value in spans_dict.items():
            # print (key, value)
            if 'ServiceName' not in value:
                continue
            if 'children' in value.keys() and 'ServiceName' in value.keys():
                source_id = value['ServiceName'] + ':' + value['OperationName']
                if source_id not in calling_tree:
                    calling_tree[source_id] = []
                for child in value['children']:
                    target_id = spans_dict[child]['ServiceName'] + ':' \
                                    + spans_dict[child]['OperationName']
                    calling_tree[source_id].append(target_id)
        return calling_tree

    def trace_to_timelines(self, old_trace):
        ''' return a dictionary of tuples, where each key is the full operation name, and each tuple has (start_timestamp, end_time, duration)
        '''
        timeline_dict = dict()
        trace = self.rename_spans_by_count(old_trace)
        root_id_list, spans_dict = LogServiceClient().build_span_tree(trace)
        for key, value in spans_dict.items():
            # print (key, value)
            if 'ServiceName' not in value:
                continue
            node_id = value['ServiceName'] + ':' + value['OperationName']            
            duration = int(value['Duration'])
            start_time = int(value['TimeStamp'])
            end_time = start_time + duration
            normal_duration = self.get_normal_duration(node_id.split(',')[0])
            if normal_duration is None:
                normal_duration = 0
                # normal_duration = end_time-start_time
            diff_duration = end_time-start_time-normal_duration
            timeline_dict[node_id] = (start_time, end_time, node_id, diff_duration)

        self.timeline_dict = timeline_dict
        return timeline_dict

    def split_timelines(self, caller_id, caller_timeline, callee_timelines): 
        ''' if one operation calls other operations, then we will split the original operation into the parts of itself and the parts of its callee
            Input: the caller's timeline and callee's timeline list
            Output: the new splited timelines of the caller, which is a list of timelines

            Note: we assume that when calling the children, the caller do nothing else, so that we can split these parts 
        '''
        cnt = 1

        orig_duration = caller_timeline[1] - caller_timeline[0]

        # sort the callee_timelines by starting time
        sorted_timelines = sorted(callee_timelines, key=lambda x:x[0]) # increasing order
        timeline_segments = [] # discontinuous segments 

        current_end_time = caller_timeline[0] # the end time of the spanning callee
        
        for timeline in sorted_timelines: # go through all the callee timelines
            # print (timeline)
            start_time = timeline[0]
            end_time = timeline[1]
            if start_time > current_end_time: # cannot continue spanning the timeline   
                # cut off the left part of the timeline
                splitted_node_id = caller_id+','+str(cnt)
                timeline = [current_end_time, start_time, splitted_node_id]
                self.timeline_dict[splitted_node_id] = timeline
                timeline_segments.append( timeline )
                cnt += 1
            if end_time > current_end_time:
                current_end_time = end_time

        # append the last timeline segment, modified on 2022-3-31
        if caller_timeline[1] - current_end_time > 0:
            splitted_node_id = caller_id+','+str(cnt)
            timeline = [current_end_time, caller_timeline[1], splitted_node_id]
            timeline_segments.append( timeline )
            self.timeline_dict[splitted_node_id] = timeline

        # spread the caller_diff to all its segments
        total_time = 0
        # print (timeline[1]-timeline[0])
        for timeline in timeline_segments:
            total_time += timeline[1] - timeline[0]

        # we calculate the remaining duration increment of the caller, which is proportional
        caller_diff = caller_timeline[3] * (total_time / orig_duration)
        # for callee_timeline in callee_timelines:
        #     caller_diff -= callee_timeline[3] # bug fix on 2020-10-26: if caller have multiple synced callees, cannot minus all
        caller_diff = min(orig_duration, caller_diff) # added on 2021-10-21: the difference cannot > the duration of the splitted timeline  

        
        for timeline in timeline_segments:
            if total_time != 0: # added on 2022-3-14: corner case
                diff = caller_diff * (timeline[1]-timeline[0])/total_time # proportional
                diff = min(timeline[1] - timeline[0], diff) # added on 2021-10-21: the difference cannot > the duration of the splitted timeline
            else:
                diff = 0
            timeline.append( diff )

        timeline_segments = [tuple(timeline) for timeline in timeline_segments]

        return timeline_segments

    def merge_timelines(self, timelines):
        ''' if multiple timelines are sync calls, merge them as one
            Input: all possible timelines to be merged
            Output: the merged big timelines, where a big timeline contains 
                the (start_time, end_time, big_timeline_id). the big timeline id is
                concatination of small timeline ids seperated by space
        '''
        def sync_timelines(timeline1, timeline2):
            # check whether timeline2 is right after timeline1
            if (timeline2[0] - timeline1[1]) < (timeline1[1] - timeline1[0])*self.sync_overlap_threshold:
                return True
            else:
                return False

        sorted_timelines = sorted(timelines, key=lambda x:x[0]) # sort by start time
        # the start_time of the following timeline should be larger than the precceding timeline
        merged_timelines = []
        remaining_timelines = sorted_timelines.copy()#[item.copy() for item in sorted_timelines]
        while len(remaining_timelines) > 0:
            # in each iteration, we find one more timeline that can be merged       
            last_j = 0
            timelines_idx_recycle = [last_j]
            big_timeline_ids = [] #[remaining_timelines[last_j][2]] # changed on 2021-10-25
            big_timeline_start = remaining_timelines[last_j][0]
            while True:
                # print (last_j)                
                synced_timelines_idx = []
                max_end_time = remaining_timelines[last_j][1]
                max_end_time_idx = last_j       

                # find all timelines that start at the end time of the last timestamps
                for j in range(last_j+1, len(remaining_timelines)):               
                    if sync_timelines(remaining_timelines[last_j], remaining_timelines[j]):
                        if remaining_timelines[j][1] > max_end_time:
                            max_end_time = remaining_timelines[j][1]
                            max_end_time_idx = j
                        synced_timelines_idx.append(j)

                big_timeline_end = remaining_timelines[max_end_time_idx][1]                        
                
                LOG.debug ("{} {}".format(remaining_timelines[last_j][2], remaining_timelines[last_j][3]))
                big_timeline_ids.append(remaining_timelines[last_j][2]) # we only keep the id with the largest duration

                # print (synced_timelines_idx)             
                if len(synced_timelines_idx) == 0:
                    break # if we do not found_new_synced_timeline, break the loop  
                if last_j == max_end_time_idx:
                    break # if max_end_time_idx does not change, it means we do not find new synced timeline??
                else:
                    last_j = max_end_time_idx                 

                timelines_idx_recycle += synced_timelines_idx
            
            timelines_idx_recycle = list(set(timelines_idx_recycle)) # we can have duplicated indices??
            total_diff = 0
            for idx in timelines_idx_recycle:
                LOG.debug (remaining_timelines[idx])
                total_diff += remaining_timelines[idx][3]
            merged_timelines.append( (big_timeline_start, big_timeline_end, \
                '~~'.join(big_timeline_ids), total_diff) )
            # processing the found sync timelines  
            # print (sorted(timelines_idx_recycle, reverse=True))
            # print (len(remaining_timelines))
            for idx in sorted(timelines_idx_recycle, reverse=True):
                # print (idx, '<', len(remaining_timelines))
                del remaining_timelines[idx]

        return merged_timelines

    def shapley_value_for_timelines(self, timelines):
        # we need not only the start_time and end_time, but also the normal time
        ''' for multiple timelines, we scan from right to left, and get the shapley values
        '''
        time_points = [] # a time_point is like {'time': xx, 'node_id': xx, 'type': xx}
        for timeline in timelines:
            if timeline[3] < 0:
                continue # if the duration diff for a timeline is negative, ignore this timeline
            start_time = timeline[0]
            end_time = timeline[1]
            old_time = timeline[1] - timeline[3]
            node_id = timeline[2]

            time_points.append( {'time': start_time, 'node_id': node_id, 'type': 'start_time'} )
            time_points.append( {'time': end_time, 'node_id': node_id, 'type': 'end_time'} )
            time_points.append( {'time': old_time, 'node_id': node_id, 'type': 'old_time'} )

        sorted_time_points = sorted(time_points, key=lambda x:x['time'], reverse=True)
        contribution_dict = dict()
        processing_node_ids = []
        last_checkout_time = None
        for idx in range(len(sorted_time_points)):
            time_point = sorted_time_points[idx]
            if time_point['type'] in ['end_time', 'old_time']:
                # add contributions for old nodes
                if len(processing_node_ids) > 0:
                    time_diff = last_checkout_time - time_point['time']
                for node_id in processing_node_ids:
                    if node_id not in contribution_dict:
                        contribution_dict[node_id] = 0
                    contribution_dict[node_id] += time_diff / len(processing_node_ids)
                last_checkout_time = time_point['time']

            if time_point['node_id'] not in processing_node_ids:              
                if time_point['type'] == 'end_time':
                    # add this new node
                    processing_node_ids.append(time_point['node_id'])
            else:
                if time_point['type'] == 'old_time':
                    # delete this old node
                    processing_node_ids.remove(time_point['node_id'])                

        return contribution_dict

    def distribute_contribution_to_nodes(self, contribution_dict):
        ''' the keys in contribution_dict correspond to splited or merged nodes
        '''
        adjusted_contribution_dict = dict()
        # print (self.timeline_dict)
        for key in contribution_dict:
            operation_names = key.split('~~')
            # spread the diff to all operation_names
            total_diff = 0
            for operation_name in operation_names:
                # Note: the synced operation can have negative contribution (although the total is positive)
                if operation_name in self.timeline_dict: # check whether in timeline dict
                    total_diff += self.timeline_dict[operation_name][3]
            for operation_name in operation_names:
                if operation_name not in adjusted_contribution_dict:
                    adjusted_contribution_dict[operation_name] = 0
                if total_diff > 0:
                    frac = self.timeline_dict[operation_name][3] / total_diff
                else:
                    frac = 0
                adjusted_contribution_dict[operation_name] += contribution_dict[key] * frac                    

        new_dict = dict()
        for old_key in adjusted_contribution_dict:
            new_key = old_key.split(',')[0]
            if new_key not in new_dict:
                new_dict[new_key] = 0
            new_dict[new_key] += adjusted_contribution_dict[old_key]
        return new_dict

if __name__ == '__main__':
    shapley_rca = ShapleyValueRCA()
    trace_id = '96900c2b156018c860bd65b48e2dcf40' # replace with your input trace id
    result = shapley_rca.analyze_by_trace_id(trace_id)
    result = sorted(result.items(), key=lambda x:x[1], reverse=True)
    LOG.info (result)
                           

                    



