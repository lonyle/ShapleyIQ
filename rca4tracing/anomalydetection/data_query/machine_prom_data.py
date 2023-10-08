from rca4tracing.common.logger import setup_logger
LOG = setup_logger(__name__, module_name='anomalydetection')


metric_type_dict = {
    'node_load5': 'load', 
    'node_cpu_utilization': 'cpu',
    'memory_utilization': 'memory', 
    'home_partition_utilization': 'disk', 
    'root_partition_utilization': 'disk', 
    'node_sockstat_TCP_tw': 'tcp', 
    'max_node_network_receive_bytes_total': 'network', 
    'max_node_network_transmit_bytes_total': 'network'
}


def extract_value_vec(result, metric_type):
    ''' the result consists of many value vectors 
        the output is a dictionary of value vectors, coressponding to columns in a df
        one key is 'timestamp'
    '''
    value_dict = dict()

    # print ('metric_type:', metric_type)
    # print (result)

    for metric_name in result:     
        if result[metric_name] is None:
            LOG.error ('result[{}] is None'.format(metric_name))
            continue
        for entry in result[metric_name]:            
            # if metric_type == 'network':
            #     id_name = entry['metric']['device']
            # elif metric_type in ['tcp']:
            #     id_name = metric_name
            #if metric_type in ['cpu', 'memory', 'disk', 'tcp', 'load', 'network']:
            id_name = metric_name[1]  # metric_name[0] +'_'+ metric_name[1]

            if len(metric_name) > 2:
                # if metric_name[2] == 'nonheap':
                #     continue
                id_name += '_' + metric_name[2]

            ts_vec = [value[0] for value in entry['values']]
            value_vec = [float(value[1]) for value in entry['values']]

            value_dict[metric_type + '.' + 'timestamp'] = ts_vec
            value_name = id_name  # metric_type + '.' + id_name

            # added on 2021-11-17: for thread state, record for detailed states
            if metric_name[1] in ['jvm_threads_states_threads', 'jvm_threads_state']:
                # print ('here')
                state_alias = {
                    'WAITING': 'waiting',
                    'TERMINATED': 'terminated',
                    'BLOCKED': 'block',
                    'NEW': 'new',
                    'RUNNABLE': 'runnable',
                    'TIMED_WAITING': 'timed-waiting'
                }
                state = entry['metric']['state']
                if state in state_alias:
                    state = state_alias[state]
                value_name = 'jvm_threads_state_' + state

            value_dict[value_name] = value_vec

    return value_dict


def get_metrics(mmc, instance_list, metric_type, **kwargs):
    # added on 2021-11-15: conversion from detailed type to high-level type
    if metric_type in metric_type_dict:
        metric_type = metric_type_dict[metric_type]

    if metric_type == 'network':
        # for network, different metric corresponds to different devices
        result = mmc.get_max_node_network_receive_bytes_total(
            instance_list, **kwargs)
        result1 = mmc.get_max_node_network_transmit_bytes_total(
            instance_list, **kwargs)
        result.update(result1)
    elif metric_type == 'memory':
        # result = mmc.get_node_memory_metrics(instance_list, **kwargs)
        result = mmc.get_memory_utilization(instance_list, **kwargs)
    elif metric_type == 'cpu':
        #result = mmc.get_cpu_metrics(instance_list, **kwargs)
        result = mmc.get_node_cpu_utilization(instance_list, **kwargs)
    elif metric_type == 'disk':
        #result = mmc.get_node_disk_metrics(instance_list, **kwargs)
        result = mmc.get_root_partition_utilization(instance_list, **kwargs)
        result1 = mmc.get_home_partition_utilization(instance_list, **kwargs)
        result.update(result1)
    elif metric_type == 'load':
        result = mmc.get_node_load_metrics(instance_list, **kwargs)
        result_cpu_cores = mmc.get_num_cpu_cores(instance_list, **kwargs)
        num_cpu_cores = 64
        try:
            for key in result_cpu_cores:
                num_cpu_cores = int(result_cpu_cores[key][0]['values'][0][1])
                break
        except Exception as e:
            LOG.error('Exception in get num_cpu_cores: ' + str(e))

        #print (num_cpu_cores)
        for metric_name in result:
            for entry in result[metric_name]:
                for idx in range(len(entry['values'])):
                    entry['values'][idx][1] = float(
                        entry['values'][idx][1]) / num_cpu_cores
        # print (result)
    elif metric_type == 'tcp':
        #result = mmc.get_node_tcp_metrics(instance_list, **kwargs)
        result = mmc.get_node_time_wait(instance_list, **kwargs)
    else:
        LOG.error('unsupported metric_type: {}'.format(metric_type))
    return result