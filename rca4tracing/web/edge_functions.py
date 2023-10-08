#!/usr/bin/env python
# -*- encoding: utf-8 -*-
'''
@File    :   edge_functions.py
@Time    :   2021/05/12 17:00:05
@Author  :   yipei 
'''

# here put the import lib
import time

# from redis.client import timestamp_to_datetime

from rca4tracing.graph.driver.elements import Vertex, Edge
# from rca4tracing.rca.shapley_value_rca import ShapleyValueRCA
# from rca4tracing.datasources.logstore.log_service_client import LogServiceClient

from rca4tracing.common.logger import setup_logger
LOG = setup_logger(__name__, module_name='web')

# from rca4tracing.common.config_parser import WebConfigParser
# web_cfg = WebConfigParser(config_filename='web_config.yaml').config



def links_dedup(links):
    links_ = []
    link_types = []
    for link in links:
        if (link.source_id, link.target_id) not in link_types:
            link_types.append((link.source_id, link.target_id))
            links_.append(link)
    return links_

def classify_links_by_label(links):
    provide_links = []
    operation_links = []
    host_links = []
    for link in links:
        if link.label == ':Provide':
            provide_links.append(link)
        elif link.label == ':Host':
            host_links.append(link)
        else:
            operation_links.append(link)
    return provide_links, operation_links, host_links

def get_edges_nodes_id(edges_list):

    all_node_id_list = []
    for edge in edges_list:
        all_node_id_list.append(edge.source_id)
        all_node_id_list.append(edge.target_id)
    all_node_id_list = list(set(all_node_id_list))

    return all_node_id_list

def get_edges_by_spans_dict(spans_dict, path_id):
    edges = set()
    # print('len(spans_dict)' ,len(spans_dict))
    for key, value in spans_dict.items():
        # print(key, value, '\n')
        if 'children' in value.keys() and 'ServiceName' in value.keys():
            source_id = value['ServiceName'] + ':' + value['OperationName']
            for child in value['children']:
                target_id = spans_dict[child]['ServiceName'] + ':' + spans_dict[
                    child]['OperationName']
                edge = Edge(source_id, target_id, path_id)
                edges.add(edge)

    return edges



if __name__ == '__main__':
    import yaml
    from os.path import dirname, abspath, join
    ROOT_DIR = dirname(dirname(dirname(abspath(__file__))))
    CONFIG_FILE = join(ROOT_DIR, 'conf/web_config.yaml')
    with open(CONFIG_FILE) as f:
        web_cfg = yaml.load(f, Loader=yaml.FullLoader)
