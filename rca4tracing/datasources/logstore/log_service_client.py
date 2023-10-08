import typing as t

from rca4tracing.common.logger import setup_logger
LOG = setup_logger(__name__)


class LogServiceClient(object):
    def __init__(self, **kwargs) -> None:
        pass

    def build_span_tree(
            self, span_list: t.List[t.Dict]) -> t.Tuple[t.List[str], t.Dict]:
        """[Build the trace tree based on the input span list]

        Args:
            self ([type]): [description]
            t ([type]): [description]

        Returns:
            [type]: [the first item is the root ids of this trace tree. If the list has none or more than one ids,
            it means this trace is incomplete. If one id is contained, you can use the 'children' entry to visit the trace tree.]
            {'VertexId': '10e0df21ae89afb5',
                'children': ['4f5e37a0e89d3b1f'],
                'ServiceName': 'RDS_API',
                'OperationName': 'DescribeProxyTypeSupport',
                'TimeStamp': '1628246205200000',
                'ParentSpanId': '11cd36880123c402',
                'Duration': '46212',
                'ServerIp': '11.195.182.53',
                'ErrorType': '0',
                'ExcepInfo': 'jaeger.version=Java-...2afe8301&$' <- error information
            }
        """
        mapping = {
            'VertexId': 'spanId',
            'ServiceName': 'serviceName',
            'OperationName': 'rpc',
            'TimeStamp': 'timestamp',
            'ParentSpanId': 'parentSpanId',
            'Duration': 'elapsed',
            'ServerIp': 'serverIp',
            'ErrorType': 'errorType',
            'ExcepInfo': 'excepInfo'
        }
        assert span_list is not None
        spans_dict = {}
        root_id = []
        is_pre = False
        for span in span_list:
            span_id = span.get('spanid', span.get('spanId'))
            if spans_dict.get(span_id) is None:
                spans_dict[span_id] = {}
            for key, value in mapping.items():
                spans_dict[span_id][key] = span.get(value,
                                                    span.get(value.lower()))
                # if key == 'Duration':
                #     spans_dict[span_id][key] = int(spans_dict[span_id][key])
                # TODO: process HTTP code
                # spans_dict[span['spanid']] = {
                # 'VertexId': span['spanid'],
                # 'ServiceName': span['servicename'],
                # 'OperationName': span['rpc'],
                # 'Timestamp': span['timestamp'],
                # 'ParentSpanId': span['parentspanid'],
                # 'Duration': int(span['elapsed']),
                # 'HostIp': span['serverip'],
                # 'ExcepInfo': span.get('excepInfo', '')
                # }

            # parent_span_id = span.get('parentspanid', span.get('parentSpanId'))
            # parent_span = spans_dict.get(parent_span_id)
            # if parent_span is None:
            #     spans_dict[parent_span_id] = {}
            #     spans_dict[parent_span_id]['VertexId'] = parent_span_id
            # spans_dict[parent_span_id]['children'] = spans_dict[
            #     parent_span_id].get('children', [])
            # spans_dict[parent_span_id]['children'].append(span_id)
            parent_span_ids = span.get('parentspanid', span.get('parentSpanId'))
            # updated on 2022-3-14: the parent_span_id can be a list
            if type(parent_span_ids) != list:
                parent_span_ids = [parent_span_ids]

            # print (parent_span_ids)
            for parent_span_id in parent_span_ids:
                parent_span = spans_dict.get(parent_span_id)
                if parent_span is None:
                    spans_dict[parent_span_id] = {}
                    spans_dict[parent_span_id]['VertexId'] = parent_span_id
                spans_dict[parent_span_id]['children'] = spans_dict[
                    parent_span_id].get('children', [])
                spans_dict[parent_span_id]['children'].append(span_id)

        for i in spans_dict.keys():
            if spans_dict[i].get('ServiceName') is None:
                root_id.extend(spans_dict[i].get('children'))

        # printed_graph = dict()
        # for span_id in spans_dict:
        #     printed_graph[span_id] = {'children': spans_dict[span_id]['children'] if 'children' in spans_dict[span_id] else []}
        # print (root_id, printed_graph)

        return root_id, spans_dict