''' get the recent performance schema data and try to form a trace-like graph
'''
from rca4tracing.common.logger import setup_logger
LOG = setup_logger(__name__, module_name='rca')


import pickle
import argparse
import numpy as np
# from rca4tracing.datasources.perf_schema.fault_injector import FaultInjector, root_cause_sql

class PerfSchemaEventNode:
    def __init__(self, node_id, node_type, data):
        self.node_id = node_id
        self.node_type = node_type
        self.parent = None
        self.children = {}
        self.blocking_parent = None # current node is blocked by its blocking parent
        self.blocking_children = {} # current node is blocking all its blocking children
        self.data = data
    
    def set_parent(self, parent):
        self.parent = parent

    def add_child(self, child):
        if child.node_id not in self.children:
            self.children[child.node_id] = child

    def set_blocking_parent(self, blocking_parent):
        self.blocking_parent = blocking_parent

    def add_blocking_child(self, blocking_child):
        if blocking_child.node_id not in self.blocking_children:
            self.blocking_children[blocking_child.node_id] = blocking_child            

class PerfSchemaData:
    def __init__(self):
        self.root_cause = None
        self.root_cause_sql = None
        self.nodes = dict()
        self.nodes['Transaction'] = {}
        self.nodes['Statement'] = {}
        self.nodes['Stage'] = {}
        self.nodes['Wait'] = {}
        #self.waits_summary = None
        #self.stages_summary = None
        #self.statements_summary = None
        #self.objects_summary = None
        #self.file_io_summary = None
        #self.table_io_waits_summary_by_index = None
        #self.table_io_waits_summary_by_table = None
        #self.table_lock_waits_summary = None
        #self.sockets_summary = None

class PerfSchemaDriver:
    def __init__(self, 
                 host='xxx', 
                 port=3306,
                 user='xxx',
                 passwd='xxx',
                 db='performance_schema'):
        self.host = host
        self.port = port
        self.user = user
        self.passwd = passwd
        self.db = db        

        self.data_lock_waits_list = []

    def get_recent_ps_data(self):
        from rca4tracing.graph.rds import get_db, execute_all
        self.database = get_db(host=self.host,
                        port=self.port,
                        user=self.user,
                        passwd=self.passwd,
                        db=self.db,
                        connector='pymysql' # it can be MySQLdb
        )
        # Read summary
        #sql = "SELECT * FROM events_waits_summary_global_by_event_name"
        #print(sql)
        #self.waits_summary = execute_all(self.database, sql, return_column_name=True)
        #sql = "SELECT * FROM events_stages_summary_global_by_event_name"
        #print(sql)
        #self.stages_summary = execute_all(self.database, sql, return_column_name=True)
        #sql = "SELECT * FROM events_statements_summary_global_by_event_name"
        #print(sql)
        #self.statements_summary = execute_all(self.database, sql, return_column_name=True)
        #sql = "SELECT * FROM objects_summary_global_by_type"
        #print(sql)
        #self.objects_summary = execute_all(self.database, sql, return_column_name=True)
        #sql = "SELECT * FROM file_summary_by_event_name"
        #print(sql)
        #self.file_io_summary = execute_all(self.database, sql, return_column_name=True)
        #sql = "SELECT * FROM table_io_waits_summary_by_index_usage"
        #print(sql)
        #self.table_io_waits_summary_by_index = execute_all(self.database, sql, return_column_name=True)
        #sql = "SELECT * FROM table_io_waits_summary_by_table"
        #print(sql)
        #self.table_io_waits_summary_by_table = execute_all(self.database, sql, return_column_name=True)
        #sql = "SELECT * FROM table_lock_waits_summary_by_table"
        #print(sql)
        #self.table_lock_waits_summary = execute_all(self.database, sql, return_column_name=True)
        #sql = "SELECT * FROM socket_summary_by_event_name"
        #print(sql)
        #self.sockets_summary = execute_all(self.database, sql, return_column_name=True)

        # Read current events
        sql = "SELECT * FROM events_transactions_current"
        print(sql)
        self.transactions_list = execute_all(self.database, sql, return_column_name=True)
        sql = "SELECT * FROM events_statements_current WHERE CURRENT_SCHEMA NOT IN ('mysql', 'performance_schema', 'information_schema')"
        print(sql)
        self.statements_list = execute_all(self.database, sql, return_column_name=True)  
        sql = "SELECT * FROM events_stages_current"
        print(sql)
        self.stages_list = execute_all(self.database, sql, return_column_name=True)
        sql = "SELECT * FROM events_waits_current"
        print(sql)
        self.waits_list = execute_all(self.database, sql, return_column_name=True)        

        # Read history events
        sql = "SELECT * FROM events_statements_history WHERE CURRENT_SCHEMA NOT IN ('mysql', 'performance_schema', 'information_schema')"
        print(sql)
        self.statements_list.extend(execute_all(self.database, sql, return_column_name=True))  
        sql = "SELECT * FROM events_stages_history"
        print(sql)
        self.stages_list.extend(execute_all(self.database, sql, return_column_name=True))      
        sql = "SELECT * FROM events_waits_history"
        print(sql)
        self.waits_list.extend(execute_all(self.database, sql, return_column_name=True))            

        # Read data_lock_waits
        sql = "SELECT * FROM data_lock_waits"
        print(sql)
        self.data_lock_waits_list = execute_all(self.database, sql, return_column_name=True)

        if len(self.data_lock_waits_list) > 0:
            engine_lock_ids = set()
            for data_lock_wait in self.data_lock_waits_list:
                engine_lock_ids.add("'{}'".format(data_lock_wait['REQUESTING_ENGINE_LOCK_ID']))
                engine_lock_ids.add("'{}'".format(data_lock_wait['BLOCKING_ENGINE_LOCK_ID']))
            sql_ = "SELECT THREAD_ID, EVENT_ID, ENGINE_LOCK_ID FROM data_locks WHERE ENGINE_LOCK_ID IN ("
            sql_ += ','.join(list(engine_lock_ids))
            sql_ += ')'
            # Read data_locks
            sql = """SELECT parent.THREAD_ID, parent.EVENT_ID, child.ENGINE_LOCK_ID FROM events_transactions_current AS parent 
                        INNER JOIN ({}) AS child
                        WHERE parent.THREAD_ID = child.THREAD_ID
                            AND parent.EVENT_ID < child.EVENT_ID
                            AND (
                                child.EVENT_ID <= parent.END_EVENT_ID
                                OR parent.END_EVENT_ID IS NULL
                                )""".format(sql_)
            print(sql)
            self.transactions_data_locks_list = execute_all(self.database, sql, return_column_name=True) 
            sql = """SELECT parent.THREAD_ID, parent.EVENT_ID, child.ENGINE_LOCK_ID FROM events_statements_current AS parent 
                        INNER JOIN ({}) AS child
                        WHERE parent.THREAD_ID = child.THREAD_ID
                            AND parent.EVENT_ID < child.EVENT_ID
                            AND (
                                child.EVENT_ID <= parent.END_EVENT_ID
                                OR parent.END_EVENT_ID IS NULL
                                )""".format(sql_)        
            print(sql)
            self.statements_data_locks_list = execute_all(self.database, sql, return_column_name=True)
            sql = """SELECT parent.THREAD_ID, parent.EVENT_ID, child.ENGINE_LOCK_ID FROM events_statements_history AS parent 
                        INNER JOIN ({}) AS child
                        WHERE parent.THREAD_ID = child.THREAD_ID
                            AND parent.EVENT_ID < child.EVENT_ID
                            AND (
                                child.EVENT_ID <= parent.END_EVENT_ID
                                OR parent.END_EVENT_ID IS NULL
                                )""".format(sql_)               
            print(sql)
            self.statements_data_locks_list.extend(execute_all(self.database, sql, return_column_name=True))
            sql = """SELECT parent.THREAD_ID, parent.EVENT_ID, child.ENGINE_LOCK_ID FROM events_stages_current AS parent 
                        INNER JOIN ({}) AS child
                        WHERE parent.THREAD_ID = child.THREAD_ID
                            AND parent.EVENT_ID < child.EVENT_ID
                            AND (
                                child.EVENT_ID <= parent.END_EVENT_ID
                                OR parent.END_EVENT_ID IS NULL
                                )""".format(sql_)           
            print(sql)  
            self.stages_data_locks_list = execute_all(self.database, sql, return_column_name=True)
            sql = """SELECT parent.THREAD_ID, parent.EVENT_ID, child.ENGINE_LOCK_ID FROM events_stages_history AS parent 
                        INNER JOIN ({}) AS child
                        WHERE parent.THREAD_ID = child.THREAD_ID
                            AND parent.EVENT_ID < child.EVENT_ID
                            AND (
                                child.EVENT_ID <= parent.END_EVENT_ID
                                OR parent.END_EVENT_ID IS NULL
                                )""".format(sql_)           
            print(sql) 
            self.stages_data_locks_list.extend(execute_all(self.database, sql, return_column_name=True))
            sql = """SELECT parent.THREAD_ID, parent.EVENT_ID, child.ENGINE_LOCK_ID FROM events_waits_current AS parent 
                        INNER JOIN ({}) AS child
                        WHERE parent.THREAD_ID = child.THREAD_ID
                            AND parent.EVENT_ID < child.EVENT_ID
                            AND (
                                child.EVENT_ID <= parent.END_EVENT_ID
                                OR parent.END_EVENT_ID IS NULL
                                )""".format(sql_)             
            print(sql)
            self.waits_data_locks_list = execute_all(self.database, sql, return_column_name=True)
            sql = """SELECT parent.THREAD_ID, parent.EVENT_ID, child.ENGINE_LOCK_ID FROM events_waits_history AS parent 
                        INNER JOIN ({}) AS child
                        WHERE parent.THREAD_ID = child.THREAD_ID
                            AND parent.EVENT_ID < child.EVENT_ID
                            AND (
                                child.EVENT_ID <= parent.END_EVENT_ID
                                OR parent.END_EVENT_ID IS NULL
                                )""".format(sql_)           
            print(sql) 
            self.waits_data_locks_list.extend(execute_all(self.database, sql, return_column_name=True))     

        # # Read history long events
        # sql = "SELECT * FROM events_statements_history_long WHERE CURRENT_SCHEMA NOT IN ('mysql', 'performance_schema', 'information_schema')"
        # self.statements_list.extend(execute_all(self.database, sql, return_column_name=True))  
        sql = "SELECT * FROM events_stages_history_long"
        self.stages_list.extend(execute_all(self.database, sql, return_column_name=True))      
        sql = "SELECT * FROM events_waits_history_long"
        self.waits_list.extend(execute_all(self.database, sql, return_column_name=True))            

        # Prepare perf schema data
        self.perf_schema_data = PerfSchemaData()
        #self.perf_schema_data.waits_summary = self.waits_summary
        #self.perf_schema_data.stages_summary = self.stages_summary
        #self.perf_schema_data.statements_summary = self.statements_summary
        #self.perf_schema_data.objects_summary = self.objects_summary
        #self.perf_schema_data.file_io_summary = self.file_io_summary
        #self.perf_schema_data.table_io_waits_summary_by_index = self.table_io_waits_summary_by_index
        #self.perf_schema_data.table_io_waits_summary_by_table = self.table_io_waits_summary_by_table
        #self.perf_schema_data.table_lock_waits_summary = self.table_lock_waits_summary
        #self.perf_schema_data.sockets_summary = self.sockets_summary
        for transaction in self.transactions_list:
            node_id = 'Transaction:ThreadID:{}-EventID:{}'.format(transaction['THREAD_ID'], transaction['EVENT_ID'])
            if node_id not in self.perf_schema_data.nodes['Transaction']:
                node = PerfSchemaEventNode(node_id, 'Transaction', transaction)
                self.perf_schema_data.nodes['Transaction'][node_id] = node
        print(len(self.transactions_list), len(self.perf_schema_data.nodes['Transaction']))
        rt = []
        sqls = set()
        for statement in self.statements_list:
            node_id = 'Statement:ThreadID:{}-EventID:{}'.format(statement['THREAD_ID'], statement['EVENT_ID'])
            if node_id not in self.perf_schema_data.nodes['Statement']:
                node = PerfSchemaEventNode(node_id, 'Statement', statement)
                rt.append(int(node.data['TIMER_WAIT']))
                self.perf_schema_data.nodes['Statement'][node_id] = node
                if statement['NESTING_EVENT_TYPE'] == 'TRANSACTION':
                    parent_node_id = 'Transaction:ThreadID:{}-EventID:{}'.format(statement['THREAD_ID'], statement['NESTING_EVENT_ID'])
                    if parent_node_id in self.perf_schema_data.nodes['Transaction']:
                        sqls.add('{}.{}'.format(node.data['THREAD_ID'],node.data['DIGEST_TEXT']))
                        parent_node = self.perf_schema_data.nodes['Transaction'][parent_node_id]
                        parent_node.add_child(node)
                        node.set_parent(parent_node)
        for sql in sqls:
            print(sql)
        print(len(self.statements_list), len(self.perf_schema_data.nodes['Statement']), np.mean(rt))
        for stage in self.stages_list:
            node_id = 'Stage:ThreadID:{}-EventID:{}'.format(stage['THREAD_ID'], stage['EVENT_ID'])
            if node_id not in self.perf_schema_data.nodes['Stage']:
                node = PerfSchemaEventNode(node_id, 'Stage', stage)
                self.perf_schema_data.nodes['Stage'][node_id] = node
                if stage['NESTING_EVENT_TYPE'] == 'STATEMENT':
                    parent_node_id = 'Statement:ThreadID:{}-EventID:{}'.format(stage['THREAD_ID'], stage['NESTING_EVENT_ID'])
                    if parent_node_id in self.perf_schema_data.nodes['Statement']:
                        parent_node = self.perf_schema_data.nodes['Statement'][parent_node_id]
                        parent_node.add_child(node)
                        node.set_parent(parent_node)          
        print(len(self.stages_list), len(self.perf_schema_data.nodes['Stage']))                        
        for wait in self.waits_list:
            node_id = 'Wait:ThreadID:{}-EventID:{}'.format(wait['THREAD_ID'], wait['EVENT_ID'])
            if node_id not in self.perf_schema_data.nodes['Wait']:
                node = PerfSchemaEventNode(node_id, 'Wait', wait)
                self.perf_schema_data.nodes['Wait'][node_id] = node
                if wait['NESTING_EVENT_TYPE'] == 'STAGE':
                    parent_node_id = 'Stage:ThreadID:{}-EventID:{}'.format(wait['THREAD_ID'], wait['NESTING_EVENT_ID'])
                    if parent_node_id in self.perf_schema_data.nodes['Stage']:
                        parent_node = self.perf_schema_data.nodes['Stage'][parent_node_id]
                        parent_node.add_child(node)
                        node.set_parent(parent_node)
        print(len(self.waits_list), len(self.perf_schema_data.nodes['Wait']))

        # Link blocking events
        for data_lock_wait in self.data_lock_waits_list:
            request_engine_lock_id = data_lock_wait['REQUESTING_ENGINE_LOCK_ID']
            block_engine_lock_id = data_lock_wait['BLOCKING_ENGINE_LOCK_ID']
            request_transaction = None
            block_transaction = None
            for transaction in self.transactions_data_locks_list:
                if transaction['ENGINE_LOCK_ID'] == request_engine_lock_id:
                    request_transaction = transaction
                if transaction['ENGINE_LOCK_ID'] == block_engine_lock_id:
                    block_transaction = transaction
            if request_transaction is not None and block_transaction is not None:
                request_transaction_node_id = 'Transaction:ThreadID:{}-EventID:{}'.format(request_transaction['THREAD_ID'], request_transaction['EVENT_ID'])
                block_transaction_node_id = 'Transaction:ThreadID:{}-EventID:{}'.format(block_transaction['THREAD_ID'], block_transaction['EVENT_ID'])
                if request_transaction_node_id in self.perf_schema_data.nodes['Transaction'] and block_transaction_node_id in self.perf_schema_data.nodes['Transaction']:
                    request_transaction_node = self.perf_schema_data.nodes['Transaction'][request_transaction_node_id]
                    block_transaction_node = self.perf_schema_data.nodes['Transaction'][block_transaction_node_id]
                    request_transaction_node.set_blocking_parent(block_transaction_node)
                    block_transaction_node.add_blocking_child(request_transaction_node)
            request_statement = None
            block_statement = None
            for statement in self.statements_data_locks_list:
                if statement['ENGINE_LOCK_ID'] == request_engine_lock_id:
                    request_statement = statement
                if statement['ENGINE_LOCK_ID'] == block_engine_lock_id:
                    block_statement = statement
            if request_statement is not None and block_statement is not None:
                request_statement_node_id = 'Statement:ThreadID:{}-EventID:{}'.format(request_statement['THREAD_ID'], request_statement['EVENT_ID'])
                block_statement_node_id = 'Statement:ThreadID:{}-EventID:{}'.format(block_statement['THREAD_ID'], block_statement['EVENT_ID'])
                if request_statement_node_id in self.perf_schema_data.nodes['Statement'] and block_statement_node_id in self.perf_schema_data.nodes['Statement']:
                    request_statement_node = self.perf_schema_data.nodes['Statement'][request_statement_node_id]
                    block_statement_node = self.perf_schema_data.nodes['Statement'][block_statement_node_id]
                    request_statement_node.set_blocking_parent(block_statement_node)
                    block_statement_node.add_blocking_child(request_statement_node)    
            request_stage = None
            block_stage = None
            for stage in self.stages_data_locks_list:
                if stage['ENGINE_LOCK_ID'] == request_engine_lock_id:
                    request_stage = stage
                if stage['ENGINE_LOCK_ID'] == block_engine_lock_id:
                    block_stage = stage
            if request_stage is not None and block_stage is not None:
                request_stage_node_id = 'Stage:ThreadID:{}-EventID:{}'.format(request_stage['THREAD_ID'], request_stage['EVENT_ID'])
                block_stage_node_id = 'Stage:ThreadID:{}-EventID:{}'.format(block_stage['THREAD_ID'], block_stage['EVENT_ID'])
                if request_stage_node_id in self.perf_schema_data.nodes['Stage'] and block_stage_node_id in self.perf_schema_data.nodes['Stage']:
                    request_stage_node = self.perf_schema_data.nodes['Stage'][request_stage_node_id]
                    block_stage_node = self.perf_schema_data.nodes['Stage'][block_stage_node_id]
                    request_stage_node.set_blocking_parent(block_stage_node)
                    block_stage_node.add_blocking_child(request_stage_node)        
            request_wait = None
            block_wait = None
            for wait in self.waits_data_locks_list:
                if wait['ENGINE_LOCK_ID'] == request_engine_lock_id:
                    request_wait = wait
                if wait['ENGINE_LOCK_ID'] == block_engine_lock_id:
                    block_wait = wait
            if request_wait is not None and block_wait is not None:
                request_wait_node_id = 'Wait:ThreadID:{}-EventID:{}'.format(request_wait['THREAD_ID'], request_wait['EVENT_ID'])
                block_wait_node_id = 'Wait:ThreadID:{}-EventID:{}'.format(block_wait['THREAD_ID'], block_wait['EVENT_ID'])
                if request_wait_node_id in self.perf_schema_data.nodes['Wait'] and block_wait_node_id in self.perf_schema_data.nodes['Wait']:
                    request_wait_node = self.perf_schema_data.nodes['Wait'][request_wait_node_id]
                    block_wait_node = self.perf_schema_data.nodes['Wait'][block_wait_node_id]
                    request_wait_node.set_blocking_parent(block_wait_node)
                    block_wait_node.add_blocking_child(request_wait_node)

    def set_perf_schema_data(self, perf_schema_data):
        self.perf_schema_data = perf_schema_data

    def get_deepest_nodes(self, fault_type='lock'):
        if fault_type == 'lock':
            deepest_transaction_statements = set()
            for transaction_node_id in self.perf_schema_data.nodes['Transaction']:
                transaction_node = self.perf_schema_data.nodes['Transaction'][transaction_node_id]
                if transaction_node.blocking_parent is None and len(transaction_node.blocking_children) > 0:
                    for child_id in transaction_node.children:
                        statement = transaction_node.children[child_id]
                        deepest_transaction_statements.add(statement.data['DIGEST_TEXT'])
                        print(statement.data['DIGEST_TEXT'], len(transaction_node.blocking_children))
            #deepest_statements = set()
            #for statement_node_id in self.perf_schema_data.nodes['Statement']:
            #    statement_node = self.perf_schema_data.nodes['Statement'][statement_node_id]
            #    if statement_node.blocking_parent is None and len(statement_node.blocking_children) > 0:
            #        deepest_statements.add(statement_node.data['DIGEST_TEXT'])
            #for sql in deepest_transaction_statements:
            #    print(sql)
            #print()
            #for sql in deepest_statements:
            #    print(sql)
            return deepest_transaction_statements
            

    def get_analyze_nodes(self, fault_type='lock'):
        if fault_type != 'lock':
            return self.get_analyze_nodes_others()
        else:
            return self.get_analyze_nodes_lock()

    def get_analyze_nodes_lock(self):   
        transaction_node_ids = set()
        statement_node_ids = set()
        
        for transaction_node_id in self.perf_schema_data.nodes['Transaction']:
            transaction_node = self.perf_schema_data.nodes['Transaction'][transaction_node_id]
            if transaction_node.blocking_parent is not None or len(transaction_node.blocking_children) > 0:
                #print(transaction_node_id)
                transaction_node_ids.add(transaction_node_id)
                for child_id in transaction_node.children:
                    statement = transaction_node.children[child_id]
                    #print(statement.data['DIGEST_TEXT'])
                #print()
        for statement_node_id in self.perf_schema_data.nodes['Statement']:
            statement_node = self.perf_schema_data.nodes['Statement'][statement_node_id]
            if statement_node.blocking_parent is not None or len(statement_node.blocking_children) > 0:
                #print(statement_node_id)
                statement_node_ids.add(statement_node_id)
                #print(statement_node.data['DIGEST_TEXT'])
                #print()        
                    
        #for wait_node_id in self.perf_schema_data.nodes['Wait']:
        #    wait_node = self.perf_schema_data.nodes['Wait'][wait_node_id]
        #    parent_stage_node = wait_node.parent
        #    if parent_stage_node is not None:
        #        parent_statement_node = parent_stage_node.parent
        #        if parent_statement_node is not None:
        #            statement_node_ids.add(parent_statement_node.node_id)
        #            parent_transaction_node = parent_statement_node.parent
        #            if parent_transaction_node is not None:
        #                transaction_node_ids.add(parent_transaction_node.node_id)
        #            print(parent_statement_node.data['DIGEST_TEXT'])
        #            print(wait_node.data)
        #            print()

        # print (transaction_node_ids, statement_node_ids)
        return transaction_node_ids, statement_node_ids
    
    def get_analyze_nodes_others(self):
        ''' if the injection is not on lock, we find all the transactions and statements
        '''
        transaction_node_ids = set(self.perf_schema_data.nodes['Transaction'].keys())
        statement_node_ids = set()
        for statement_node_id in self.perf_schema_data.nodes['Statement']:
            statement_node = self.perf_schema_data.nodes['Statement'][statement_node_id]
            if statement_node.parent is None: # if not included in a transaction
                statement_node_ids.add(statement_node_id)
            elif statement_node.parent.node_id not in transaction_node_ids:
                statement_node_ids.add(statement_node_id)
        return transaction_node_ids, statement_node_ids

    def get_recent_ps_data_(self):
        sql = "SELECT * FROM events_stages_history_long"
        stages_history = execute_all(self.database, sql, return_column_name=True)
        # sql = "SELECT * FROM events_stages_current" # the last one         

        sql = "SELECT * FROM events_statements_history_long"
        statements_history = execute_all(self.database, sql, return_column_name=True)
        # sql = "SELECT * FROM events_statements_current" # the last one         

        sql = "SELECT * FROM events_waits_history_long"
        waits_history = execute_all(self.database, sql, return_column_name=True)
        sql = "SELECT * FROM events_waits_current" # a small number
        waits_current = execute_all(self.database, sql, return_column_name=True)

        sql = "SELECT * FROM events_transactions_history_long"
        transactions_history = execute_all(self.database, sql, return_column_name=True)
        # sql = "SELECT * FROM events_transactions_current"                  

        data = {
            'STAGE': stages_history,
            'STATEMENT': statements_history,
            'WAIT': waits_history,
            'waits_current': waits_current,
            'TRANSACTION': transactions_history
        }
        return data

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--workload', type=str, default='tpcc', help='workload')
    parser.add_argument('--injection', type=str, default='io1', help='injection')
    parser.add_argument('--host', type=str, default='xxx', help='injection')
    parser.add_argument('--port', type=int, default=3306, help='port')
    parser.add_argument('--user', type=str, default='xxx', help='user')
    parser.add_argument('--passwd', type=str, default='xxx', help='password')
    args = parser.parse_args()

    ps_driver = PerfSchemaDriver(host=args.host, port=args.port, user=args.user, passwd=args.passwd)
    fn = 'rca4tracing/rca4tracing/datasources/perf_schema/datasets/{}_{}.pickle'.format(args.workload, args.injection)
    ps_driver.get_recent_ps_data()
    ps_driver.perf_schema_data.root_cause = args.injection
    ps_driver.perf_schema_data.root_cause_sql = root_cause_sql[args.workload][args.injection]
    with open(fn, 'wb') as f:
        pickle.dump(ps_driver.perf_schema_data, f)
    with open(fn, 'rb') as f:
        perf_schema_data = pickle.load(f)    
        ps_driver.set_perf_schema_data(perf_schema_data)
    transaction_node_ids, statement_node_ids = ps_driver.get_analyze_nodes()
    print(transaction_node_ids)
    print(statement_node_ids)
    # trace_dict = ps_driver.ps_data_to_traces(data)
    # print (len(trace_dict))
    
            
