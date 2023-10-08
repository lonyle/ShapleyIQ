import ray
import time
import argparse
import numpy as np
from collections import defaultdict
from rca4tracing.graph.rds import get_db, execute_one, execute_all, execute

root_cause_sql = defaultdict(dict)
root_cause_sql['tpcc']['lock1'] = 'UPDATE warehouse SET w_ytd = 1'
root_cause_sql['tpcc']['lock2'] = 'UPDATE warehouse SET w_ytd = 1 WEHER w_id = ?'
root_cause_sql['tpcc']['lock3'] = 'UPDATE district SET d_ytd = 1 WHERE d_id < 5'
root_cause_sql['tpcc']['lock4'] = 'UPDATE district SET d_ytd = 1 WHERE d_w_id = ?'
root_cause_sql['tpcc']['io1'] = 'SELECT * FROM order_line WHERE ol_amount <= ? ORDER BY ol_amount DESC limit 10000'
root_cause_sql['tpcc']['cpu'] = None
root_cause_sql['tpcc']['network'] = None
root_cause_sql['tpcc']['none'] = None
root_cause_sql['smallbank']['none'] = None
root_cause_sql['smallbank']['io1'] = 'SELECT * FROM savings WHERE bal < 10000'
root_cause_sql['smallbank']['io2'] = 'SELECT * FROM checking WHERE bal > 10000'
root_cause_sql['smallbank']['lock1'] = 'UPDATE savings SET bal = 100'
root_cause_sql['smallbank']['lock2'] = 'UPDATE savings SET bal = 100 WHERE custid < 5000'
root_cause_sql['voter']['none'] = None
root_cause_sql['voter']['lock1'] = "UPDATE votes SET contestant_number = 1 WEHRE created < NOW()"
root_cause_sql['voter']['io1'] = "SELECT * FROM votes WHERE contestant_number = 1"
root_cause_sql['seats']['none'] = None
root_cause_sql['seats']['lock1'] = 'UPDATE reservation SET r_price = 1000'
root_cause_sql['seats']['lock2'] = 'UPDATE reservation SET r_price = 0 WHERE r_price = 100'
root_cause_sql['seats']['lock3'] = 'UPDATE flight SET f_seats_left = 0 WHERE f_seats_left < 10'
root_cause_sql['seats']['lock4'] = 'UPDATE customer SET c_balance = 100 WHERE c_balance = 200'
root_cause_sql['seats']['io1'] = 'SELECT * from reservation WHERE r_price < 1000'
root_cause_sql['tatp']['none'] = None
root_cause_sql['tatp']['lock1'] = "UPDATE subscriber SET vlr_location = 1 WHERE msc_location < 1000"
root_cause_sql['tatp']['lock2'] = "UPDATE special_facility SET data_b = 'a' WHERE data_a < 100"
root_cause_sql['tatp']['io1'] = "SELECT * FROM access_info WHERE data1 = 1"
root_cause_sql['tatp']['io2'] = "SELECT * FROM call_forwarding WHERE numberx = 'a'"

@ray.remote
class FaultInjector:
    def __init__(self,
                 host='xxx', 
                 port=3306,
                 user='xxx',
                 passwd='xxx',
                 db='benchbase',
                 workload='tpcc',
                 injection='none'):
        self.host = host
        self.port = port
        self.user = user
        self.passwd = passwd
        self.db = workload       
        self.workload = workload
        self.injection = injection

    def connect_db(self):
        self.database = get_db(host=self.host,
                        port=self.port,
                        user=self.user,
                        passwd=self.passwd,
                        db=self.db,
                        connector='pymysql' # it can be MySQLdb
        )    

    def close_db(self):
        self.database.close()

    def inject_io(self):
        sql = None
        if self.workload == 'tpcc':
            if self.injection == 'io1':
                rnum = np.random.randint(1,11)
                sql = "SELECT * FROM order_line WHERE ol_amount <= {} ORDER BY ol_amount DESC limit 100000".format(rnum)
        if self.workload == 'smallbank':
            if self.injection == 'io1':
                sql = "SELECT * FROM savings WHERE bal < 10000"
            if self.injection == 'io2':
                sql = 'SELECT * FROM checking WHERE bal > 10000'
        if self.workload == 'voter':
            if self.injection == 'io1':
                sql = 'SELECT * FROM votes WHERE contestant_number = 1'
        if self.workload == 'tatp':
            if self.injection == 'io1':
                sql = 'SELECT * FROM access_info WHERE data1 = 1'
            if self.injection == 'io2':
                sql = "SELECT * FROM call_forwarding WHERE numberx = 'a'"
        if self.workload == 'seats':
            if self.injection == 'io1':
                sql = 'SELECT * from reservation WHERE r_price < 1000'


        if sql is not None:            
            self.connect_db()
            print(sql)
            execute(self.database, sql)
            self.close_db()
        # sql = 'START TRANSACTION'
        # execute(self.database, sql)        
        # tid = np.random.randint(1,11)
        # rnum = np.random.randint(1,11)
        # # sql = "SELECT * FROM orders{} WHERE o_ol_cnt >= {} ORDER BY o_ol_cnt DESC limit 100".format(tid, rnum)
        # # sql = "SELECT * FROM orders{} ORDER BY o_ol_cnt DESC limit 100".format(tid)
        # # sql = "SELECT * FROM customer{} WHERE c_ytd_payment <= {} ORDER BY c_ytd_payment DESC limit 100".format(tid, rnum)
        # # sql = "SELECT * FROM customer{} ORDER BY c_ytd_payment DESC limit 100".format(tid)
        # # sql = "SELECT * FROM order_line{} WHERE ol_amount <= {} ORDER BY ol_amount DESC limit 10000".format(tid, rnum)
        # sql = "SELECT sum(ol_amount) FROM order_line{} WHERE ol_w_id = {}".format(tid, rnum)
        # # sql = "SELECT * FROM order_line{} ORDER BY ol_amount DESC limit 100".format(tid)
        # print(sql)
        # results = execute(self.database, sql)
        # sql = 'COMMIT'
        # execute(self.database, sql)        
        # curr_time = time.time()
        # # time.sleep(1)
        # self.database.close()

    def inject_lock(self):
        sql = None
        start_sql = 'START TRANSACTION'
        commit_sql = 'COMMIT'
        sleep_time = 10
        if self.workload == 'tpcc':
            if self.injection == 'lock1':
                sql = "UPDATE warehouse SET w_ytd = 1"
            if self.injection == 'lock2':
                w_id = np.random.randint(1,11)
                sql = "UPDATE warehouse SET w_ytd = 1 WHERE w_id = {}".format(w_id)
            if self.injection == 'lock3':
                sql = "UPDATE district SET d_ytd = 1 WHERE d_id < 5"
            if self.injection == 'lock4':
                d_w_id = np.random.randint(1,11)
                sql = "UPDATE district SET d_ytd = 1 WHERE d_w_id = {}".format(d_w_id) 
        if self.workload == 'smallbank':
            if self.injection == 'lock1':
                sql = 'UPDATE savings SET bal = 100'
            if self.injection == 'lock2':
                sql = 'UPDATE savings SET bal = 100 WHERE custid < 1000'
        if self.workload == 'voter':
            if self.injection == 'lock1':
                sql = "UPDATE votes SET contestant_number = 1 WHERE created < NOW()"
        if self.workload == 'tatp':
            if self.injection == 'lock1':
                sql = 'UPDATE subscriber SET vlr_location = 1 WHERE msc_location < 1000'
            if self.injection == 'lock2':
                sql = "UPDATE special_facility SET data_b = 'a' WHERE data_a < 100"
        if self.workload == 'seats':
            if self.injection == 'lock1':
                sql = 'UPDATE reservation SET r_price = 1000'
            if self.injection == 'lock2':
                sql = 'UPDATE reservation SET r_price = 0 WHERE r_price = 100'
            if self.injection == 'lock3':
                sql = 'UPDATE flight SET f_seats_left = 0 WHERE f_seats_left < 10'
            if self.injection == 'lock4':
                sql = 'UPDATE customer SET c_balance = 100 WHERE c_balance = 200'
        if sql is not None:
            self.connect_db()
            execute(self.database, start_sql)
            print(sql)
            execute(self.database, sql)
            time.sleep(sleep_time)
            execute(self.database, commit_sql)
            self.close_db()
        # # sql = "SELECT * FROM order_line1 ORDER BY ol_amount DESC limit 100"
        # # w_id = np.random.randint(1,11)
        # # sql = "UPDATE warehouse1 SET w_ytd = 1 WHERE w_id = 1"
        # # sql = "UPDATE stock1 SET s_quantity = 1 WHERE s_i_id < 10 and s_w_id = 1"
        # # sql = "UPDATE district1 SET d_ytd = 1 WHERE d_id = 1 AND d_w_id = 1" 
        # # sql = "SELECT * FROM district1 WHERE d_id = 1 AND d_w_id = 1 FOR UPDATE"
        # sql = "UPDATE district1 SET d_ytd = 1 WHERE d_id < 10" 
        # # sql = "SELECT * FROM warehouse1"
        # print(sql)
        # results = execute(self.database, sql)
        # curr_time = time.time()
        # time.sleep(2)        
        # sql = "SET FOREIGN_KEY_CHECKS=0"
        # results = execute(self.database, sql)
        # sql = "DELETE FROM warehouse1 WHERE w_id = 1"
        # print(sql)
        # results = execute(self.database, sql)
        # sql = "SET FOREIGN_KEY_CHECKS=1"
        # results = execute(self.database, sql)        
        # time.sleep(2)
        # sql = 'COMMIT'
        # execute(self.database, sql)
        # self.database.close()

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--workload', type=str, default='tpcc', help='workload')
    parser.add_argument('--injection', type=str, default='io1', help='injection')
    parser.add_argument('--host', type=str, default='xxx', help='injection')
    parser.add_argument('--port', type=int, default=3306, help='port')
    parser.add_argument('--user', type=str, default='xxx', help='user')
    parser.add_argument('--passwd', type=str, default='xxx', help='password')
    parser.add_argument('--time', type=int, default=3600, help='port')
    parser.add_argument('--thread', type=int, default=8, help='port')
    args = parser.parse_args()

    ray.init()

    init_time = time.time()
    curr_time = time.time()
    while curr_time - init_time < args.time:
        injectors = [FaultInjector.remote(host=args.host, port=args.port, user=args.user, passwd=args.passwd, workload=args.workload, injection=args.injection) for i in range(args.thread)]
        if args.injection.startswith('lock'):
            injectors_futures = [injector.inject_lock.remote() for injector in injectors]
            ray.get(injectors_futures)
        if args.injection.startswith('io'):
            injectors_futures = [injector.inject_io.remote() for injector in injectors]
            ray.get(injectors_futures)
        #injector = FaultInjector(host=args.host, port=args.port, user=args.user, passwd=args.passwd)
        #if args.injection.startswith('lock'):
        #    injector.inject_lock(args.workload, args.injection)
        #if args.injection.startswith('io'):
        #    injector.inject_io(args.workload, args.injection)
        curr_time = time.time()
