''' use queueing model to calculate the wait time
'''
import numpy as np
import matplotlib.pyplot as plt

class QueueEstimator:
    def __init__(self) -> None:
        pass

    def timestamp2qps(self, timestamp_list):
        ''' convert the timestamps to qps 
            timestamp_list: the list of timestamp for each request, to estimate the wait time

            sometimes, the qps data does not have enough length
        '''
        qps_list = []
        last_checkpoint = timestamp_list[0]
        count = 0
        for timestamp in timestamp_list:
            if timestamp - last_checkpoint < 1000:
                count += 1
            else:
                last_checkpoint += 1000 
                qps_list.append(count)
                count = 1
        return qps_list 


    def queueing_model(self, wait_count_list, qps_list, qps_interval_len=30):
        ''' Parameters
            ----------     
            wait_count_list: the number of waiting threads at different timestamps
            qps_list: the qps at different second
            qps_interval_len: how many qps points between two wait_count points
        '''
        delay_list = []
        idx = 0
        # print (f"wait_count_list: {wait_count_list}, qps_list: {qps_list}")
        for wait_count in wait_count_list:
            if idx*qps_interval_len < len(qps_list):
                qps_sublist = qps_list[idx*qps_interval_len:]
            else:
                qps_sublist = qps_list[-1]
            delay = self.delay_for_wait_count(wait_count, qps_sublist)
            delay_list.append(delay*1000) 
        return delay_list # use ms as unit
            
    def delay_for_wait_count(self, wait_count, qps_sublist):
        _sum = 0
        count = 0
        for qps in qps_sublist:         
            if _sum + qps > wait_count:
                count += (wait_count - _sum) / qps 
                return count 
            _sum += qps
            count += 1 
        # if no enough remaining data, use the average qps as the last qps
        return count + (wait_count - _sum) / np.average(qps_sublist)
        

    def plot(self, RT_list, delay_list, normal_RT=10):
        ''' Parameters
            ---------- 
            RT_list: the list of RT for each requests
        '''
        delta_RT_list = [RT-normal_RT for RT in RT_list]
        print (np.average(RT_list), delay_list)
        # plt.plot(delta_RT_list, delay_list)
        # plt.show()
        