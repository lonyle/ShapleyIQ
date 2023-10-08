''' distribute to maximize the probability assuming gamma distribution
    suppose the shape parameter for the i_th variable is k_i
'''
import numpy as np

class GammaDistributor:
    def __init__(self, mean_vec, k_vec, L):
        self.mean_vec = mean_vec 
        self.k_vec = k_vec
        self.L = L
        self.n = len(self.mean_vec)
        self.theta_vec = np.asarray(self.mean_vec) / np.asarray(self.k_vec)

    def distribute(self):
        ''' mean_vec are the mean values of the variables
        '''
        dist_vec = [0] * self.n
        lambda_ = self.solve_lambda()
        for i in range(self.n):
            val = (self.k_vec[i]-1) / (1/self.theta_vec[i] - lambda_)
            if val >= 0:
                dist_vec[i] = val 
        return dist_vec


    def solve_lambda(self):
        # solve lambda by binary search
        delta = 10 ** (-5)
        left = -self.L
        right = self.L
        while right - left > delta:
            mid = (left + right) / 2
            val = self.evaluate_lambda(mid) 
            # print (mid, val)
            if val > self.L or val == 0:
                right = mid 
            else:
                left = mid
        return left

    def plot_lambda(self):
        import matplotlib.pyplot as plt
        x_vec = np.arange(-10, 10, 0.01)
        y_vec = []
        for x in x_vec:
            y_vec.append(self.evaluate_lambda(x))
        
        # print (y_vec)
        # print (x_vec)
        plt.plot(x_vec, y_vec)
        plt.show()


    def evaluate_lambda(self, lambda_):
        # monotone increasing
        _sum = 0
        for i in range(self.n):
            if (1/self.theta_vec[i] - lambda_) < 10**(-3):
                return 0
            val = (self.k_vec[i]-1) / (1/self.theta_vec[i] - lambda_)
            if val >= 0:
                _sum += val 
        return _sum 
            
if __name__ == '__main__':
    mean_vec = [1, 10, 0.1]
    k_vec = [1, 2, 2]
    L = 100
    gamma_distributor = GammaDistributor(mean_vec, k_vec, L)
    # print (gamma_distributor.evaluate_lambda(0))
    # gamma_distributor.plot_lambda()
    ret = gamma_distributor.distribute()
    print (ret)

    
