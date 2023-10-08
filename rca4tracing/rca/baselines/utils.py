import numpy as np 

def similarity(a, b, default_value=0):
    ''' when the length of a and b is not the same
    '''
    # print (a)
    # print (b)
    if len(a) != len(b):
        min_len = min(len(a), len(b))
        a = a[:-min_len]
        b = b[:-min_len]
    if a and b and np.var(a) and np.var(b):
        return abs(np.corrcoef(a, b)[1, 0])
    else:
        return default_value