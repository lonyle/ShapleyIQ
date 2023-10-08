''' bar plot of the result
'''
import json

from rca4tracing.rca.experiment.plot.get_bar_plot import get_bar_graph


def plot_top_k(input_path, image_name, 
               algorithms=['ShapleyValueRCA', 'MicroHECL', 'MicroRCA', 'TON', 'MicroRank'], 
               top_k_list=[1,2,3]):
    
    with open(input_path + 'top_k.json') as f:
        top_k = json.load(f)

    data_dict = dict()
    for k in top_k_list:
        key = f'top_{k}_score_dict'
        data_dict[key] = top_k[key]
    
    xtick = []
    # for k in top_k_list:
    for k in range(1, len(top_k_list)+1):
        xtick.append(f"top-{k}") #= ["top-1", "top-3", "top-5"]

    get_bar_graph(image_name, data_dict, algorithms=algorithms, xtick=xtick)

    