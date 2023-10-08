import json

from rca4tracing.rca.experiment.plot.get_bar_plot import get_bar_graph


algorithms=['ShapleyValueRCA', 'MicroHECL', 'MicroRCA', 'TON', 'MicroRank']
prefix = 'rca4tracing/rca/experiment/output_data/experiment_jaeger/'

ylabel = 'top-1 acc. (%)'

def load_data_from_folders(folder_names):
    data_dict = dict()
    for folder_name in folder_names:
        input_path = prefix + folder_name
        with open(input_path + 'top_k.json') as f:
            top_k = json.load(f)
            data_dict[folder_name] = top_k['top_1_score_dict']
    return data_dict

def plot_delay(image_name):
    algorithms=['ShapleyValueRCA', 'MicroHECL', 'MicroRCA', 'TON', 'MicroRank']
    folder_names = [
        '_result_summary_20220413-10:44[100][5][5]_trace/',
        '_result_summary_20220413-11:48[200][5][5]_trace/',
        '_result_summary_20220413-11:56[500][5][5]_trace/',
        # '_result_summary_20220413-11:57[1000][5][5]_trace/'
    ]
    data_dict = load_data_from_folders(folder_names)
    xtick = ['delay=100ms', 'delay=200ms', 'delay=500ms']
    get_bar_graph(image_name, data_dict, algorithms=algorithms, xtick=xtick,
                  ylabel=ylabel)

def plot_users(image_name):    
    folder_names = [
        '_result_summary_20220413-11:56[500][5][5]_trace/',
        '_result_summary_20220413-19:30[500][10][5]_trace/',
        '_result_summary_20220414-09:28[500][20][5]_trace/',
        # '_result_summary_20220414-09:42[500][50][10]_trace/'
    ]
    data_dict = load_data_from_folders(folder_names)
    xtick = ['num. users=5', 'num. users=10', 'num. users=20']
    get_bar_graph(image_name, data_dict, algorithms=algorithms, xtick=xtick,
                  ylabel=ylabel)

def plot_spawn_rate(image_name):    
    folder_names = [
        '_result_summary_20220413-11:56[500][5][5]_trace/',
        '_result_summary_20220414-09:40[500][5][10]_trace/',
        '_result_summary_20220414-09:32[500][5][20]_trace/',
    ]
    data_dict = load_data_from_folders(folder_names)
    xtick = ['spawn rate=5', 'spawn rate=10', 'spawn rate=20']
    get_bar_graph(image_name, data_dict, algorithms=algorithms, xtick=xtick, 
                  ylabel=ylabel)


if __name__ == '__main__':
    import argparse 
    parser = argparse.ArgumentParser()
    parser.add_argument('--figure',
                        help='the name/id of the figure for different settings',
                        default='jaeger_delay')
    args = parser.parse_args()
    figure_name = args.figure
    if figure_name == 'jaeger_delay':
        plot_delay('jaeger_delay')
    elif figure_name == 'jaeger_user':
        plot_users('jaeger_user')
    elif figure_name == 'jaeger_spawn_rate':
        plot_spawn_rate('jaeger_spawn_rate')