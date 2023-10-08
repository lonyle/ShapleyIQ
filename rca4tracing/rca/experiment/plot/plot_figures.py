from rca4tracing.rca.experiment.plot.plot_top_k import plot_top_k 

PREFIX = 'rca4tracing/rca/experiment/output_data/'

if __name__ == '__main__':
    import argparse 
    parser = argparse.ArgumentParser()
    parser.add_argument('--figure',
                        help='the name/id of the figure',
                        default='jaeger_trace')
    args = parser.parse_args()
    figure_name = args.figure
    if figure_name == 'jaeger_trace':
        plot_top_k(PREFIX + 'experiment_jaeger/_result_summary_jaeger_trace[200][5][5][]_trace/', 'jaeger_trace')
    elif figure_name == 'jaeger_global':
        plot_top_k(PREFIX + 'experiment_jaeger/_result_summary_[100, 200, 500][1, 5, 10][1, 5]_global/', 'jaeger_global')
    elif figure_name == 'jaeger_multi_root_cause':
        plot_top_k(PREFIX + 'experiment_jaeger/_result_summary_jaeger_multi_root_cause[200][5][5][]_trace/', 'jaeger_multi_root_cause', top_k_list=[2,3,4])