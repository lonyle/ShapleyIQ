export PYTHONPATH=$(pwd)

python rca4tracing/rca/experiment/experiment_jaeger.py --collect_data=True
python rca4tracing/rca/experiment/plot/plot_figures.py --figure=jaeger_trace

python rca4tracing/rca/experiment/experiment_jaeger.py --exp=multiple_root_causes --collect_data=True
python rca4tracing/rca/experiment/plot/plot_figures.py --figure=jaeger_multi_root_cause

python rca4tracing/rca/experiment/experiment_jaeger_operation.py --collect_data=True
python rca4tracing/rca/experiment/plot/plot_figures.py --figure=jaeger_global

python rca4tracing/rca/experiment/plot/plot_running_time.py




