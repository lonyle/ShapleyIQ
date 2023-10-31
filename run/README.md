## 1. We need to prepare the environment variable before running and plotting

```bash
export PYTHONPATH=/path/to/ShapleyIQ/
```

```bash
cd /path/to/ShapleyIQ/
```
We use python 3.7. If some of the Python packages are missing, please refer to requirements.txt.

## 2. Run the experiments using the TrainTicket dataset
- Experiments for the cases with a single root cause (the results will be similar to that in Figure 7(b))
```bash
python rca4tracing/rca/experiment/experiment_jaeger.py 
```
After you run, the raw results will be stored in **rca4tracing/rca/experiment/output_data/experiment_jaeger/**. To plot the figures, you should follow Section "3. Plot the figures".


- Experiments for the cases with multiple root causes (Figure 9(b))
```bash
python rca4tracing/rca/experiment/experiment_jaeger.py --exp=multiple_root_causes
```

- Experiments for global root cause analysis (Figure 8(b))
```bash
python rca4tracing/rca/experiment/experiment_jaeger_operation.py
```

<!-- - Experiments to compare under different settings (Figure 10(a) and Figure 10(b))
```bash
python rca4tracing/rca/experiment/experiment_jaeger.py --exp=different_delay
python rca4tracing/rca/experiment/experiment_jaeger.py --exp=different_users
``` -->

## 3. Plot the figures 
The scripts to plot the figures are in the folder **plot**, you can run the following bash script:
```bash
python rca4tracing/rca/experiment/plot/plot_figures.py --figure=jaeger_trace
python rca4tracing/rca/experiment/plot/plot_figures.py --figure=jaeger_global
python rca4tracing/rca/experiment/plot/plot_figures.py --figure=jaeger_multi_root_cause
```

For Figure 9(b) in the paper (corresponding to `--figure=jaeger_multi_root_cause`), as we do not release the code for Sage, we have changed the layout of the figure to remove the results of Sage and add one more group top-$|{S}_i^*|+2$.

```bash
python rca4tracing/rca/experiment/plot/plot_running_time.py
```
<!-- ```bash
python rca4tracing/rca/experiment/plot/plot_different_setting.py --figure=jaeger_delay
python rca4tracing/rca/experiment/plot/plot_different_setting.py --figure=jaeger_user
``` -->
