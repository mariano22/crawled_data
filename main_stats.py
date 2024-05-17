import argparse
import pickle
import os

from sampling import CrawlingDataset_SubSample
from calc_stats import get_stat_calculator
from tools import pretty_key_values


def main(args):
    sample_fn = args.sample
    n_jobs = args.n_jobs

    print(f'Loading subsample at {sample_fn}')
    with open( sample_fn, "rb" ) as f:
        sample = pickle.load(f)
        
    # TODO: move this to calc_stats.py
    metric_names = [
                    'number_of_objects',
                    'total_length',
                    'full_domain_distribution',
                    'main_domain_distribution',
                    'text_length_div100_distribution',
                    'text_lines_div10_distribution',
                    ]
    
    for metric_name in metric_names:
        print(f'Calculating metric: {metric_name}')
        runner = get_stat_calculator(metric_name)
        
        print(f'Calculating {metric_name} in subsample...')
        result_subsample = runner.run_sample(sample.entries, n_jobs = n_jobs)
        
        # TODO: move this to calc_stats.py
        if 'distribution' in metric_name:
            top_show = 10
            result_subsample_top = result_subsample[:top_show]
            print(f'{metric_name} for subsample:')
            print(pretty_key_values(result_subsample_top))
        else:
            print(f'{metric_name} for subsample: {result_subsample}')


if __name__ ==  '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--sample", 
                        help="The pickle of the sampled dataset", 
                        required=True,
                        type=str)
    parser.add_argument("--n_jobs", 
                        help="Number of process used for the task. Default: os.cpu_count()",
                        required=False,
                        default=os.cpu_count(),
                        type=int)
    args = parser.parse_args()
    main(args)