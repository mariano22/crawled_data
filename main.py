import argparse
import pickle
import os

from sampling import CrawlingDataset_SubSample
from calc_stats import get_stat_calculator
from tools import pretty_key_values


def main(args):
    directories = args.input_dirs.split(',')
    pick_ratio = args.pick_ratio
    n_jobs = args.n_jobs
    out_fn = args.output_filename
    
    print('Calculating subsample...')
    sample = CrawlingDataset_SubSample()
    sample.load_directory_list(directories, pick_probability = pick_ratio, n_jobs=n_jobs)
    
    print(f'Saving subsample at {out_fn}')
    with open( out_fn, "wb" ) as f:
        pickle.dump( sample, f)
        
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
        
        print(f'Calculating {metric_name} in all dataset...')
        result_all_dataset = runner.run_directories(directories, n_jobs = n_jobs)
        
        print(f'Calculating {metric_name} in subsample...')
        result_subsample = runner.run_sample(sample.entries, n_jobs = n_jobs)
        
        # TODO: move this to calc_stats.py
        if 'distribution' in metric_name:
            top_show = 10
            result_all_dataset_top = result_all_dataset[:top_show]
            result_subsample_top = result_subsample[:top_show]
            print(f'{metric_name} for all dataset:')
            print(pretty_key_values(result_all_dataset_top))
            print(f'{metric_name} for subsample:')
            print(pretty_key_values(result_subsample_top))
            print('Ratio distributions: ')
            result_subsample_dict = dict(result_subsample)
            ratio_distributions = [ (k,result_subsample_dict.get(k,0)/v) for k,v in result_all_dataset_top if k in result_subsample_dict ]
            print(pretty_key_values(ratio_distributions))
        else:
            print(f'{metric_name} for all dataset: {result_all_dataset}')
            print(f'{metric_name} for subsample: {result_subsample}')
            print(f'Ratio: {result_subsample/result_all_dataset}')


if __name__ ==  '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--input_dirs", 
                        help="Comma separated directory name list. The directories must contain the dataset .json files.",
                        required=True,
                        type=str)
    parser.add_argument("--output_filename", 
                        help="Output filename path for storing the generated sample.", 
                        required=True,
                        type=str)
    parser.add_argument("--pick_ratio", 
                        help="Sample ratio of the generated sample. Default is 0.01 (will sample 1%% of the dataset)",
                        required=False,
                        default=0.01,
                        type=float)
    parser.add_argument("--n_jobs", 
                        help="Number of process used for the task. Default: os.cpu_count()",
                        required=False,
                        default=os.cpu_count(),
                        type=int)
    args = parser.parse_args()
    main(args)