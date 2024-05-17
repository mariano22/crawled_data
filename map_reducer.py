import glob
from functools import partial
from pathlib import Path
import jsonlines
from pqdm.processes import pqdm
from collections import defaultdict


def _open_json_and_collect(filename, map_function):
    values = []
    with jsonlines.open(filename) as reader:
        for obj in reader:
            values.extend(map_function(obj))
    return values

class MapReducer:
    def __init__(self, map_function, reduce_function, post_processing = None):
        self.map_function = map_function
        self.reduce_function = reduce_function
        self.post_processing = post_processing

    def run_sample(self, iterator, n_jobs):
        map_results = pqdm(iterator, self.map_function, n_jobs=n_jobs)
        
        key_to_values = defaultdict(list)
        for key_value_map_result in map_results:
            for k,v in key_value_map_result:
                key_to_values[k].append(v)
        
        result = {}
        for k in key_to_values:
            result[k] = self.reduce_function(k, key_to_values[k])
        
        if self.post_processing:
            result = self.post_processing(result)
        
        return result
    
    def run_directories(self, directories, *args, **kwargs):
        map_function_jsons = partial(_open_json_and_collect, map_function=self.map_function)
        inner_runner =  MapReducer(map_function_jsons, self.reduce_function)
        
        global_map_reduce_result = defaultdict(list)
        for directory_name in directories:
            glob_path = Path(directory_name) / f'*.json'
            json_filenames = glob.glob(str(glob_path))
            map_reduce_result = inner_runner.run_sample(json_filenames, *args, **kwargs)
            
            for k,v in map_reduce_result.items():
                global_map_reduce_result[k].append(v)
                
        result = {}
        for k,vs in global_map_reduce_result.items():
            result[k] = self.reduce_function(k, vs)

        if self.post_processing:
            result = self.post_processing(result)
        
        return result