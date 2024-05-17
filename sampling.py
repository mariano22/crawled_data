from pathlib import Path
from functools import partial
from pqdm.processes import pqdm
import jsonlines
import random
import glob


def _get_objcts_from_jsonl(filename, pick_probability):
    objs = []
    with jsonlines.open(filename) as reader:
        for obj in reader:
            # Randomly pick an entry to avoid any bias
            if random.uniform(0, 1) < pick_probability:
                objs.append(obj)
    return objs

class CrawlingDataset_SubSample:
    def __init__(self, stored_fields = ['url', 'text'], text_field = 'text'):
        self.entries = []
        self.total_length = 0
        self.stored_fields = stored_fields
        self.text_field = text_field
    
    def _get_text(self, obj):
        return obj.get(self.text_field, '')
    
    def _store(self, obj, ds_tag=''):
        obj_to_store = { k: obj[k] for k in self.stored_fields }
        self.total_length = len(self._get_text(obj))
        if ds_tag:
            obj_to_store['ds'] = ds_tag
        self.entries.append(obj_to_store)

    def load_directory(self, directory_name, pick_probability, n_jobs = 1):
        glob_path = Path(directory_name) / f'*.json'
        json_filenames = glob.glob(str(glob_path))
        load_function = partial(_get_objcts_from_jsonl, pick_probability=pick_probability)
        objs_list = pqdm(json_filenames, load_function, n_jobs=n_jobs)
        for objs in objs_list:
            for obj in objs:
                self._store(obj, ds_tag=directory_name)
    
    def load_directory_list(self, directory_list, pick_probability, **kwargs):
        for directory_name in directory_list:
            self.load_directory(directory_name, pick_probability, **kwargs)