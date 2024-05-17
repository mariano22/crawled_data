from map_reducer import MapReducer

from tools import get_full_domain, get_main_domain, get_n_lines

# number_of_objects(entries) = len(entries)

def number_of_objects_map(_):
    return [(None, 1)]

def number_of_objects_reduce(_, values):
    return sum(values)

# total_length(entries) = sum(obj['text] for obj in entries)

def total_length_map(obj):
    return [(None, len(obj['text']))]

def total_length_reduce(_, values):
    return sum(values)

# full_domain_distribution(entries) = Counter(get_full_domain(obj['url]) for obj in entries)

def full_domain_distribution_map(obj):
    return [(get_full_domain(obj['url']), 1)]

def full_domain_distribution_reduce(_, values):
    return sum(values)

# full_domain_distribution(entries) = Counter(get_main_domain(obj['url]) for obj in entries)

def main_domain_distribution_map(obj):
    return [(get_main_domain(obj['url']), 1)]

def main_domain_distribution_reduce(_, values):
    return sum(values)

# text_length_div100_distribution(entries) = Counter(len(obj['text])//100 for obj in entries)

def text_length_div100_distribution_map(obj):
    return [(len(obj['text'])//100, 1)]

def text_length_div100_distribution_reduce(_, values):
    return sum(values)

# text_lines_div10_distribution(entries) = Counter(get_n_lines(obj['text])//100 for obj in entries)

def text_lines_div10_distribution_map(obj):
    return [(get_n_lines(obj['text'])//10, 1)]

def text_lines_div10_distribution_reduce(_, values):
    return sum(values)

def singleton_post_processing(result):
    return result[None]

def distribution_post_processing(result):
    total = sum(result.values())
    return sorted([(k,v/total) for k,v in result.items()], key = lambda tuple: -tuple[1])

def get_post_processing(stat_name):
    if 'distribution' in stat_name:
        return distribution_post_processing
    return singleton_post_processing

def get_stat_calculator(stat_name):
    map_cb = eval(stat_name+'_map')
    reduce_cb = eval(stat_name+'_reduce')
    post_processing_cb = get_post_processing(stat_name)
    return MapReducer(map_cb, reduce_cb, post_processing_cb)



