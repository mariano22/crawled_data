from urllib.parse import urlsplit, urlunsplit
import tldextract
import difflib


def get_lines(text):
    return text.split('\n')

def calculate_diff_string(text_old, text_new):
    d = difflib.Differ()
    lines_old = get_lines(text_old)
    lines_new = get_lines(text_new)
    df = d.compare(lines_old, lines_new)
    return '\n'.join([difference for difference in df if difference[0] not in ' '])

def get_full_domain(url):
    split_url = urlsplit(url)
    return split_url.netloc.lower()

def get_main_domain(url):
    return tldextract.extract(url).domain

def get_n_lines(text):
    return text.count('\n')

def pretty_key_values(key_values):
    return '\n'.join(f'\t- {k}: {v}' for k,v in key_values)