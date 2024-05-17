from urllib.parse import urlsplit, urlunsplit
import tldextract


def get_full_domain(url):
    split_url = urlsplit(url)
    return split_url.netloc.lower()

def get_main_domain(url):
    return tldextract.extract(url).domain

def get_n_lines(text):
    return text.count('\n')

def pretty_key_values(key_values):
    return '\n'.join(f'\t- {k}: {v}' for k,v in key_values)