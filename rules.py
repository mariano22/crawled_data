from abc import ABC, abstractmethod
import logging
from contextlib import contextmanager
import re

from tools import calculate_diff_string
from english_detector import remove_non_english_parts

logger = logging.getLogger('clean_data')
logger.setLevel(logging.DEBUG)
# create file handler which logs even debug messages
fh = logging.FileHandler('clean_data.log')
fh.setLevel(logging.DEBUG)
logger.addHandler(fh)

class Rule(ABC):
    def fit(self, entries):
        pass
    
    @abstractmethod
    def apply(self, obj):
        pass

# TODO: modify the monitor to annotate with the rules that are applied
@contextmanager
def monitor_text_changes(obj):
    old_text = obj['text']
    yield
    new_text = obj['text']
    diff_str = calculate_diff_string(old_text, new_text)
    if diff_str:
        uuid = obj.get('uuid', 'no-id')
        url = obj.get('url', 'no-url')
        logger.info(f'Difference found for uuid {uuid} (url {url})\n'+diff_str)

class EraseObjRule(Rule):
    def __init__(self, erase_condition_cb):
        self.erase_condition_cb = erase_condition_cb

    def apply(self, obj):
        if self.erase_condition_cb(obj):
            obj['text'] = ''

class EraseTextRule(Rule):
    def __init__(self, string_to_erase):
        self.string_to_erase = string_to_erase

    def apply(self, obj):
        obj['text'] = obj['text'].replace(self.string_to_erase,'')

class ModifyTextRule(Rule):
    def __init__(self, modify_text_cb):
        self.modify_text_cb = modify_text_cb

    def apply(self, obj):
        obj['text'] = self.modify_text_cb(obj['text'])

class CompositionRule(Rule):
    def __init__(self, rules):
        self.rules = rules
    
    def fit(self, entries):
        for rule in self.rules:
            rule.fit(entries)
    
    def apply(self, obj):
        for rule in self.rules:
            rule.apply(obj)

def has_nonalphanumeric_live_rule(obj):
    pattern = r'\Wlive\W'
    return re.search(pattern, obj['url']) is not None

def short_text_rule(obj):
    return len(obj['text']) < 500

def spanish_url_rule(obj):
    return 'espanol' in obj['url']

def video_url_rule(obj):
    return '/video' in obj['url']

def lists_url_rule(obj):
    return '/lists' in obj['url']

def job_the_guardian_url_rule(obj):
    return 'jobs.theguardian.com' in obj['url']

ALL_RULES = CompositionRule([
    EraseObjRule(has_nonalphanumeric_live_rule),
    EraseObjRule(short_text_rule),
    EraseObjRule(spanish_url_rule),
    EraseObjRule(video_url_rule),
    EraseObjRule(lists_url_rule),
    EraseObjRule(job_the_guardian_url_rule),
    EraseTextRule("Gannett may earn revenue from sports betting operators for audience referrals to betting services. Sports betting operators have no influence over nor are any such revenues in any way dependent on or linked to the newsrooms or news coverage. See operator site for Terms and Conditions. If you or someone you know has a gambling problem, help is available. Call the National Council on Problem Gambling 24/7 at 1-800-GAMBLER. Must be 21 or older to gamble."),
    EraseTextRule("View CBS News In CBS News App Open Chrome Safari Continue Be the first to know Get browser notifications for breaking news, live events, and exclusive reporting. Not Now Turn On"),
    EraseTextRule("© 2022 CBS Broadcasting Inc. All Rights Reserved.Thanks for reading CBS NEWS. Create your free account or log infor more features. Continue Please enter email address to continue Please enter valid email address to continue Featured Local Savings"),
    EraseTextRule("Watch CBS News©2022 CBS Broadcasting Inc. All Rights Reserved."),
    EraseTextRule("View more on twitter"),
    EraseTextRule("View more opinion on CNN"),
    EraseTextRule("View more opinion articles on CNN"),
    EraseTextRule("Reuse this content"),
    EraseTextRule("Our Standards: The Thomson Reuters Trust Principles."),
    EraseTextRule("Register now for FREE unlimited access to Reuters.com Register"),
    EraseTextRule("Register now for FREE unlimited access to Reuters.com"),
    EraseTextRule("Add articles to your saved list and come back to them any time. "),
    EraseTextRule("Add galleries to your saved list and come back to them any time."),
    EraseTextRule("The Morning Edition newsletter is our guide to the day’s most important and interesting stories, analysis and insights. Sign up here ."),
    EraseTextRule("Subscribers can sign up to our weekly Inside Politics newsletter here ."),
    EraseTextRule("News, results and expert analysis from the weekend of sport sent every Monday. Sign up for our Sport newsletter ."),
    EraseTextRule("Keep up to date with the best AFL coverage in the country. Sign up for the Real Footy newsletter ."),
    EraseTextRule("The Business Briefing newsletter delivers major stories, exclusive coverage and expert opinion. Sign up to get it every weekday morning ."),
    EraseTextRule("Get exclusive travel deals delivered straight to your inbox. Sign up now ."),
    EraseTextRule("Sign up for CNN's Wonder Theory science newsletter. Explore the universe with news on fascinating discoveries, scientific advancements and more ."),
    # TODO: this rule should be more robust
    #ModifyTextRule(remove_non_english_parts),
])

def apply_all_rules_and_log(obj, fit_entries = []):
    if fit_entries:
        ALL_RULES.fit(fit_entries)
        
    with monitor_text_changes(obj):
        ALL_RULES.apply(obj)