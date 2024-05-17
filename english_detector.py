import nltk
from polyglot.detect import Detector
from polyglot.detect.base import logger as polyglot_logger

polyglot_logger.propagate = False
nltk.download('punkt')

def is_english(text):
    try:
        return Detector(text, quiet=True).language.code == 'en'
    except Exception as e:
        return False

def remove_non_english_parts(text):
    try:
        # Split the text into sentences
        sentences = nltk.sent_tokenize(text)
        english_parts = []

        erased_parts = []
        for sentence in sentences:
            if is_english(sentence):
                english_parts.append(sentence)
            else:
                erased_parts.append(sentence)

        # Join the English parts back into a single string
        cleaned_text = ' '.join(english_parts)
        erased_text = ' '.join(erased_parts)
        if len(erased_text)>50:
            return cleaned_text
        return text

    except Exception as e:
        return text
