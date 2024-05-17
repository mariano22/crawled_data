# Crawled Data Stats Calculator

## Install

Install dependencies with:

```bash
pip install  -r requirements.txt
```

## Usage

#### Generate sample

For generating a random sample of the data and calculating the stats on it and on the whole data (and compare to check is non-biased) run:

```bash
python main_sample.py --input_dirs ./ds/webz_2022_01-2023_10,./ds/webz_2008_01-2013_12 --output_filename ./1_percentage_data.pickle --pick_ratio 0.01 --n_jobs 8
```

#### Calculate metrics on sample

For just calculating the metrics on some (already computed sample) run:

```bash
python main_stats.py --sample ./1_percentage_data.pickle --n_jobs 8
```

`n_jobs` always indicate the number of CPU used.

#### Clean a single .json file

For testing how rules works, you can clean one json file by doing:

```bash
python main_clean.py --input_json=./ds/webz_2022_01-2023_10/57090_webz_2022_11_4369c040ce466da884c26a1c1b50f39d_0000002.json
```

An output `./cleaned_<INPUT_JSON_FILENAME>` will be created with the cleaned data.
Also, `./cleaned_data.log` will contain the log of cleaned text in diff format.

## Directory structure

- `./ds/`: you can use for storing the datasets. This folder is not tracked.
- `calc_stats.py`: here are the metrics calculated. Adding more metrics for exploration is straightforward by providing a map and reduce functions.
- `rules.py`: contains the rule structure definition (see `main_clean.py` to check an example of usage), And way of monitoring the rules impact.

## Rules are extensible and learneable

You can create your own rules by implementing:

```python
class Rule(ABC):
    def fit(self, entries):
        pass

    @abstractmethod
    def apply(self, obj):
        pass

```

Rules might be learneable (for cases where it depends on the data stats).
