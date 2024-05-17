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

## Directory structure

- `./ds/`: you can use for storing the datasets. This folder is not tracked.
- `main_sample.py`: for generating the sample and calculating the metrics on it. It will also calculate the metrics on the whole data for checking the distribution is non-biased.
- `calc_stats.py`: here are the metrics calculated. Adding more metrics for exploration is straightforward by providing a map and reduce functions.
