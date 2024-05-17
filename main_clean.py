import argparse
import os
import jsonlines

from rules import apply_all_rules_and_log


def main(args):
    input_filepath = args.input_json
    input_filename = os.path.basename(input_filepath)
    input_filedir = os.path.dirname(input_filepath)
    
    output_preffix = 'cleaned_'
    output_filepath = os.path.join('./' , output_preffix + input_filename)
    
    with jsonlines.open(output_filepath, mode='w') as writer:
        with jsonlines.open(input_filepath) as reader:
            for obj in reader:
                apply_all_rules_and_log(obj)
            writer.write(obj)

if __name__ ==  '__main__':
    parser = argparse.ArgumentParser()
    parser = argparse.ArgumentParser()
    parser.add_argument("--input_json", 
                        help="JSON file with the objects to clean",
                        required=True,
                        type=str)
    args = parser.parse_args()
    main(args)