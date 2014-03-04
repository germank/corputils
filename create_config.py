#!/usr/bin/env python

import os
import argparse

def main():
    p = argparse.ArgumentParser(description='Create a configuration file')
    p.add_argument('filename', default='config.yml', nargs='?',
                    help='Configuration filename')
    args = p.parse_args()
    config_file = os.path.join(os.path.dirname(__file__), 'example_config.yml')
    config_str = file(config_file).read()
    with open(args.filename, 'w') as f:
        f.write(config_str)


if __name__ == '__main__':
    main()