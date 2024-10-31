import argparse
import logging

from utils.load.json import JsonLoader

logging.basicConfig(level=logging.INFO)


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="load-json")
    parser.add_argument("--conf")
    return parser.parse_args()


def main() -> None:
    args = parse_arguments()
    loader = JsonLoader.from_config(args.conf)
    loader.load()


if __name__ == "__main__":
    main()
