import argparse
import logging

from utils.transform.transformer import Transformer

logging.basicConfig(level=logging.INFO)


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="transform-sales")
    parser.add_argument("--conf")
    return parser.parse_args()


def main() -> None:
    args = parse_arguments()
    transformer = Transformer.from_config(args.conf)
    transformer.transform()


if __name__ == "__main__":
    main()
