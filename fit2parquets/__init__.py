"""
.. include:: ../README.md
"""

import fire

from fit2parquets.parser import Parser


def main() -> None:
    fire.Fire(Parser.fit2parquets)


if __name__ == "__main__":
    main()
