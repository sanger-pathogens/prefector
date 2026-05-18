import argparse
import sys

from prefector.blocks.cli import main as blocks_main
from prefector.deployments.cli import main as deployments_main


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Manage Prefect resources from reusable specs.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "kind",
        choices=("blocks", "deployments"),
        help="Resource type to manage.",
    )
    return parser


def main(args: list[str] | None = None) -> int:
    argv = list(sys.argv[1:] if args is None else args)
    parser = _build_parser()
    parsed = parser.parse_args(argv[:1])

    if parsed.kind == "blocks":
        return blocks_main(argv[1:])

    if parsed.kind == "deployments":
        return deployments_main(argv[1:])

    parser.error(f"Unsupported command: {parsed.kind}")


if __name__ == "__main__":
    raise SystemExit(main())
