import io
import logging
import argparse
import importlib
import contextlib

from pathlib import Path
from rich.console import Console
from typing import Iterable, Any
from prefect.settings import temporary_settings
from prefect.exceptions import ParameterTypeError
from prefect.types.entrypoint import EntrypointType

from prefector.argparse.prefect_connection import (
    attach_prefect_connection_options,
    generate_prefect_settings,
)
from prefector.deployments.base import DeploymentSpec, load_deployments, load_image_manifest

CONSOLE = Console()


def _select_targets(selected: Iterable[str], deployments: list[DeploymentSpec]) -> list[DeploymentSpec]:
    if not selected:
        return deployments

    selected = set(selected)
    index = {deployment.name: deployment for deployment in deployments if deployment.name in selected}

    missing = sorted(selected - set(index))
    if missing:
        available = ", ".join(sorted(deployment.name for deployment in deployments))
        raise ValueError(f"Unknown deployment name(s): {', '.join(missing)}. Available: {available}")

    return list(index.values())


def _resolve_flow(spec: DeploymentSpec):
    module = importlib.import_module(spec.module)
    return getattr(module, spec.function)


def _print_deployment_header(spec: DeploymentSpec) -> None:
    CONSOLE.print("[blue]──[/blue]")
    CONSOLE.print(f"Deployment: [bold]{spec.name}[/bold]")
    CONSOLE.print(f"[dim]Flow:[/dim] {spec.function}")
    CONSOLE.print(f"[dim]Image:[/dim] {spec.image_key}")


def _validate_parameters(flow_obj: Any, spec: DeploymentSpec) -> None:
    logger = logging.getLogger("prefect.flows")
    original_level = logger.level
    logger.setLevel(logging.CRITICAL)
    try:
        flow_obj.validate_parameters(spec.parameters)
    except ParameterTypeError as exc:
        details = str(exc).replace("Flow run received invalid parameters:", "").strip()
        raise ValueError(f"Invalid parameters for deployment '{spec.name}':\n{details}") from None
    finally:
        logger.setLevel(original_level)


def _build_image_name(*, image_prefix: str, image_name: str, image_tag: str) -> str:
    prefix = image_prefix.rstrip("/")
    base_name = f"{prefix}/{image_name}" if prefix else image_name

    has_tag = ":" in base_name.rsplit("/", maxsplit=1)[-1]
    if has_tag:
        return base_name
    return f"{base_name}:{image_tag}" if image_tag else base_name


def deploy_target(  # noqa: PLR0913
    spec: DeploymentSpec,
    *,
    work_pool_name: str,
    work_queue_name: str | None,
    image: str,
    dry_run: bool,
):
    _print_deployment_header(spec)
    CONSOLE.print("[1/3] Preparing deployment")

    flow_obj = _resolve_flow(spec)

    CONSOLE.print("[2/3] Validating parameters")
    _validate_parameters(flow_obj, spec)

    kwargs: dict[str, Any] = {
        "name": spec.name,
        "work_pool_name": work_pool_name,
        "image": image,
        "build": False,
        "push": False,
        "print_next_steps": False,
        "entrypoint_type": EntrypointType.MODULE_PATH,
    }

    if work_queue_name is not None:
        kwargs["work_queue_name"] = work_queue_name
    if spec.tags:
        kwargs["tags"] = spec.tags
    if spec.parameters:
        kwargs["parameters"] = spec.parameters
    if spec.cron:
        kwargs["cron"] = spec.cron

    if dry_run:
        CONSOLE.print("[green][✓][/green] Done (dry run)")
        return

    CONSOLE.print("[3/3] Creating deployment")
    with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
        deployment_id = flow_obj.deploy(**kwargs)

    CONSOLE.print(f"[green][✓][/green] Done (ID: {deployment_id})")


def _print_deployments(deployments: Iterable[DeploymentSpec]) -> None:
    for deployment in deployments:
        _print_deployment_header(deployment)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="List and deploy Prefect deployments from YAML specs.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    list_parser = subparsers.add_parser("list", help="Print configured deployment specs.")
    list_parser.add_argument(
        "--deployments-dir",
        type=Path,
        required=True,
        help="Directory with deployment YAML files.",
    )

    deploy_parser = subparsers.add_parser("deploy", help="Create/update deployments on Prefect server.")
    deploy_parser.add_argument(
        "--deployments-dir",
        type=Path,
        required=True,
        help="Directory with deployment YAML files.",
    )
    deploy_parser.add_argument(
        "--images-manifest",
        type=Path,
        required=True,
        help="Path to image manifest YAML (key -> name mapping).",
    )
    attach_prefect_connection_options(deploy_parser)
    deploy_parser.add_argument(
        "--target",
        action="append",
        default=[],
        help="Deployment name to deploy. Repeat --target to pass multiple values.",
    )
    deploy_parser.add_argument(
        "--work-pool",
        required=True,
        help="Work pool name for all deployments.",
    )
    deploy_parser.add_argument(
        "--work-queue",
        help="Work queue name for all deployments.",
    )
    deploy_parser.add_argument(
        "--image-prefix",
        required=True,
        help="Image prefix/registry, e.g. ghcr.io/org.",
    )
    deploy_parser.add_argument(
        "--image-tag",
        default="latest",
        help="Image tag for all deployments.",
    )
    deploy_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the deployment plan without calling Prefect API.",
    )

    return parser


def main(args: list[str] | None = None) -> int:
    parser = _build_parser()
    parsed = parser.parse_args(args)
    deployments = load_deployments(parsed.deployments_dir)

    if parsed.command == "list":
        _print_deployments(deployments)
        return 0

    if parsed.command == "deploy":
        try:
            images = load_image_manifest(parsed.images_manifest)
            prefect_settings = generate_prefect_settings(parsed)
            targets = _select_targets(parsed.target, deployments)
        except ValueError as exc:
            parser.exit(2, f"error: {exc}\n")

        with temporary_settings(updates=prefect_settings):
            for index, target in enumerate(targets):
                try:
                    deploy_target(
                        spec=target,
                        work_pool_name=parsed.work_pool,
                        work_queue_name=parsed.work_queue,
                        image=_build_image_name(
                            image_prefix=parsed.image_prefix,
                            image_name=images.get(target.image_key).name,
                            image_tag=parsed.image_tag,
                        ),
                        dry_run=parsed.dry_run,
                    )
                except ValueError as exc:
                    parser.exit(2, f"error: {exc}\n")
                if index < len(targets) - 1:
                    CONSOLE.print()

        return 0

    parser.error(f"Unsupported command: {parsed.command}")
