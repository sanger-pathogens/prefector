import contextlib
from collections.abc import Iterator

import click


@contextlib.contextmanager
def handle_errors() -> Iterator[None]:
    """Catch unhandled exceptions and format them as clean CLI errors.

    In --debug mode (ctx.obj['debug'] is True) the original exception and
    traceback are re-raised so the caller can inspect the full stack.
    """
    debug = (click.get_current_context().find_object(dict) or {}).get("debug", False)
    try:
        yield
    except click.ClickException:
        raise
    except Exception as exc:
        if debug:
            raise
        raise click.ClickException(str(exc)) from None
