import importlib.metadata
import logging

import click
import mcp.types as mt
from datahub.ingestion.graph.client import get_default_graph
from datahub.ingestion.graph.config import ClientMode
from datahub.sdk.main_client import DataHubClient
from datahub.telemetry import telemetry
from datahub.utilities.perf_timer import PerfTimer
from fastmcp.server.middleware import CallNext, Middleware, MiddlewareContext
from fastmcp.server.middleware.logging import LoggingMiddleware
from typing_extensions import Literal

from mcp_server_datahub.mcp_server import mcp, with_datahub_client

logging.basicConfig(level=logging.INFO)


class TelemetryMiddleware(Middleware):
    """Middleware that logs tool calls."""

    async def on_call_tool(
        self,
        context: MiddlewareContext[mt.CallToolRequestParams],
        call_next: CallNext[mt.CallToolRequestParams, mt.CallToolResult],
    ) -> mt.CallToolResult:
        with PerfTimer() as timer:
            result = await call_next(context)

        telemetry.telemetry_instance.ping(
            "mcp-server-tool-call",
            {
                "tool": context.message.name,
                "source": context.source,
                "type": context.type,
                "method": context.method,
                "duration_seconds": timer.elapsed_seconds(),
            },
        )

        return result


@click.command()
@click.option(
    "--transport",
    type=click.Choice(["stdio", "sse", "http"]),
    default="stdio",
)
@click.option(
    "--debug",
    is_flag=True,
    default=False,
)
@telemetry.with_telemetry(
    capture_kwargs=["transport"],
)
def main(transport: Literal["stdio", "sse", "http"], debug: bool) -> None:
    # Because we want to override the datahub_component, we can't use DataHubClient.from_env()
    # and need to use the DataHubClient constructor directly.
    mcp_version = importlib.metadata.version("mcp-server-datahub")
    graph = get_default_graph(
        client_mode=ClientMode.SDK,
        datahub_component=f"mcp-server-datahub/{mcp_version}",
    )
    client = DataHubClient(graph=graph)

    if debug:
        mcp.add_middleware(LoggingMiddleware(include_payloads=True))
    mcp.add_middleware(TelemetryMiddleware())

    with with_datahub_client(client):
        if transport == "http":
            mcp.run(transport=transport, show_banner=False, stateless_http=True)
        else:
            mcp.run(transport=transport, show_banner=False)


if __name__ == "__main__":
    main()
