import json
import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime

from mcp.server.fastmcp import FastMCP
from mcp.server.transport_security import TransportSecuritySettings

from spark_history_mcp.config.config import Config
from spark_history_mcp.core.client_context import ClientContext


@dataclass
class AppContext:
    clients: ClientContext
    config: Config


class DateTimeEncoder(json.JSONEncoder):
    """Custom JSON encoder that handles datetime objects."""

    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)


@asynccontextmanager
async def app_lifespan(server: FastMCP) -> AsyncIterator[AppContext]:
    # Config() automatically loads from SHS_MCP_CONFIG env var (set in main.py)
    config = Config()
    yield AppContext(clients=ClientContext(config), config=config)


def run(config: Config):
    mcp.settings.host = config.mcp.address
    mcp.settings.port = int(config.mcp.port)
    mcp.settings.debug = bool(config.mcp.debug)

    # Configure transport security settings for DNS rebinding protection
    # See: https://github.com/modelcontextprotocol/python-sdk/issues/1798
    if config.mcp.transport_security:
        ts_config = config.mcp.transport_security
        mcp.settings.transport_security = TransportSecuritySettings(
            enable_dns_rebinding_protection=ts_config.enable_dns_rebinding_protection,
            allowed_hosts=ts_config.allowed_hosts,
            allowed_origins=ts_config.allowed_origins,
        )

    mcp.run(transport=os.getenv("SHS_MCP_TRANSPORT", config.mcp.transports[0]))


mcp = FastMCP("Spark Events", lifespan=app_lifespan)

# Import tools to register them with MCP
from spark_history_mcp.tools import tools  # noqa: E402,F401
