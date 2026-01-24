"""
ClientContext for managing static and ephemeral Spark clients.

Static servers (regular SHS, EMR on EC2): single client per server
Ephemeral servers (EMR Serverless): on-demand client creation per job run
"""

import logging
import time
from typing import Dict, Iterator, List, Optional, Protocol, Tuple

from spark_history_mcp.api.emr_persistent_ui_client import EMRPersistentUIClient
from spark_history_mcp.api.spark_client import SparkRestClient
from spark_history_mcp.config.config import Config

logger = logging.getLogger(__name__)


class EphemeralClientProvider(Protocol):
    """Protocol for providers that create clients on-demand for discovered apps."""

    def get_client_for_app(self, app_id: str) -> SparkRestClient:
        """Get/create client for a specific app (job run)."""
        ...

    def discover_apps(
        self,
        status: Optional[List[str]] = None,
        min_date: Optional[str] = None,
        max_date: Optional[str] = None,
        min_end_date: Optional[str] = None,
        max_end_date: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> List[str]:
        """Discover available apps with filtering, return app_ids."""
        ...


class ClientContext:
    """Manages both static and ephemeral Spark clients."""

    def __init__(self, config: Config, cache_ttl: int = 300):
        """
        Initialize ClientContext from configuration.

        Args:
            config: The application configuration
            cache_ttl: TTL in seconds for app_id -> server cache (default 300)
        """
        self._config = config
        self._default_server: Optional[str] = None
        self._static_servers: Dict[str, SparkRestClient] = {}
        self._ephemeral_providers: Dict[str, EphemeralClientProvider] = {}
        self._app_cache: Dict[str, Dict] = {}  # app_id -> {"server": str, "last_updated": float}
        self._cache_ttl: int = cache_ttl

        for name, server_config in config.servers.items():
            if server_config.emr_serverless_application_id:
                # EMR Serverless - register as ephemeral provider
                emr_serverless_client = EMRServerlessClient(server_config)
                self._ephemeral_providers[name] = emr_serverless_client

                if server_config.default and self._default_server is None:
                    self._default_server = name

            elif server_config.emr_cluster_arn:
                # EMR on EC2 - single client per cluster
                emr_client = EMRPersistentUIClient(server_config)

                # Initialize EMR client (create persistent UI, get presigned URL, setup session)
                base_url, session = emr_client.initialize()

                # Create a modified server config with the base URL
                emr_server_config = server_config.model_copy()
                emr_server_config.url = base_url

                # Create SparkRestClient with the session
                spark_client = SparkRestClient(emr_server_config)
                spark_client.session = session

                self._static_servers[name] = spark_client

                if server_config.default and self._default_server is None:
                    self._default_server = name

            else:
                # Regular Spark REST client
                self._static_servers[name] = SparkRestClient(server_config)

                if server_config.default and self._default_server is None:
                    self._default_server = name

    def _is_cache_expired(self, entry: Dict) -> bool:
        """Check if a cache entry has expired."""
        return time.time() - entry["last_updated"] > self._cache_ttl

    def lookup_client(
        self, server_name: Optional[str] = None, app_id: Optional[str] = None
    ) -> SparkRestClient:
        """
        Look up a client by server name and/or app_id.

        Args:
            server_name: Optional server name to use
            app_id: Optional app ID for discovery

        Returns:
            SparkRestClient for the requested server/app

        Raises:
            ValueError: If no client is found
        """
        # If no server specified, use default
        if not server_name:
            if not self._default_server:
                raise ValueError("No server specified and no default configured")
            server_name = self._default_server

        # Check ephemeral providers first
        if server_name in self._ephemeral_providers:
            provider = self._ephemeral_providers[server_name]
            if app_id:
                return provider.get_client_for_app(app_id)
            else:
                # Discover apps and return client for the first one
                apps = provider.discover_apps()
                if not apps:
                    raise ValueError(f"No apps found for server '{server_name}'")
                return provider.get_client_for_app(apps[0])

        # Check static servers
        if server_name in self._static_servers:
            return self._static_servers[server_name]

        # Fallback: search static servers for app_id
        if app_id:
            # Check cache first
            if app_id in self._app_cache:
                entry = self._app_cache[app_id]
                if not self._is_cache_expired(entry):
                    cached_server = entry["server"]
                    if cached_server in self._static_servers:
                        return self._static_servers[cached_server]
                else:
                    del self._app_cache[app_id]

            # Search static servers
            for name, client in self._static_servers.items():
                try:
                    client.get_application(app_id)
                    # Cache the successful lookup
                    self._app_cache[app_id] = {
                        "server": name,
                        "last_updated": time.time(),
                    }
                    return client
                except Exception:
                    continue

        raise ValueError(f"Server '{server_name}' not found")

    def _iter_ephemeral_clients(
        self,
        server_name: str,
        provider: EphemeralClientProvider,
        status: Optional[List[str]] = None,
        min_date: Optional[str] = None,
        max_date: Optional[str] = None,
        min_end_date: Optional[str] = None,
        max_end_date: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> Iterator[Tuple[str, SparkRestClient]]:
        """Helper to iterate over ephemeral provider's clients."""
        app_ids = provider.discover_apps(
            status=status,
            min_date=min_date,
            max_date=max_date,
            min_end_date=min_end_date,
            max_end_date=max_end_date,
            limit=limit,
        )
        for app_id in app_ids:
            client = provider.get_client_for_app(app_id)
            yield f"{server_name}/{app_id}", client

    def iter_clients(
        self,
        server_name: Optional[str] = None,
        status: Optional[List[str]] = None,
        min_date: Optional[str] = None,
        max_date: Optional[str] = None,
        min_end_date: Optional[str] = None,
        max_end_date: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> Iterator[Tuple[str, SparkRestClient]]:
        """
        Iterate over clients. Filters passed through to ephemeral providers.

        Args:
            server_name: Optional server name to filter by
            status: Filter by application status (passed to ephemeral providers)
            min_date: Earliest start date (passed to ephemeral providers)
            max_date: Latest start date (passed to ephemeral providers)
            min_end_date: Earliest end date (passed to ephemeral providers)
            max_end_date: Latest end date (passed to ephemeral providers)
            limit: Maximum number of applications (passed to ephemeral providers)

        Yields:
            Tuple of (server_name, SparkRestClient)
        """

        def yield_from_server(name: str) -> Iterator[Tuple[str, SparkRestClient]]:
            if name in self._static_servers:
                yield name, self._static_servers[name]
            elif name in self._ephemeral_providers:
                yield from self._iter_ephemeral_clients(
                    name,
                    self._ephemeral_providers[name],
                    status=status,
                    min_date=min_date,
                    max_date=max_date,
                    min_end_date=min_end_date,
                    max_end_date=max_end_date,
                    limit=limit,
                )

        if server_name:
            if server_name not in self._static_servers and server_name not in self._ephemeral_providers:
                raise ValueError(f"Server '{server_name}' not found")
            yield from yield_from_server(server_name)
        else:
            for name in self._static_servers:
                yield from yield_from_server(name)
            for name in self._ephemeral_providers:
                yield from yield_from_server(name)
