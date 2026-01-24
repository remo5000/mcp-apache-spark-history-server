from unittest.mock import MagicMock

import pytest

from spark_history_mcp.api.spark_client import SparkRestClient
from spark_history_mcp.config.config import Config
from spark_history_mcp.core.client_context import ClientContext


@pytest.fixture
def mock_clients():
    """Create mock SparkRestClients for testing."""
    return MagicMock(spec=SparkRestClient), MagicMock(spec=SparkRestClient)


@pytest.fixture
def mock_client_context():
    """Factory fixture to create mock ClientContext instances for testing."""

    def _create(
        static_servers=None,
        ephemeral_providers=None,
        default_server=None,
    ):
        # Create a minimal config
        mock_config = MagicMock(spec=Config)
        mock_config.servers = {}

        # Create instance bypassing __init__
        instance = object.__new__(ClientContext)
        instance._config = mock_config
        instance._static_servers = static_servers or {}
        instance._ephemeral_providers = ephemeral_providers or {}
        instance._default_server = default_server
        return instance

    return _create


class TestClientContext:
    def test_lookup_client_by_server_name(self, mock_client_context, mock_clients):
        """Test looking up a static client by server name."""
        client1, client2 = mock_clients
        ctx = mock_client_context(
            default_server="server1",
            static_servers={"server1": client1, "server2": client2},
        )

        client = ctx.lookup_client("server2")
        assert client == client2

    def test_lookup_client_default(self, mock_client_context, mock_clients):
        """Test looking up the default client."""
        client1, client2 = mock_clients
        ctx = mock_client_context(
            default_server="server1",
            static_servers={"server1": client1, "server2": client2},
        )

        client = ctx.lookup_client()
        assert client == client1

    def test_lookup_client_no_default_raises(self, mock_client_context, mock_clients):
        """Test that looking up with no server and no default raises ValueError."""
        client1, _ = mock_clients
        ctx = mock_client_context(
            default_server=None,
            static_servers={"server1": client1},
        )

        with pytest.raises(ValueError, match="No server specified and no default"):
            ctx.lookup_client()

    def test_lookup_client_not_found_raises(self, mock_client_context, mock_clients):
        """Test that looking up a non-existent server raises ValueError."""
        client1, _ = mock_clients
        ctx = mock_client_context(
            default_server="server1",
            static_servers={"server1": client1},
        )

        with pytest.raises(ValueError, match="not found"):
            ctx.lookup_client("nonexistent")

    def test_lookup_client_with_app_id_fallback(
        self, mock_client_context, mock_clients
    ):
        """Test that lookup falls back to searching by app_id in static servers."""
        client1, client2 = mock_clients
        client2.get_application.return_value = {"id": "app-123"}
        client1.get_application.side_effect = Exception("Not found")

        ctx = mock_client_context(
            default_server=None,
            static_servers={"server1": client1, "server2": client2},
        )

        # No default set, so this should raise
        with pytest.raises(ValueError):
            ctx.lookup_client(app_id="app-123")

    def test_iter_clients_all_static(self, mock_client_context, mock_clients):
        """Test iterating over all static clients."""
        client1, client2 = mock_clients
        ctx = mock_client_context(
            default_server="server1",
            static_servers={"server1": client1, "server2": client2},
        )

        clients = list(ctx.iter_clients())
        assert len(clients) == 2

        server_names = [name for name, _ in clients]
        assert "server1" in server_names
        assert "server2" in server_names

    def test_iter_clients_specific_server(self, mock_client_context, mock_clients):
        """Test iterating over a specific static server."""
        client1, client2 = mock_clients
        ctx = mock_client_context(
            default_server="server1",
            static_servers={"server1": client1, "server2": client2},
        )

        clients = list(ctx.iter_clients(server_name="server1"))
        assert len(clients) == 1
        assert clients[0][0] == "server1"
        assert clients[0][1] == client1

    def test_iter_clients_ephemeral_provider(self, mock_client_context, mock_clients):
        """Test iterating over ephemeral provider clients."""
        client1, client2 = mock_clients
        mock_provider = MagicMock()
        mock_provider.discover_apps.return_value = ["job-run-1", "job-run-2"]
        mock_provider.get_client_for_app.side_effect = [client1, client2]

        ctx = mock_client_context(
            default_server="emr",
            static_servers={},
            ephemeral_providers={"emr": mock_provider},
        )

        clients = list(ctx.iter_clients(server_name="emr"))
        assert len(clients) == 2

        mock_provider.discover_apps.assert_called_once()
        assert mock_provider.get_client_for_app.call_count == 2

        assert clients[0][0] == "emr/job-run-1"
        assert clients[1][0] == "emr/job-run-2"

    def test_lookup_client_ephemeral_with_app_id(
        self, mock_client_context, mock_clients
    ):
        """Test looking up an ephemeral client with a specific app_id."""
        client1, _ = mock_clients
        mock_provider = MagicMock()
        mock_provider.get_client_for_app.return_value = client1

        ctx = mock_client_context(
            default_server="emr",
            static_servers={},
            ephemeral_providers={"emr": mock_provider},
        )

        client = ctx.lookup_client("emr", app_id="job-run-123")
        assert client == client1
        mock_provider.get_client_for_app.assert_called_once_with("job-run-123")

    def test_lookup_client_ephemeral_without_app_id(
        self, mock_client_context, mock_clients
    ):
        """Test looking up an ephemeral client without app_id discovers first app."""
        client1, _ = mock_clients
        mock_provider = MagicMock()
        mock_provider.discover_apps.return_value = ["job-run-1", "job-run-2"]
        mock_provider.get_client_for_app.return_value = client1

        ctx = mock_client_context(
            default_server="emr",
            static_servers={},
            ephemeral_providers={"emr": mock_provider},
        )

        client = ctx.lookup_client("emr")
        assert client == client1
        mock_provider.discover_apps.assert_called_once()
        mock_provider.get_client_for_app.assert_called_once_with("job-run-1")

    def test_lookup_client_ephemeral_no_apps_raises(self, mock_client_context):
        """Test that looking up an ephemeral server with no apps raises ValueError."""
        mock_provider = MagicMock()
        mock_provider.discover_apps.return_value = []

        ctx = mock_client_context(
            default_server="emr",
            static_servers={},
            ephemeral_providers={"emr": mock_provider},
        )

        with pytest.raises(ValueError, match="No apps found"):
            ctx.lookup_client("emr")
