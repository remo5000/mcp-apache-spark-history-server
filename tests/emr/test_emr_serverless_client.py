import os
import sys
import unittest
from unittest.mock import ANY, MagicMock, patch

import requests
from botocore.exceptions import ClientError

# Add root directory to Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))


from spark_history_mcp.api.emr_serverless_client import (
    EMRServerlessClient,
)
from spark_history_mcp.config.config import ServerConfig


class TestEMRServerlessClient(unittest.TestCase):
    def setUp(self):
        """Set up test fixtures."""
        self.application_id = "00g1test1234"
        self.server_config = ServerConfig(
            emr_serverless_application_id=self.application_id, timeout=30
        )

    def test_init_with_application_id(self):
        """Test initialization with application ID."""
        client = EMRServerlessClient(self.server_config)

        self.assertEqual(client.application_id, "00g1test1234")

    @patch("spark_history_mcp.api.emr_serverless_client.boto3.client")
    def test_init_without_application_id(self, mock_boto3_client):
        """Test initialization without application ID raises error."""
        mock_client = MagicMock()
        mock_boto3_client.return_value = mock_client

        server_config = ServerConfig(timeout=30)

        with self.assertRaises(ValueError) as context:
            EMRServerlessClient(server_config)

        self.assertIn(
            "emr_serverless_application_id is required", str(context.exception)
        )

    @patch("spark_history_mcp.api.emr_serverless_client.boto3.client")
    def test_list_job_runs_success(self, mock_boto3_client):
        """Test successful listing of job runs (includes running by default)."""
        mock_client = MagicMock()
        mock_boto3_client.return_value = mock_client

        # Mock paginator
        mock_paginator = MagicMock()
        mock_client.get_paginator.return_value = mock_paginator
        mock_paginator.paginate.return_value = [
            {
                "jobRuns": [
                    {
                        "id": "job-run-1",
                        "applicationId": "00g1test1234",
                        "name": "test-job-1",
                        "state": "SUCCESS",
                        "createdAt": "2025-01-01T00:00:00Z",
                    },
                    {
                        "id": "job-run-2",
                        "applicationId": "00g1test1234",
                        "name": "test-job-2",
                        "state": "FAILED",
                        "createdAt": "2025-01-02T00:00:00Z",
                    },
                ]
            }
        ]

        client = EMRServerlessClient(self.server_config)
        job_runs = client.list_job_runs(limit=5)

        self.assertEqual(len(job_runs), 2)
        self.assertEqual(job_runs[0].job_run_id, "job-run-1")
        self.assertEqual(job_runs[0].name, "test-job-1")
        self.assertEqual(job_runs[0].state, "SUCCESS")
        self.assertEqual(job_runs[1].job_run_id, "job-run-2")
        self.assertEqual(job_runs[1].state, "FAILED")

        mock_client.get_paginator.assert_called_once_with("list_job_runs")
        # Default now includes RUNNING
        mock_paginator.paginate.assert_called_once_with(
            applicationId="00g1test1234",
            states=["SUCCESS", "FAILED", "RUNNING"],
            createdAtAfter=ANY,
        )

    @patch("spark_history_mcp.api.emr_serverless_client.boto3.client")
    def test_list_job_runs_include_running(self, mock_boto3_client):
        """Test listing job runs including running jobs."""
        mock_client = MagicMock()
        mock_boto3_client.return_value = mock_client

        # Mock paginator
        mock_paginator = MagicMock()
        mock_client.get_paginator.return_value = mock_paginator
        mock_paginator.paginate.return_value = [
            {
                "jobRuns": [
                    {
                        "id": "job-run-1",
                        "applicationId": "00g1test1234",
                        "name": "test-job-1",
                        "state": "RUNNING",
                        "createdAt": "2025-01-01T00:00:00Z",
                    },
                ]
            }
        ]

        client = EMRServerlessClient(self.server_config)
        job_runs = client.list_job_runs(include_running=True)

        self.assertEqual(len(job_runs), 1)
        self.assertEqual(job_runs[0].state, "RUNNING")

        mock_paginator.paginate.assert_called_once_with(
            applicationId="00g1test1234",
            states=["SUCCESS", "FAILED", "RUNNING"],
            createdAtAfter=ANY,
        )

    @patch("spark_history_mcp.api.emr_serverless_client.boto3.client")
    def test_list_job_runs_client_error(self, mock_boto3_client):
        """Test handling of ClientError during job run listing."""
        mock_client = MagicMock()
        mock_boto3_client.return_value = mock_client

        error_response = {
            "Error": {"Code": "ValidationException", "Message": "Invalid application"}
        }
        mock_client.get_paginator.side_effect = ClientError(
            error_response, "ListJobRuns"
        )

        client = EMRServerlessClient(self.server_config)

        with self.assertRaises(ClientError) as context:
            client.list_job_runs()

        self.assertEqual(
            context.exception.response["Error"]["Code"], "ValidationException"
        )

    @patch("spark_history_mcp.api.emr_serverless_client.boto3.client")
    def test_get_dashboard_url_success(self, mock_boto3_client):
        """Test successful retrieval of dashboard URL."""
        mock_client = MagicMock()
        mock_boto3_client.return_value = mock_client
        mock_client.get_dashboard_for_job_run.return_value = {
            "url": "https://example.com/dashboard?authToken=test123"
        }

        client = EMRServerlessClient(self.server_config)
        url = client.get_dashboard_url("job-run-1")

        self.assertEqual(url, "https://example.com/dashboard?authToken=test123")
        mock_client.get_dashboard_for_job_run.assert_called_once_with(
            applicationId="00g1test1234",
            jobRunId="job-run-1",
        )

    @patch("spark_history_mcp.api.emr_serverless_client.boto3.client")
    def test_get_dashboard_url_client_error(self, mock_boto3_client):
        """Test handling of ClientError during dashboard URL retrieval."""
        mock_client = MagicMock()
        mock_boto3_client.return_value = mock_client

        error_response = {
            "Error": {"Code": "ResourceNotFoundException", "Message": "Job not found"}
        }
        mock_client.get_dashboard_for_job_run.side_effect = ClientError(
            error_response, "GetDashboardForJobRun"
        )

        client = EMRServerlessClient(self.server_config)

        with self.assertRaises(ClientError) as context:
            client.get_dashboard_url("invalid-job")

        self.assertEqual(
            context.exception.response["Error"]["Code"], "ResourceNotFoundException"
        )

    @patch("spark_history_mcp.api.emr_serverless_client.boto3.client")
    def test_setup_session_success_shs(self, mock_boto3_client):
        """Test successful session setup with SHS URL."""
        mock_boto3_client.return_value = MagicMock()

        client = EMRServerlessClient(self.server_config)

        # Mock the session.get to return SHS URL
        with patch.object(requests.Session, "get") as mock_get:
            mock_response = MagicMock()
            mock_response.url = "https://p-jobrun1-app1.emrappui-prod.us-west-2.amazonaws.com/shs/history/jobrun1/jobs/"
            mock_response.raise_for_status.return_value = None
            mock_get.return_value = mock_response

            base_url, session = client._setup_session(
                "https://example.com/dashboard?authToken=test"
            )

            self.assertEqual(
                base_url,
                "https://p-jobrun1-app1.emrappui-prod.us-west-2.amazonaws.com/shs",
            )
            self.assertIsInstance(session, requests.Session)

    @patch("spark_history_mcp.api.emr_serverless_client.boto3.client")
    def test_setup_session_success_live_ui(self, mock_boto3_client):
        """Test successful session setup with Live UI URL (running jobs)."""
        mock_boto3_client.return_value = MagicMock()

        client = EMRServerlessClient(self.server_config)

        # Mock the session.get to return Live UI URL
        with patch.object(requests.Session, "get") as mock_get:
            mock_response = MagicMock()
            mock_response.url = (
                "https://j-jobrun1-1.dashboard.emr-serverless.us-west-2.amazonaws.com/"
            )
            mock_response.raise_for_status.return_value = None
            mock_get.return_value = mock_response

            base_url, session = client._setup_session(
                "https://example.com/dashboard?authToken=test"
            )

            # Live UI base URL should NOT have /shs prefix
            self.assertEqual(
                base_url,
                "https://j-jobrun1-1.dashboard.emr-serverless.us-west-2.amazonaws.com",
            )
            self.assertIsInstance(session, requests.Session)

    @patch("spark_history_mcp.api.emr_serverless_client.boto3.client")
    def test_setup_session_unknown_url_error(self, mock_boto3_client):
        """Test session setup fails with unknown URL format."""
        mock_boto3_client.return_value = MagicMock()

        client = EMRServerlessClient(self.server_config)

        # Mock the session.get to return an unknown URL format
        with patch.object(requests.Session, "get") as mock_get:
            mock_response = MagicMock()
            mock_response.url = "https://unknown.example.com/some/path"
            mock_response.raise_for_status.return_value = None
            mock_get.return_value = mock_response

            with self.assertRaises(ValueError) as context:
                client._setup_session("https://example.com/dashboard?authToken=test")

            self.assertIn(
                "Unknown EMR Serverless dashboard URL format",
                str(context.exception),
            )

    @patch("spark_history_mcp.api.emr_serverless_client.boto3.client")
    def test_initialize_with_application_id_discovery(self, mock_boto3_client):
        """Test initialization with application ID discovers job runs."""
        mock_client = MagicMock()
        mock_boto3_client.return_value = mock_client

        # Mock list_job_runs paginator
        mock_paginator = MagicMock()
        mock_client.get_paginator.return_value = mock_paginator
        mock_paginator.paginate.return_value = [
            {
                "jobRuns": [
                    {
                        "id": "job-run-1",
                        "applicationId": "00g1test1234",
                        "name": "test-job-1",
                        "state": "SUCCESS",
                    },
                    {
                        "id": "job-run-2",
                        "applicationId": "00g1test1234",
                        "name": "test-job-2",
                        "state": "SUCCESS",
                    },
                ]
            }
        ]

        # Mock get_dashboard_for_job_run
        mock_client.get_dashboard_for_job_run.return_value = {
            "url": "https://example.com/dashboard?authToken=test123"
        }

        client = EMRServerlessClient(self.server_config, max_job_runs=2)

        # Mock _setup_session
        with patch.object(client, "_setup_session") as mock_setup:
            mock_session = MagicMock()
            mock_setup.return_value = (
                "https://p-test.emrappui-prod.us-west-2.amazonaws.com/shs",
                mock_session,
            )

            results = client.initialize()

            self.assertEqual(len(results), 2)
            self.assertEqual(results[0].job_run_id, "job-run-1")
            self.assertEqual(results[1].job_run_id, "job-run-2")

    @patch("spark_history_mcp.api.emr_serverless_client.boto3.client")
    def test_initialize_skips_failed_jobs(self, mock_boto3_client):
        """Test initialization skips jobs that fail to initialize."""
        mock_client = MagicMock()
        mock_boto3_client.return_value = mock_client

        # Mock list_job_runs paginator
        mock_paginator = MagicMock()
        mock_client.get_paginator.return_value = mock_paginator
        mock_paginator.paginate.return_value = [
            {
                "jobRuns": [
                    {
                        "id": "job-run-1",
                        "applicationId": "00g1test1234",
                        "name": "test-job-1",
                        "state": "SUCCESS",
                    },
                    {
                        "id": "job-run-2",
                        "applicationId": "00g1test1234",
                        "name": "test-job-2",
                        "state": "SUCCESS",
                    },
                ]
            }
        ]

        # Mock get_dashboard_for_job_run
        mock_client.get_dashboard_for_job_run.return_value = {
            "url": "https://example.com/dashboard?authToken=test123"
        }

        client = EMRServerlessClient(self.server_config, max_job_runs=2)

        # Mock _setup_session to fail for first job, succeed for second
        with patch.object(client, "_setup_session") as mock_setup:
            mock_session = MagicMock()
            mock_setup.side_effect = [
                ValueError("Job redirected to Live UI"),
                (
                    "https://p-test.emrappui-prod.us-west-2.amazonaws.com/shs",
                    mock_session,
                ),
            ]

            results = client.initialize()

            # Only second job should succeed
            self.assertEqual(len(results), 1)
            self.assertEqual(results[0].job_run_id, "job-run-2")

    @patch("spark_history_mcp.api.emr_serverless_client.boto3.client")
    def test_initialize_empty_job_runs(self, mock_boto3_client):
        """Test initialization with no completed job runs."""
        mock_client = MagicMock()
        mock_boto3_client.return_value = mock_client

        # Mock list_job_runs paginator to return empty
        mock_paginator = MagicMock()
        mock_client.get_paginator.return_value = mock_paginator
        mock_paginator.paginate.return_value = [{"jobRuns": []}]

        client = EMRServerlessClient(self.server_config)
        results = client.initialize()

        self.assertEqual(len(results), 0)


if __name__ == "__main__":
    unittest.main()
