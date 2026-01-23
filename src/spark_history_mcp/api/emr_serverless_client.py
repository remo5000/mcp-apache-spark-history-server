#!/usr/bin/env python3
"""
EMR Serverless Client

This module provides functionality to access Spark History Server for EMR Serverless
applications via the GetDashboardForJobRun API.
"""

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import List, Optional, Tuple
from urllib.parse import urlparse

import boto3
import requests
from botocore.exceptions import ClientError

from spark_history_mcp.api.spark_client import SparkRestClient
from spark_history_mcp.config.config import ServerConfig

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


@dataclass
class EMRServerlessJobRun:
    """Represents an EMR Serverless job run."""

    job_run_id: str
    application_id: str
    name: str
    state: str
    created_at: Optional[str] = None


@dataclass
class EMRServerlessClientResult:
    """Result from EMR Serverless client initialization."""

    base_url: str
    session: requests.Session
    job_run_id: str
    application_id: str


class EMRServerlessClient:
    """Client for accessing EMR Serverless Spark History Server.

    Implements the EphemeralClientProvider protocol for dynamic job run discovery.
    """

    def __init__(self, server_config: ServerConfig, max_job_runs: int = 5):
        """
        Initialize the EMR Serverless client.

        Args:
            server_config: ServerConfig with emr_serverless_application_id
            max_job_runs: Maximum number of recent job runs to discover (default: 5)
        """
        if not server_config.emr_serverless_application_id:
            raise ValueError(
                "emr_serverless_application_id is required for EMR Serverless"
            )

        self.server_config = server_config
        self.application_id = server_config.emr_serverless_application_id
        self.timeout = server_config.timeout
        self.max_job_runs = max_job_runs

    @property
    def emr_serverless_client(self):
        # Fresh client each time, picks up latest creds
        logger.info(f"Initializing EMR Serverless client: app={self.application_id}")
        return boto3.client("emr-serverless")

    def list_job_runs(
        self,
        limit: Optional[int] = None,
        states: Optional[List[str]] = None,
        include_running: bool = True,
    ) -> List[EMRServerlessJobRun]:
        """
        List job runs for the application.

        Note: Only returns jobs from the last 30 days due to AWS EMR Serverless
        PersistentAppUI limitation (dashboard unavailable for jobs >= 30 days old).

        Args:
            limit: Maximum number of job runs to return (uses max_job_runs if not specified)
            states: List of states to filter by. Defaults to ["SUCCESS", "FAILED"].
                    Valid states: SUBMITTED, PENDING, SCHEDULED, RUNNING, SUCCESS, FAILED, CANCELLING, CANCELLED
            include_running: Whether to include RUNNING jobs (default: True).

        Returns:
            List of EMRServerlessJobRun objects
        """
        limit = limit or self.max_job_runs
        if states is None:
            states = ["SUCCESS", "FAILED"]
        if include_running and "RUNNING" not in states:
            states = states + ["RUNNING"]

        logger.info(
            f"Listing job runs for application: {self.application_id} (states: {states}, max age: 30 days)"
        )

        try:
            job_runs = []
            paginator = self.emr_serverless_client.get_paginator("list_job_runs")

            # EMR Serverless PersistentAppUI is only available for jobs < 30 days old
            thirty_days_ago = datetime.now(timezone.utc) - timedelta(days=30)

            for page in paginator.paginate(
                applicationId=self.application_id,
                states=states,
                createdAtAfter=thirty_days_ago,
            ):
                for job_run in page.get("jobRuns", []):
                    job_runs.append(
                        EMRServerlessJobRun(
                            job_run_id=job_run["id"],
                            application_id=job_run["applicationId"],
                            name=job_run.get("name", ""),
                            state=job_run["state"],
                            created_at=str(job_run.get("createdAt", "")),
                        )
                    )
                    if len(job_runs) >= limit:
                        break
                if len(job_runs) >= limit:
                    break

            logger.info(f"Found {len(job_runs)} job runs")
            return job_runs

        except ClientError as e:
            error_code = e.response["Error"]["Code"]
            error_message = e.response["Error"]["Message"]
            logger.error(f"Failed to list job runs: {error_code} - {error_message}")
            raise

    def get_dashboard_url(self, job_run_id: str) -> str:
        """
        Get the dashboard URL for a specific job run.

        Args:
            job_run_id: The job run ID

        Returns:
            Dashboard URL string
        """
        logger.info(f"Getting dashboard URL for job run: {job_run_id}")

        try:
            response = self.emr_serverless_client.get_dashboard_for_job_run(
                applicationId=self.application_id,
                jobRunId=job_run_id,
            )

            url = response.get("url")
            logger.info(f"Dashboard URL obtained for job run {job_run_id}")
            return url

        except ClientError as e:
            error_code = e.response["Error"]["Code"]
            error_message = e.response["Error"]["Message"]
            logger.error(f"Failed to get dashboard URL: {error_code} - {error_message}")
            raise

    def _setup_session(self, dashboard_url: str) -> Tuple[str, requests.Session]:
        """
        Set up an HTTP session by following redirects and collecting cookies.

        Args:
            dashboard_url: The dashboard URL from get_dashboard_for_job_run

        Returns:
            Tuple of (base_url, session)
        """
        logger.info("Setting up HTTP session with cookie management")

        session = requests.Session()
        session.headers.update(
            {
                "User-Agent": "EMR-Serverless-MCP-Client/1.0",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.5",
                "Accept-Encoding": "gzip, deflate",
                "Connection": "keep-alive",
                "Upgrade-Insecure-Requests": "1",
            }
        )

        try:
            # Follow redirects to establish session and get cookies
            response = session.get(
                dashboard_url, timeout=self.timeout, allow_redirects=True
            )
            response.raise_for_status()

            # Extract base URL from final URL
            # SHS URL format (completed jobs): https://p-{job_run_id}-{app_id}.emrappui-prod.{region}.amazonaws.com/shs/history/{job_run_id}/jobs/
            # Live UI format (running jobs): https://j-{job_run_id}-*.dashboard.emr-serverless.{region}.amazonaws.com/...
            final_url = response.url
            parsed = urlparse(final_url)

            # Determine base URL based on which UI we got redirected to
            # Both UIs expose the Spark REST API, but at different paths
            if "emrappui-prod" in parsed.netloc:
                # Spark History Server (completed jobs) - API at /shs/api/v1/
                base_url = f"{parsed.scheme}://{parsed.netloc}/shs"
                logger.info("Connected to Spark History Server (completed job)")
            elif "dashboard.emr-serverless" in parsed.netloc:
                # Live Spark UI (running jobs) - API at /api/v1/
                base_url = f"{parsed.scheme}://{parsed.netloc}"
                logger.info("Connected to Live Spark UI (running job)")
            else:
                raise ValueError(
                    f"Unknown EMR Serverless dashboard URL format: {final_url}"
                )

            logger.info(f"Session established. Base URL: {base_url}")
            logger.info(f"Cookies: {len(session.cookies)} cookie(s) stored")

            return base_url, session

        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to establish HTTP session: {str(e)}")
            raise

    def initialize_for_job_run(self, job_run_id: str) -> EMRServerlessClientResult:
        """
        Initialize a client for a specific job run.

        Args:
            job_run_id: The job run ID

        Returns:
            EMRServerlessClientResult with base_url, session, and job metadata
        """
        logger.info(f"Initializing client for job run: {job_run_id}")

        # Get dashboard URL
        dashboard_url = self.get_dashboard_url(job_run_id)

        # Setup session
        base_url, session = self._setup_session(dashboard_url)

        return EMRServerlessClientResult(
            base_url=base_url,
            session=session,
            job_run_id=job_run_id,
            application_id=self.application_id,
        )

    def initialize(
        self, include_running: bool = True
    ) -> List[EMRServerlessClientResult]:
        """
        Initialize the EMR Serverless client.

        Discovers recent job runs for the application and initializes clients for each.

        Args:
            include_running: Whether to include running jobs in discovery (default: True).

        Returns:
            List of EMRServerlessClientResult objects
        """
        logger.info(f"Discovering job runs for application: {self.application_id}")
        job_runs = self.list_job_runs(include_running=include_running)

        if not job_runs:
            logger.warning(f"No job runs found for application {self.application_id}")
            return []

        # Initialize clients for each job run
        results = []
        for job_run in job_runs:
            try:
                result = self.initialize_for_job_run(job_run.job_run_id)
                results.append(result)
                logger.info(
                    f"Initialized client for job run: {job_run.job_run_id} ({job_run.name}) [{job_run.state}]"
                )
            except Exception as e:
                logger.warning(
                    f"Failed to initialize client for job run {job_run.job_run_id}: {e}"
                )
                continue

        logger.info(f"Successfully initialized {len(results)} client(s)")
        return results

    def get_client_for_app(self, app_id: str) -> SparkRestClient:
        """
        Get a SparkRestClient for a specific job run.

        Implements EphemeralClientProvider protocol. Creates a fresh client each time
        since EMR Serverless dashboard URLs are ephemeral and expire when jobs end.

        Args:
            app_id: The job run ID

        Returns:
            SparkRestClient configured for this job run
        """
        result = self.initialize_for_job_run(app_id)

        # Create a modified server config with the base URL
        emr_server_config = self.server_config.model_copy()
        emr_server_config.url = result.base_url

        # Create SparkRestClient with the session
        spark_client = SparkRestClient(emr_server_config)
        spark_client.session = result.session

        return spark_client

    def discover_apps(
        self,
        status: Optional[List[str]] = None,
        min_date: Optional[str] = None,
        max_date: Optional[str] = None,
        min_end_date: Optional[str] = None,
        max_end_date: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> List[str]:
        """
        Discover available job runs with filtering, return job run IDs.

        Implements EphemeralClientProvider protocol.

        Args:
            status: Filter by status (maps to EMR Serverless states)
            min_date: Earliest start date (not currently used by EMR API)
            max_date: Latest start date (not currently used by EMR API)
            min_end_date: Earliest end date (not currently used by EMR API)
            max_end_date: Latest end date (not currently used by EMR API)
            limit: Maximum number of job runs to return

        Returns:
            List of job run IDs
        """
        # Map generic status to EMR Serverless states
        states = None
        include_running = True
        if status:
            # Map "completed" -> SUCCESS/FAILED, "running" -> RUNNING
            states = []
            for s in status:
                if s.lower() == "completed":
                    states.extend(["SUCCESS", "FAILED"])
                elif s.lower() == "running":
                    states.append("RUNNING")
                    include_running = True
                else:
                    # Pass through EMR-specific states
                    states.append(s.upper())
            if "RUNNING" not in states:
                include_running = False

        job_runs = self.list_job_runs(
            limit=limit or self.max_job_runs,
            states=states,
            include_running=include_running,
        )

        return [job_run.job_run_id for job_run in job_runs]
