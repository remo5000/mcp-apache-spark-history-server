# Spark History Server MCP for Amazon EMR Serverless

This guide shows how to use the Spark History Server MCP with Amazon EMR Serverless applications.

## Overview

EMR Serverless runs Spark jobs without managing clusters. Each job run has its own Spark UI:
- **Completed jobs**: Spark History Server (REST API at `/shs/api/v1/`)
- **Running jobs**: Live Spark UI (REST API at `/api/v1/`)

The MCP server automatically discovers recent job runs and provides access to both UIs.

## Prerequisites

- AWS credentials configured with access to EMR Serverless
- An EMR Serverless application with job runs
- Python 3.10+

## Step 1: Setup the MCP Server

```bash
git clone https://github.com/kubeflow/mcp-apache-spark-history-server.git
cd mcp-apache-spark-history-server

# Install dependencies
pip install -e .
```

## Step 2: Configure the MCP Server

Edit `config.yaml` with your EMR Serverless application ID:

```yaml
servers:
  emr-serverless:
    default: true
    # Use the application ID (not the full ARN)
    emr_serverless_application_id: "00g1abc123xyz"
    timeout: 30

mcp:
  transports:
    - stdio
  debug: false
```

### Finding Your Application ID

Your application ID is the last segment of the application ARN:
- ARN: `arn:aws:emr-serverless:us-west-2:123456789012:/applications/00g1abc123xyz`
- Application ID: `00g1abc123xyz`

You can also find it in the EMR Studio console or via CLI:
```bash
aws emr-serverless list-applications --query 'applications[].{id:id,name:name}'
```

## Step 3: Run the MCP Server

```bash
# Using stdio transport (for Claude Code, etc.)
python -m spark_history_mcp.core.main --config config.yaml
```

## Step 4: Use with an AI Agent

### Claude Code

```bash
claude mcp add spark-history --transport stdio -- python -m spark_history_mcp.core.main --config /path/to/config.yaml
```

Then verify:
```bash
claude mcp list
```

### Claude Desktop

Add to your Claude Desktop configuration (`~/Library/Application Support/Claude/claude_desktop_config.json` on macOS):

```json
{
  "mcpServers": {
    "spark-history": {
      "command": "python",
      "args": ["-m", "spark_history_mcp.core.main", "--config", "/path/to/config.yaml"]
    }
  }
}
```

## How It Works

1. The MCP server calls `ListJobRuns` to discover job runs (SUCCESS, FAILED, and RUNNING by default)
2. For each job run, it calls `GetDashboardForJobRun` to get an authenticated dashboard URL
3. The server follows redirects to establish an HTTP session with cookies
4. Completed jobs redirect to Spark History Server; running jobs redirect to Live Spark UI
5. The MCP server creates a client for each job run and aggregates them

## Limitations

- **Session expiration**: Dashboard sessions expire after ~1 hour. Restart the MCP server to refresh.
- **Recent jobs**: By default, discovers the 5 most recent job runs.

## Required IAM Permissions

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "emr-serverless:ListJobRuns",
        "emr-serverless:GetDashboardForJobRun"
      ],
      "Resource": "arn:aws:emr-serverless:*:*:/applications/*"
    }
  ]
}
```

## Example Usage

Once configured, you can ask your AI agent:

- "List all Spark applications"
- "Show me the slowest stages in job 00g2abc123"
- "What are the bottlenecks in my most recent job?"
- "Compare the performance of my last two job runs"

## Troubleshooting

### "No job runs found"

Ensure your EMR Serverless application has at least one job run (SUCCESS, FAILED, or RUNNING).

### Session timeout

If you see authentication errors after running for a while, restart the MCP server to refresh the session tokens.
