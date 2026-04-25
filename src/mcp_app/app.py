from mcp.server.fastmcp import FastMCP
from databricks import sql
import os

# ── Connection ────────────────────────────────────────────────────
def get_connection():
    return sql.connect(
        server_hostname = os.environ.get("DATABRICKS_HOST", "").replace("https://", ""),
        http_path       = os.environ.get("DATABRICKS_HTTP_PATH"),
        access_token    = os.environ.get("DATABRICKS_TOKEN")
    )

def run_query(query: str) -> list:
    with get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(query)
            return cursor.fetchall()

# ── MCP server ────────────────────────────────────────────────────
mcp = FastMCP("Databricks Governance")

# ── Tool 1 — get_job_id ───────────────────────────────────────────
@mcp.tool()
def get_job_id(job_name: str) -> str:
    """
    Gets the Databricks job ID for a given job name.
    Pass the EXACT full job name including brackets and special characters.

    Args:
        job_name: exact full name of the job
    """
    try:
        rows = run_query(f"""
            SELECT job_id
            FROM system.lakeflow.jobs
            WHERE name = '{job_name}'
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY job_id ORDER BY change_time DESC
            ) = 1
        """)

        if not rows:
            return f"No job found with name '{job_name}'"
        return str(rows[0][0])
    except Exception as e:
        return f"Error: {str(e)}"

# ── Tool 2 — get_job_creator ──────────────────────────────────────
@mcp.tool()
def get_job_creator(job_id: str) -> str:
    """
    Gets the creator of a Databricks job given its job ID.
    If you only have a job name use get_job_id first.

    Args:
        job_id: the Databricks job ID
    """
    try:
        rows = run_query(f"""
            SELECT creator_user_name, run_as_user_name, creator_id
            FROM system.lakeflow.jobs
            WHERE job_id = '{job_id}'
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY job_id ORDER BY change_time DESC
            ) = 1
        """)

        if not rows:
            return f"No job found with ID '{job_id}'"

        row     = rows[0]
        creator = row[0] or row[1] or row[2] or "Unknown"
        return f"Job {job_id} was created by: {creator}"
    except Exception as e:
        return f"Error: {str(e)}"

# ── Tool 3 — get_job_status ───────────────────────────────────────
@mcp.tool()
def get_job_status(job_id: str) -> str:
    """
    Gets the latest run status of a Databricks job.

    Args:
        job_id: the Databricks job ID
    """
    try:
        rows = run_query(f"""
            SELECT
                result_state,
                trigger_type,
                period_start_time,
                period_end_time,
                run_duration_seconds
            FROM system.lakeflow.job_run_timeline
            WHERE job_id = '{job_id}'
            ORDER BY period_start_time DESC
            LIMIT 1
        """)

        if not rows:
            return f"No runs found for job ID '{job_id}'"

        row      = rows[0]
        duration = row[4] or 0

        return (
            f"Job {job_id} latest run:\n"
            f"  Status:   {row[0]}\n"
            f"  Trigger:  {row[1]}\n"
            f"  Started:  {row[2]}\n"
            f"  Ended:    {row[3]}\n"
            f"  Duration: {duration}s"
        )
    except Exception as e:
        return f"Error: {str(e)}"

# ── Tool 4 — get_job_run_history ──────────────────────────────────
@mcp.tool()
def get_job_run_history(job_id: str, n: int = 5) -> str:
    """
    Gets the last N runs of a Databricks job.

    Args:
        job_id: the Databricks job ID
        n: number of recent runs to return (default 5)
    """
    try:
        rows = run_query(f"""
            SELECT
                result_state,
                period_start_time,
                run_duration_seconds,
                termination_code
            FROM system.lakeflow.job_run_timeline
            WHERE job_id = '{job_id}'
            ORDER BY period_start_time DESC
            LIMIT {n}
        """)

        if not rows:
            return f"No run history found for job ID '{job_id}'"

        lines = [f"Last {n} runs for job {job_id}:\n"]
        for i, row in enumerate(rows, 1):
            duration = row[2] or 0
            lines.append(
                f"  Run {i}: {row[0]} | "
                f"Started: {row[1]} | "
                f"Duration: {duration}s | "
                f"Termination: {row[3]}"
            )
        return "\n".join(lines)
    except Exception as e:
        return f"Error: {str(e)}"

# ── Tool 5 — get_failed_jobs ──────────────────────────────────────
@mcp.tool()
def get_failed_jobs(hours: int = 24) -> str:
    """
    Returns all jobs that failed in the last N hours.

    Args:
        hours: how many hours to look back (default 24)
    """
    try:
        rows = run_query(f"""
            SELECT
                r.job_id,
                j.name,
                r.result_state,
                r.period_start_time,
                r.termination_code
            FROM system.lakeflow.job_run_timeline r
            LEFT JOIN (
                SELECT job_id, name
                FROM system.lakeflow.jobs
                QUALIFY ROW_NUMBER() OVER (
                    PARTITION BY job_id ORDER BY change_time DESC
                ) = 1
            ) j ON r.job_id = j.job_id
            WHERE r.result_state IN ('ERROR', 'FAILED', 'TIMEDOUT')
            AND r.period_start_time >= NOW() - INTERVAL {hours} HOURS
            ORDER BY r.period_start_time DESC
        """)

        if not rows:
            return f"No failed jobs in the last {hours} hours ✅"

        lines = [f"Failed jobs in last {hours} hours:\n"]
        for row in rows:
            lines.append(
                f"  Job:    {row[1] or row[0]}\n"
                f"  State:  {row[2]}\n"
                f"  At:     {row[3]}\n"
                f"  Reason: {row[4]}\n"
            )
        return "\n".join(lines)
    except Exception as e:
        return f"Error: {str(e)}"

# ── Tool 6 — check_job_sla ────────────────────────────────────────
@mcp.tool()
def check_job_sla(job_id: str, expected_seconds: int) -> str:
    """
    Checks if the latest run of a job completed within SLA.

    Args:
        job_id: the Databricks job ID
        expected_seconds: maximum acceptable run duration in seconds
    """
    try:
        rows = run_query(f"""
            SELECT run_duration_seconds, result_state, period_start_time
            FROM system.lakeflow.job_run_timeline
            WHERE job_id = '{job_id}'
            AND result_state IS NOT NULL
            ORDER BY period_start_time DESC
            LIMIT 1
        """)

        if not rows:
            return f"No completed runs found for job ID '{job_id}'"

        row       = rows[0]
        duration  = row[0] or 0
        compliant = duration <= expected_seconds

        return (
            f"SLA Check for job {job_id}:\n"
            f"  Expected:  <= {expected_seconds}s\n"
            f"  Actual:    {duration}s\n"
            f"  Status:    {row[1]}\n"
            f"  SLA:       {'✅ COMPLIANT' if compliant else '❌ BREACHED'}"
        )
    except Exception as e:
        return f"Error: {str(e)}"

# ── Tool 7 — get_job_tasks ────────────────────────────────────────
@mcp.tool()
def get_job_tasks(job_id: str) -> str:
    """
    Gets all tasks for a Databricks job and their dependencies.

    Args:
        job_id: the Databricks job ID
    """
    try:
        rows = run_query(f"""
            SELECT task_key, depends_on_keys
            FROM system.lakeflow.job_tasks
            WHERE job_id = '{job_id}'
            AND delete_time IS NULL
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY task_key ORDER BY change_time DESC
            ) = 1
        """)

        if not rows:
            return f"No tasks found for job ID '{job_id}'"

        lines = [f"Tasks for job {job_id}:\n"]
        for row in rows:
            lines.append(
                f"  Task: {row[0]} | Depends on: {row[1] or 'none'}"
            )
        return "\n".join(lines)
    except Exception as e:
        return f"Error: {str(e)}"

# ── Tool 8 — get_job_schedule ─────────────────────────────────────
@mcp.tool()
def get_job_schedule(job_id: str) -> str:
    """
    Gets the schedule configuration for a Databricks job.

    Args:
        job_id: the Databricks job ID
    """
    try:
        rows = run_query(f"""
            SELECT
                trigger_type,
                paused,
                trigger.schedule.quartz_cron_expression AS cron,
                trigger.schedule.timezone_id AS timezone
            FROM system.lakeflow.jobs
            WHERE job_id = '{job_id}'
            AND delete_time IS NULL
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY job_id ORDER BY change_time DESC
            ) = 1
        """)

        if not rows:
            return f"No job found with ID '{job_id}'"

        row = rows[0]

        if not row[0]:
            return f"Job {job_id} has no schedule (manual trigger only)"

        lines = [f"Schedule for job {job_id}:"]
        lines.append(f"  Trigger: {row[0]}")
        lines.append(f"  Paused:  {row[1]}")
        if row[2]:
            lines.append(f"  Cron:    {row[2]}")
            lines.append(f"  TZ:      {row[3]}")
        return "\n".join(lines)
    except Exception as e:
        return f"Error: {str(e)}"

# ── Tool 9 — get_table_lineage ────────────────────────────────────
@mcp.tool()
def get_table_lineage(job_id: str) -> str:
    """
    Gets which tables a Databricks job reads from and writes to.

    Args:
        job_id: the Databricks job ID
    """
    try:
        rows = run_query(f"""
            SELECT DISTINCT
                source_table_full_name,
                target_table_full_name,
                created_by,
                event_date
            FROM system.access.table_lineage
            WHERE entity_type = 'JOB'
            AND entity_metadata.job_info.job_id = '{job_id}'
            ORDER BY event_date DESC
        """)

        if not rows:
            return f"No lineage found for job ID '{job_id}'"

        lines = [f"Table lineage for job {job_id}:\n"]
        for row in rows:
            lines.append(
                f"  Source: {row[0] or 'N/A'} → "
                f"Target: {row[1] or 'N/A'} | "
                f"By: {row[2]}"
            )
        return "\n".join(lines)
    except Exception as e:
        return f"Error: {str(e)}"

# ── Run ───────────────────────────────────────────────────────────
if __name__ == "__main__":
    mcp.run(transport="streamable-http")