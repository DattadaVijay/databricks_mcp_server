from mcp.server.fastmcp import FastMCP
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# ── Spark ─────────────────────────────────────────────────────────
spark = SparkSession.builder.getOrCreate()

# ── Windows ───────────────────────────────────────────────────────
job_window  = Window.partitionBy("job_id").orderBy(
    F.col("change_time").desc()
)
task_window = Window.partitionBy("task_key").orderBy(
    F.col("change_time").desc()
)

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
        rows = spark.table("system.lakeflow.jobs") \
                    .withColumn("rn", F.row_number().over(job_window)) \
                    .filter(F.col("rn") == 1) \
                    .filter(F.col("name") == job_name) \
                    .select("job_id") \
                    .collect()

        if not rows:
            return f"No job found with name '{job_name}'"
        return str(rows[0]["job_id"])
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
        rows = spark.table("system.lakeflow.jobs") \
                    .withColumn("rn", F.row_number().over(job_window)) \
                    .filter(F.col("rn") == 1) \
                    .filter(F.col("job_id") == job_id) \
                    .select(
                        "creator_user_name",
                        "run_as_user_name",
                        "creator_id"
                    ) \
                    .collect()

        if not rows:
            return f"No job found with ID '{job_id}'"

        row     = rows[0]
        creator = (
            row["creator_user_name"] or
            row["run_as_user_name"]  or
            row["creator_id"]        or
            "Unknown"
        )
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
        rows = spark.table("system.lakeflow.job_run_timeline") \
                    .filter(F.col("job_id") == job_id) \
                    .orderBy(F.col("period_start_time").desc()) \
                    .limit(1) \
                    .select(
                        "result_state",
                        "trigger_type",
                        "period_start_time",
                        "period_end_time",
                        "run_duration_seconds"
                    ) \
                    .collect()

        if not rows:
            return f"No runs found for job ID '{job_id}'"

        row      = rows[0]
        duration = row["run_duration_seconds"] or 0

        return (
            f"Job {job_id} latest run:\n"
            f"  Status:   {row['result_state']}\n"
            f"  Trigger:  {row['trigger_type']}\n"
            f"  Started:  {row['period_start_time']}\n"
            f"  Ended:    {row['period_end_time']}\n"
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
        rows = spark.table("system.lakeflow.job_run_timeline") \
                    .filter(F.col("job_id") == job_id) \
                    .orderBy(F.col("period_start_time").desc()) \
                    .limit(n) \
                    .select(
                        "result_state",
                        "period_start_time",
                        "run_duration_seconds",
                        "termination_code"
                    ) \
                    .collect()

        if not rows:
            return f"No run history found for job ID '{job_id}'"

        lines = [f"Last {n} runs for job {job_id}:\n"]
        for i, row in enumerate(rows, 1):
            duration = row["run_duration_seconds"] or 0
            lines.append(
                f"  Run {i}: {row['result_state']} | "
                f"Started: {row['period_start_time']} | "
                f"Duration: {duration}s | "
                f"Termination: {row['termination_code']}"
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
        cutoff  = F.now() - F.expr(f"INTERVAL {hours} HOURS")

        jobs_df = spark.table("system.lakeflow.jobs") \
                       .withColumn("rn", F.row_number().over(job_window)) \
                       .filter(F.col("rn") == 1) \
                       .select("job_id", "name")

        runs_df = spark.table("system.lakeflow.job_run_timeline") \
                       .filter(F.col("result_state").isin(
                           "ERROR", "FAILED", "TIMEDOUT"
                       )) \
                       .filter(F.col("period_start_time") >= cutoff) \
                       .orderBy(F.col("period_start_time").desc()) \
                       .select(
                           "job_id",
                           "result_state",
                           "period_start_time",
                           "termination_code"
                       )

        rows = runs_df.join(jobs_df, on="job_id", how="left").collect()

        if not rows:
            return f"No failed jobs in the last {hours} hours ✅"

        lines = [f"Failed jobs in last {hours} hours:\n"]
        for row in rows:
            lines.append(
                f"  Job:    {row['name'] or row['job_id']}\n"
                f"  State:  {row['result_state']}\n"
                f"  At:     {row['period_start_time']}\n"
                f"  Reason: {row['termination_code']}\n"
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
        rows = spark.table("system.lakeflow.job_run_timeline") \
                    .filter(F.col("job_id") == job_id) \
                    .filter(F.col("result_state").isNotNull()) \
                    .orderBy(F.col("period_start_time").desc()) \
                    .limit(1) \
                    .select(
                        "run_duration_seconds",
                        "result_state",
                        "period_start_time"
                    ) \
                    .collect()

        if not rows:
            return f"No completed runs found for job ID '{job_id}'"

        row       = rows[0]
        duration  = row["run_duration_seconds"] or 0
        compliant = duration <= expected_seconds

        return (
            f"SLA Check for job {job_id}:\n"
            f"  Expected:  <= {expected_seconds}s\n"
            f"  Actual:    {duration}s\n"
            f"  Status:    {row['result_state']}\n"
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
        rows = spark.table("system.lakeflow.job_tasks") \
                    .filter(F.col("job_id") == job_id) \
                    .filter(F.col("delete_time").isNull()) \
                    .withColumn("rn", F.row_number().over(task_window)) \
                    .filter(F.col("rn") == 1) \
                    .select("task_key", "depends_on_keys") \
                    .collect()

        if not rows:
            return f"No tasks found for job ID '{job_id}'"

        lines = [f"Tasks for job {job_id}:\n"]
        for row in rows:
            deps     = row["depends_on_keys"] or []
            deps_str = ", ".join(deps) if deps else "none"
            lines.append(
                f"  Task: {row['task_key']} | Depends on: {deps_str}"
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
        rows = spark.table("system.lakeflow.jobs") \
                    .filter(F.col("job_id") == job_id) \
                    .filter(F.col("delete_time").isNull()) \
                    .withColumn("rn", F.row_number().over(job_window)) \
                    .filter(F.col("rn") == 1) \
                    .select(
                        "trigger_type",
                        "paused",
                        F.col("trigger.schedule.quartz_cron_expression")
                         .alias("cron"),
                        F.col("trigger.schedule.timezone_id")
                         .alias("timezone")
                    ) \
                    .collect()

        if not rows:
            return f"No job found with ID '{job_id}'"

        row = rows[0]

        if not row["trigger_type"]:
            return f"Job {job_id} has no schedule (manual trigger only)"

        lines = [f"Schedule for job {job_id}:"]
        lines.append(f"  Trigger: {row['trigger_type']}")
        lines.append(f"  Paused:  {row['paused']}")
        if row["cron"]:
            lines.append(f"  Cron:    {row['cron']}")
            lines.append(f"  TZ:      {row['timezone']}")
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
        rows = spark.table("system.access.table_lineage") \
                    .filter(F.col("entity_type") == "JOB") \
                    .filter(
                        F.col("entity_metadata.job_info.job_id") == job_id
                    ) \
                    .select(
                        "source_table_full_name",
                        "target_table_full_name",
                        "created_by",
                        "event_date"
                    ) \
                    .distinct() \
                    .orderBy(F.col("event_date").desc()) \
                    .collect()

        if not rows:
            return f"No lineage found for job ID '{job_id}'"

        lines = [f"Table lineage for job {job_id}:\n"]
        for row in rows:
            lines.append(
                f"  Source: {row['source_table_full_name'] or 'N/A'} → "
                f"Target: {row['target_table_full_name'] or 'N/A'} | "
                f"By: {row['created_by']}"
            )
        return "\n".join(lines)
    except Exception as e:
        return f"Error: {str(e)}"

# ── Run ───────────────────────────────────────────────────────────
if __name__ == "__main__":
    mcp.run(transport="streamable-http")