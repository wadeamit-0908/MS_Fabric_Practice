# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "03e256ce-fa0b-4d58-8366-7c93f46ed4a6",
# META       "default_lakehouse_name": "lk_customer_billing",
# META       "default_lakehouse_workspace_id": "d08afa54-65d9-4c57-8ae5-f93fc706e510",
# META       "known_lakehouses": [
# META         {
# META           "id": "03e256ce-fa0b-4d58-8366-7c93f46ed4a6"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

%run "./common_functions"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if not log_table or not str(log_table).strip():
    log_table = "control.etl_run_log"
# Read the actual log table schema supplied for this solution.
log_df = spark.table(log_table)
required_columns = {"run_id", "pipeline_name", "task_name", "file_name", "table_name","layer", "business_date", "run_start_time", "run_end_time", 
"run_status", "total_inserted_count", 
"total_updated_count","error_message", "email_status"}
missing_columns = sorted(required_columns - set(log_df.columns))

if missing_columns:
    raise ValueError(f"{log_table} is missing expected columns: {', '.join(missing_columns)}")

run_df = log_df.filter((F.col("pipeline_name") == pipeline_name) & 
(F.to_timestamp(F.col("run_start_time")) >= F.to_timestamp(F.lit(pipeline_start_time))))
records = run_df.orderBy("run_start_time", "layer", "task_name").collect()

if not records:
    raise RuntimeError(f"No records found for pipeline={pipeline_name} " f"since {pipeline_start_time}.")
print(f"Found {len(records)} log record(s) " f"for pipeline {pipeline_name}.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def _text(value, default="N/A"):
    if value is None:
        return default
    value = str(value).strip()
    return value if value else default


def _status_category(value):
    status = _text(value, "").lower()
    if any(word in status for word in ("fail", "error", "exception")):
        return "failed"
    if any(word in status for word in ("success", "succeeded", "complete", "completed")):
        return "success"
    if any(word in status for word in ("running", "in progress", "started")):
        return "running"
    if any(word in status for word in ("cancel", "abort", "skip")):
        return "other"
    return "other"


def _format_datetime(value):
    if value is None:
        return "N/A"
    try:
        return value.strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return str(value)

# Aggregate activity-level results.
total_tasks = len(records)
success_count = 0
failed_count = 0
running_count = 0
other_count = 0
total_inserted = 0
total_updated = 0

layer_summary = {}
failed_details = []
activity_details = []
start_times = []
end_times = []

for row in records:
    status_raw = row["run_status"]
    category = _status_category(status_raw)
    layer = _text(row["layer"], "First Stage")

    if category == "success":
        success_count += 1
    elif category == "failed":
        failed_count += 1
    elif category == "running":
        running_count += 1
    else:
        other_count += 1

    inserted = int(row["total_inserted_count"] or 0)
    updated = int(row["total_updated_count"] or 0)
    total_inserted += inserted
    total_updated += updated

    if layer not in layer_summary:
        layer_summary[layer] = {"total": 0, "success": 0, "failed": 0, "running": 0, "other": 0, "inserted": 0, "updated": 0}

    layer_summary[layer]["total"] += 1
    layer_summary[layer][category] += 1
    layer_summary[layer]["inserted"] += inserted
    layer_summary[layer]["updated"] += updated

    start_value = row["run_start_time"]
    end_value = row["run_end_time"]

    if start_value is not None:
        start_times.append(start_value)
    if end_value is not None:
        end_times.append(end_value)

    detail = {
        "layer": layer,
        "task": _text(row["task_name"]),
        "file": _text(row["file_name"]),
        "table": _text(row["table_name"]),
        "status": _text(status_raw),
        "start": _format_datetime(start_value),
        "end": _format_datetime(end_value),
        "inserted": inserted,
        "updated": updated,
        "error": _text(row["error_message"], "")
    }
    activity_details.append(detail)

    if category == "failed":
        failed_details.append(detail)

# Pipeline start parameter takes precedence over activity start times.
overall_start = min(start_times) if start_times else None
overall_end = max(end_times) if end_times else None

if pipeline_start_time:
    try:
        parsed_start = datetime.fromisoformat(str(pipeline_start_time).replace("Z", "+00:00"))
        overall_start = parsed_start
    except Exception:
        print("Warning: pipeline_start_time could not be parsed; " "using earliest run_start_time from the log table.")

if overall_start and overall_end:
    # Normalize timezone awareness to avoid subtracting aware from naive datetimes.
    if getattr(overall_start, "tzinfo", None) is not None and getattr(overall_end, "tzinfo", None) is None:
        overall_start = overall_start.replace(tzinfo=None)
    elif getattr(overall_start, "tzinfo", None) is None and getattr(overall_end, "tzinfo", None) is not None:
        overall_end = overall_end.replace(tzinfo=None)

    duration_seconds = max(0, (overall_end - overall_start).total_seconds())
    hours, remainder = divmod(int(duration_seconds), 3600)
    minutes, seconds = divmod(remainder, 60)
    duration_text = f"{hours:02d}:{minutes:02d}:{seconds:02d}"
else:
    duration_text = "Not available"

if failed_count:
    overall_status = "FAILED"
elif running_count or other_count:
    overall_status = "COMPLETED WITH NON-SUCCESS TASKS"
else:
    overall_status = "SUCCESS"

print(f"Overall status: {overall_status}")
print(f"Tasks: {total_tasks}; Success: {success_count}; Failed: {failed_count}")
print(f"Inserted: {total_inserted}; Updated: {total_updated}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

subject = f"Pipeline Execution Report | {pipeline_name}"
# CUSTOMER-FACING HTML EMAIL
def esc(value):
    return html.escape(str(value if value is not None else "-"))

def status_colors(status):
    s = str(status).lower()
    if any(x in s for x in ["success", "succeeded", "complete"]):
        return "#15945D", "#E5F7EF"
    if any(x in s for x in ["fail", "error", "exception"]):
        return "#D34242", "#FFF0F0"
    if any(x in s for x in ["running", "progress", "started"]):
        return "#BA811D", "#FFF4DF"
    return "#64748B", "#EEF2F7"

def badge(value):
    fg, bg = status_colors(value)
    return (f'<span style="display:inline-block;padding:5px 10px;border-radius:14px;'
            f'background:{bg};color:{fg};border:1px solid {fg};font-size:10px;'
            f'font-weight:bold;white-space:nowrap;">{esc(value).upper()}</span>')

def section(title, subtitle, color):
    return f'''<table width="100%" cellpadding="0" cellspacing="0" style="margin:24px 0 9px;">
    <tr><td width="5" bgcolor="{color}"></td><td style="padding:4px 0 4px 11px;">
    <div style="font-size:15px;font-weight:bold;color:#25265E;">{esc(title)}</div>
    <div style="font-size:10px;color:#718096;margin-top:4px;">{esc(subtitle)}</div>
    </td></tr></table>'''

def html_table(headers, rows, status_index=None, layer_index=None):
    h = "".join(f'<th bgcolor="#343A80" style="padding:10px 7px;border:1px solid #6872B1;color:#fff;font: bold 10px Arial;text-align:left;">{esc(x)}</th>' for x in headers)
    body = ""
    for ri, row in enumerate(rows):
        bg = "#FFFFFF" if ri % 2 == 0 else "#F1F5FC"
        cells = ""
        for ci, value in enumerate(row):
            content = esc(value)
            if status_index == ci:
                content = badge(value)
            if layer_index == ci:
                layer = str(value).lower()
                colors = {"bronze": ("#FFF0D8", "#965A00"), "silver": ("#E9EDF4", "#53657B"), "gold": ("#FFF1C2", "#8A6200")}
                lbg, lfg = colors.get(layer, ("#EAF2FF", "#2865BC"))
                content = f'<span style="background:{lbg};color:{lfg};padding:4px 7px;border-radius:4px;font-weight:bold;">{esc(value).upper()}</span>'
            cells += f'<td bgcolor="{bg}" style="padding:9px 7px;border:1px solid #CBD5E1;font:10px Arial;color:#344054;vertical-align:top;line-height:15px;word-break:break-word;">{content}</td>'
        body += f"<tr>{cells}</tr>"
    return f'<table width="100%" cellpadding="0" cellspacing="0" style="border-collapse:collapse;border:1px solid #CBD5E1;margin-bottom:12px;"><thead><tr>{h}</tr></thead><tbody>{body}</tbody></table>'

# Overview details
pipeline_rows = [
    ["Pipeline Name", pipeline_name],
    ["Pipeline Run ID", pipeline_run_id],
    ["Overall Status", overall_status],
    ["Execution Start", _format_datetime(overall_start)],
    ["Execution End", _format_datetime(overall_end)],
    ["Total Duration", duration_text]
]

# KPI cards
kpis = [
    ("TOTAL ACTIVITIES", total_tasks, "#2865BC", "#EAF2FF"),
    ("SUCCESSFUL", success_count, "#15945D", "#E5F7EF"),
    ("FAILED", failed_count, "#D34242", "#FFF0F0"),
    ("INSERTED", f"{total_inserted:,}", "#8052C8", "#F3ECFF"),
    ("UPDATED", f"{total_updated:,}", "#BA811D", "#FFF4DF")
]
kpi_html = '<table width="100%" cellpadding="3" cellspacing="3"><tr>'
for label, value, fg, bg in kpis:
    kpi_html += f'<td width="20%" bgcolor="{bg}" align="center" style="padding:13px 4px;border:1px solid #DCE5EF;"><div style="font:bold 22px Arial;color:{fg};">{esc(value)}</div><div style="font:bold 9px Arial;color:{fg};margin-top:5px;">{label}</div></td>'
kpi_html += '</tr></table>'

# Layer table
layer_rows = []
for layer in sorted(layer_summary):
    s = layer_summary[layer]
    layer_rows.append([layer, s["total"], s["success"], s["failed"], s["running"], s["other"], f'{s["inserted"]:,}', f'{s["updated"]:,}'])

# All activity records in the email
activity_rows = []
for i, item in enumerate(activity_details, 1):
    activity_rows.append([i, item["layer"], item["task"], item["file"], item["table"], item["status"], item["start"], item["end"], f'{item["inserted"]:,}', f'{item["updated"]:,}', item.get("error") or "-"])

# Failed activity records
failed_rows = []
for i, item in enumerate(failed_details, 1):
    failed_rows.append([i, item["layer"], item["task"], item["file"], item["table"], item.get("error") or "No error_message recorded"])
status_fg, status_bg = status_colors(overall_status)

email_body = f'''<!DOCTYPE html><html><head><meta charset="UTF-8"></head><body style="margin:0;padding:0;background:#EDF1F6;">
<table width="100%" cellpadding="0" cellspacing="0" bgcolor="#EDF1F6"><tr><td align="center" style="padding:20px 8px;">
<table width="100%" cellpadding="0" cellspacing="0" bgcolor="#FFFFFF" style="max-width:1100px;border:1px solid #D8E1EC;">
<tr><td bgcolor="#25265E" align="center" style="padding:27px 15px;border-bottom:5px solid #3997D5;">
<div style="font:bold 24px Arial;color:#FFFFFF;margin-top:10px;">Pipeline Execution Report</div><div style="font:12px Arial;color:#D9D9FF;margin-top:7px;">{esc(pipeline_name)}</div></td></tr>
<tr><td style="padding:18px 20px 5px;"><table width="100%" bgcolor="{status_bg}" style="border:1px solid {status_fg};"><tr><td style="padding:14px;"><div style="font:bold 10px Arial;color:{status_fg};letter-spacing:1px;">OVERALL EXECUTION STATUS</div><div style="font:bold 20px Arial;color:{status_fg};margin-top:5px;">{esc(overall_status).upper()}</div><div style="font:11px Arial;color:#344054;margin-top:5px;">Total duration: {esc(duration_text)}</div></td></tr></table></td></tr>
<tr><td style="padding:5px 20px 25px;">
{section("Execution Overview", "Pipeline identification and execution timeline", "#2865BC")}
{html_table(["Information", "Details"], pipeline_rows)}
{section("Execution Performance", "At-a-glance processing metrics", "#15945D")}
{kpi_html}
{section("Layer Performance", "Bronze, Silver and Gold processing summary", "#8052C8")}
{html_table(["Layer", "Total", "Success", "Failed", "Running", "Other", "Inserted", "Updated"], layer_rows, layer_index=0)}
{section("Activity Execution Details", "Complete activity-level details for this pipeline run", "#2865BC")}
{html_table(["#", "Layer", "Task", "File", "Table", "Status", "Start Time", "End Time", "Inserted", "Updated", "Error Details"], activity_rows, status_index=5, layer_index=1)}
{section("Failed Activity Details", "Exception details requiring attention" if failed_rows else "No failed activities were recorded", "#D34242")}
{html_table(["#", "Layer", "Task", "File", "Table", "Error Details"], failed_rows, layer_index=1) if failed_rows else '<div style="padding:13px;background:#E5F7EF;border:1px solid #B9E3CE;color:#176B45;font:bold 11px Arial;">&#10003; No failed activities recorded for this execution.</div>'}
</td></tr>
<tr><td bgcolor="#25265E" align="center" style="padding:20px;"><div style="font:bold 15px Arial;color:#FFFFFF;">Thank You</div>
<div style="font:11px Arial;color:#D9D9FF;margin-top:7px;">Automated notification | Data Engineering Team For Customer Billing</div></td></tr>
</table></td></tr></table></body></html>'''

print(f"HTML email prepared. Activity rows: {len(activity_rows)}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Send one consolidated notification for this pipeline run.
# Keep the shared function signature unchanged.
send_mail_fn(subject, email_body)
print("Consolidated pipeline notification sent.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
