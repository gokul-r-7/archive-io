#!/usr/bin/env python3
"""
report_generation_io_part1.py

Generates validation reports (summary & failure-only) for source-target tables.
Refactored to avoid plagiarism while keeping all functionality intact.
"""

import boto3
import json
import re
import ast
from io import BytesIO
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, PageBreak
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.pagesizes import letter
from reportlab.lib import colors
from reportlab.lib.colors import HexColor

# ---------------------------
# Constants & Clients
# ---------------------------
S3_CLIENT = boto3.client("s3")
ATHENA_CLIENT = boto3.client("athena")
BUCKET_NAME = "archival-io-227"
BASE_PREFIX = "validation_report"
ATHENA_DATABASE = "validation_reports"
ATHENA_TABLE = "validation_reports"
ATHENA_OUTPUT = "s3://archival-io-227/athena_results/"
AWS_REGION = "us-east-1"

# ---------------------------
# PDF Styles
# ---------------------------
BASE_STYLES = getSampleStyleSheet()

TITLE_STYLE = ParagraphStyle(
    "ReportTitle",
    parent=BASE_STYLES["Heading1"],
    fontName="Helvetica-Bold",
    fontSize=20,
    textColor=HexColor("#003366"),
    alignment=1,
    spaceAfter=24
)

SECTION_STYLE = ParagraphStyle(
    "SectionHeader",
    parent=BASE_STYLES["Heading2"],
    fontName="Helvetica-Bold",
    fontSize=13,
    textColor=HexColor("#003366"),
    spaceBefore=16,
    spaceAfter=10
)

TEXT_STYLE = ParagraphStyle(
    "BodyTextCustom",
    parent=BASE_STYLES["BodyText"],
    fontName="Helvetica",
    fontSize=9.5,
    textColor=HexColor("#000000"),
    leading=13,
    spaceAfter=6
)

# ---------------------------
# PDF Utilities
# ---------------------------
def add_page_header_footer(canvas, doc):
    canvas.saveState()
    canvas.setFont("Helvetica", 8)
    canvas.drawString(40, 25, "Validation Report - Confidential")
    canvas.drawRightString(570, 25, f"Page {doc.page}")
    canvas.restoreState()

def empty_page_footer(canvas, doc):
    pass  # No header/footer for cover page

def draw_horizontal_line(width=500):
    tbl = Table([[""]], colWidths=[width])
    tbl.setStyle(TableStyle([("LINEBELOW", (0, 0), (-1, -1), 1, HexColor("#888888"))]))
    return tbl

def format_table(tbl):
    tbl.setStyle(TableStyle([
        ("FONTNAME", (0, 0), (-1, -1), "Helvetica"),
        ("FONTSIZE", (0, 0), (-1, -1), 8),
        ("TEXTCOLOR", (0, 1), (-1, -1), HexColor("#000000")),
        ("BACKGROUND", (0, 0), (-1, 0), colors.black),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("ALIGN", (0, 0), (-1, -1), "CENTER"),
        ("BOTTOMPADDING", (0, 0), (-1, 0), 6),
        ("TOPPADDING", (0, 0), (-1, 0), 6),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [HexColor("#E8E8E8"), HexColor("#F5F5F5")]),
        ("GRID", (0, 0), (-1, -1), 0.25, HexColor("#999999")),
    ]))
    return tbl

def colorize_result(value):
    val_upper = str(value).upper()
    if val_upper in ("PASSED", "SUCCEEDED"):
        return f'<font color="#006400"><b>{val_upper}</b></font>'
    if val_upper in ("FAILED", "ERROR"):
        return f'<font color="#B22222"><b>{val_upper}</b></font>'
    return val_upper

# ---------------------------
# S3 & Metadata Helpers
# ---------------------------
def fetch_validation_json_records(database, table):
    prefix = f"{BASE_PREFIX}/{database}/{table}/validation_summary/json/"
    records = []
    paginator = S3_CLIENT.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=BUCKET_NAME, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith(".json"):
                body = S3_CLIENT.get_object(Bucket=BUCKET_NAME, Key=key)["Body"].read().decode("utf-8")
                for line in body.strip().split("\n"):
                    try:
                        records.append(json.loads(line.strip()))
                    except json.JSONDecodeError:
                        print(f"⚠️ Skipping malformed line in {key}")
    return records

def filter_jobs_by_table(jobs, table, key="--target_table"):
    return [job for job in jobs if job["script_args"].get(key) == table]

def group_records_by_validation_type(records):
    return {r["validation_report"]: r for r in records}


# ---------------------------
# PDF Sections
# ---------------------------
def add_row_count_section(story, rc):
    story.append(Paragraph("ROW COUNT VALIDATION", SECTION_STYLE))
    if rc:
        match = re.search(r"source_table_count: (\d+), target_table_count: (\d+)", rc["value"])
        if match:
            src, tgt = map(int, match.groups())
            diff = abs(src - tgt)
            tbl = Table([["Source Count", "Target Count", "Difference", "Result"],
                         [src, tgt, diff, Paragraph(colorize_result(rc["result"]), TEXT_STYLE)]], colWidths=[120]*4)
            format_table(tbl)
            story.append(tbl)
    story.append(Spacer(1, 12))

def add_schema_section(story, sc):
    story.append(Paragraph("SCHEMA VALIDATION", SECTION_STYLE))
    if sc:
        match = re.search(r"source_columns: \[(.*?)\], target_columns: \[(.*?)\]", sc["value"])
        if match:
            src_cols = [c.strip("' ") for c in match.group(1).split(",")]
            tgt_cols = [c.strip("' ") for c in match.group(2).split(",")]
            all_cols = sorted(set(src_cols) | set(tgt_cols))
            data = [["Source Col", "Target Col", "Difference", "Result"]]
            for col in all_cols:
                diff = "-" if col in src_cols and col in tgt_cols else col
                res = "PASSED" if col in src_cols and col in tgt_cols else "FAILED"
                data.append([col if col in src_cols else "",
                             col if col in tgt_cols else "",
                             diff,
                             Paragraph(colorize_result(res), TEXT_STYLE)])
            tbl = Table(data, colWidths=[140, 140, 140, 80])
            format_table(tbl)
            story.append(tbl)
    story.append(Spacer(1, 12))

def add_column_checksum_section(story, cc):
    story.append(Paragraph("COLUMN CHECKSUM VALIDATION", SECTION_STYLE))
    if cc and "value" in cc:
        src_cs, tgt_cs = {}, {}
        smatch = re.search(r"source_column_checksum: \[(.*?)\]", cc["value"])
        tmatch = re.search(r"target_column_checksum: \[(.*?)\]", cc["value"])
        if smatch:
            for item in smatch.group(1).split(", "):
                if ":" in item:
                    col, val = item.split(":", 1)
                    src_cs[col.strip()] = val.strip()
        if tmatch:
            for item in tmatch.group(1).split(", "):
                if ":" in item:
                    col, val = item.split(":", 1)
                    tgt_cs[col.strip()] = val.strip()
        cols = sorted(set(src_cs) | set(tgt_cs))
        data = [["Source Column", "Target Column", "Mismatch", "Result"]]
        for c in cols:
            s_val = src_cs.get(c, "N/A")
            t_val = tgt_cs.get(c, "N/A")
            mismatch = c if s_val != t_val else "-"
            res = "FAILED" if s_val != t_val else "PASSED"
            data.append([c, c, mismatch, Paragraph(colorize_result(res), TEXT_STYLE)])
            data.append([s_val, t_val, "" if mismatch == "-" else mismatch, ""])
        tbl = Table(data, colWidths=[140, 140, 140, 80])
        format_table(tbl)
        story.append(tbl)
    story.append(Spacer(1, 12))

# ---------------------------
# ROW CHECKSUM SECTION
# ---------------------------
def add_row_checksum_section(story, rcsh):
    story.append(Paragraph("ROW CHECKSUM VALIDATION", SECTION_STYLE))
    if rcsh:
        remarks = rcsh.get("remarks", "")
        records_match = re.search(r"Records:\s*(\[.*\])", remarks)
        record_list = []
        if records_match:
            try:
                record_list = ast.literal_eval(records_match.group(1))
            except Exception as e:
                story.append(Paragraph(f"Error parsing row mismatches: {e}", TEXT_STYLE))

        validated_match = re.search(r"row checksums validated: (\d+)", rcsh.get("value", ""))
        mismatch_match = re.search(r"Mismatched checksums: (\d+) rows \(([\d.]+)%\)", remarks)

        validated_count = int(validated_match.group(1)) if validated_match else 0
        mismatch_count = int(mismatch_match.group(1)) if mismatch_match else len(record_list)
        mismatch_percent = float(mismatch_match.group(2)) if mismatch_match else 0.0
        result = "FAILED" if mismatch_count > 0 else "PASSED"

        summary_data = [
            ["ROW CHECKSUM VALIDATED COUNT", "ROW CHECKSUM MISMATCH COUNT", "ROW CHECKSUM MISMATCH %", "RESULT"],
            [validated_count, mismatch_count, f"{mismatch_percent}%", Paragraph(colorize_result(result), TEXT_STYLE)],
        ]
        summary_table = Table(summary_data, colWidths=[160, 160, 160, 80])
        format_table(summary_table)
        story.append(summary_table)
        story.append(Spacer(1, 12))

        if record_list:
            story.append(Paragraph("ROW CHECKSUM MISMATCH RECORDS", SECTION_STYLE))
            for i in range(0, len(record_list), 2):
                src_data, tgt_data = {}, {}
                rec1 = record_list[i]
                rec2 = record_list[i+1] if i+1 < len(record_list) else {}

                if "source_row" in rec1: src_data = rec1["source_row"]
                if "target_row" in rec1: tgt_data = rec1["target_row"]
                if "source_row" in rec2: src_data = rec2["source_row"]
                if "target_row" in rec2: tgt_data = rec2["target_row"]

                src_r = src_data.get("record", {}) if src_data else {}
                tgt_r = tgt_data.get("record", {}) if tgt_data else {}
                s_ck = src_data.get("checksum", "") if src_data else ""
                t_ck = tgt_data.get("checksum", "") if tgt_data else ""

                cols = sorted(set(src_r.keys()) | set(tgt_r.keys()))
                rows = [["COLUMN", "SOURCE ROW VALUE", "TARGET ROW VALUE"]]
                for c in cols:
                    rows.append([c, str(src_r.get(c, "")), str(tgt_r.get(c, ""))])
                rows.append(["CHECKSUM", s_ck, t_ck])

                tbl = Table(rows, colWidths=[90, 235, 235])
                format_table(tbl)
                story.append(tbl)
                story.append(Spacer(1, 6))

    else:
        story.append(Paragraph("⚠️ No row checksum data found.", TEXT_STYLE))

    story.append(Spacer(1, 24))
    story.append(draw_horizontal_line())
    story.append(Spacer(1, 12))

# ---------------------------
# TABLE PROCESSING
# ---------------------------
def process_table_report(story, src_table, tgt_db, tgt_table, val_records, arch_jobs, val_jobs):
    story.append(PageBreak())
    story.append(draw_horizontal_line())
    story.append(Spacer(1, 6))
    story.append(Paragraph("VALIDATION REPORT", TITLE_STYLE))
    story.append(Paragraph(f"<b>SOURCE TABLE:</b> {src_table}", TEXT_STYLE))
    story.append(Paragraph(f"<b>TARGET TABLE:</b> {tgt_db}.{tgt_table}", TEXT_STYLE))
    story.append(Spacer(1, 12))

    story.append(Paragraph("ARCHIVAL JOB", SECTION_STYLE))
    for job in arch_jobs:
        story.append(Paragraph(f"<b>JOB NAME:</b> {job['job_name']}", TEXT_STYLE))
        story.append(Paragraph(f"<b>JOB RUN ID:</b> {job['job_run_id']}", TEXT_STYLE))
        story.append(Paragraph(f"<b>JOB RUN TIME (SEC):</b> {job['duration_seconds']}", TEXT_STYLE))
        story.append(Paragraph(f"<b>JOB STATUS:</b> {colorize_result(job['status'])}", TEXT_STYLE))
        story.append(Spacer(1, 6))

    story.append(Paragraph("VALIDATION JOB", SECTION_STYLE))
    for job in val_jobs:
        story.append(Paragraph(f"<b>JOB NAME:</b> {job['job_name']}", TEXT_STYLE))
        story.append(Paragraph(f"<b>JOB RUN ID:</b> {job['job_run_id']}", TEXT_STYLE))
        story.append(Paragraph(f"<b>JOB RUN TIME (SEC):</b> {job['duration_seconds']}", TEXT_STYLE))
        story.append(Paragraph(f"<b>JOB STATUS:</b> {colorize_result(job['status'])}", TEXT_STYLE))
        story.append(Spacer(1, 6))

    validations = group_records_by_validation_type(val_records)
    add_row_count_section(story, validations.get("row_count_validation"))
    add_schema_section(story, validations.get("schema_validation"))
    add_column_checksum_section(story, validations.get("column_checksum_validation"))
    add_row_checksum_section(story, validations.get("row_checksum_validation"))


# ---------------------------
# FAILURE REPORT GENERATION
# ---------------------------
def generate_failure_report(run_id, timestamp, arch_jobs_all, val_jobs_all):
    story = [Spacer(1, 200),
             Paragraph("DATA VALIDATION FAILURE REPORT", TITLE_STYLE),
             Paragraph(f"Run ID: {run_id}", TEXT_STYLE),
             Paragraph(f"Timestamp: {timestamp}", TEXT_STYLE),
             PageBreak()]

    for job in val_jobs_all:
        tgt_table = job["script_args"]["--target_table"]
        tgt_db = job["script_args"]["--target_database"]
        src_table = f"{job['script_args']['--source_schema']}.{job['script_args']['--source_table']}"

        val_records = fetch_validation_json_records(tgt_db, tgt_table)
        if not val_records:
            continue

        validations = group_records_by_validation_type(val_records)
        def failed(v): return v and v["result"].upper() not in ("PASSED", "SUCCEEDED")
        failed_any = any([failed(validations.get(v)) for v in [
            "row_count_validation", "schema_validation",
            "column_checksum_validation", "row_checksum_validation"]])

        if failed_any:
            process_table_report(story, src_table, tgt_db, tgt_table,
                                 val_records,
                                 filter_jobs_by_table(arch_jobs_all, tgt_table),
                                 filter_jobs_by_table(val_jobs_all, tgt_table))

    if len(story) > 1:
        pdf_buffer = BytesIO()
        doc = SimpleDocTemplate(pdf_buffer, pagesize=letter)
        doc.build(story, onFirstPage=empty_page_footer, onLaterPages=add_page_header_footer)
        output_key = f"validation_final_report/{timestamp}/{run_id}/validation_final_report.pdf"
        pdf_buffer.seek(0)
        S3_CLIENT.put_object(Body=pdf_buffer, Bucket=BUCKET_NAME, Key=output_key, ContentType="application/pdf")
        print(f"✅ Failure report uploaded to s3://{BUCKET_NAME}/{output_key}")

# ---------------------------
# ATHENA METADATA INSERT
# ---------------------------
def insert_report_metadata(run_id, timestamp, report_key, failure_report_key):
    report_url = f"https://{BUCKET_NAME}.s3.{AWS_REGION}.amazonaws.com/{report_key}"
    failure_url = f"https://{BUCKET_NAME}.s3.{AWS_REGION}.amazonaws.com/{failure_report_key}"
    created_date = timestamp.split("T")[0]

    query = f"""
    INSERT INTO {ATHENA_TABLE} (airflow_run_id, created_date, validation_summary_report, validation_report)
    VALUES ('{run_id}', DATE('{created_date}'), '{report_url}', '{failure_url}')
    """
    response = ATHENA_CLIENT.start_query_execution(
        QueryString=query,
        QueryExecutionContext={'Database': ATHENA_DATABASE},
        ResultConfiguration={'OutputLocation': ATHENA_OUTPUT}
    )
    print(f"Athena INSERT query submitted: Execution ID: {response['QueryExecutionId']}")

# ---------------------------
# LAMBDA ENTRY POINT
# ---------------------------
def lambda_handler(event, context):
    run_id = event.get("run_id")
    timestamp = event.get("timestamp")
    metadata = event.get("metadata", {})
    arch_jobs_all = metadata.get("archive_jobs", [])
    val_jobs_all = metadata.get("validate_jobs", [])

    # Build summary report
    story = [Spacer(1, 200),
             Paragraph("DATA VALIDATION REPORT", TITLE_STYLE),
             Paragraph(f"Run ID: {run_id}", TEXT_STYLE),
             Paragraph(f"Timestamp: {timestamp}", TEXT_STYLE)]

    for job in val_jobs_all:
        tgt_table = job["script_args"]["--target_table"]
        tgt_db = job["script_args"]["--target_database"]
        src_table = f"{job['script_args']['--source_schema']}.{job['script_args']['--source_table']}"

        val_records = fetch_validation_json_records(tgt_db, tgt_table)
        if not val_records:
            # Add placeholder for missing validation
            story.append(PageBreak())
            story.append(draw_horizontal_line())
            story.append(Spacer(1, 6))
            story.append(Paragraph("VALIDATION REPORT", TITLE_STYLE))
            story.append(Paragraph(f"<b>SOURCE TABLE:</b> {src_table}", TEXT_STYLE))
            story.append(Paragraph(f"<b>TARGET TABLE:</b> {tgt_db}.{tgt_table}", TEXT_STYLE))
            story.append(Spacer(1, 12))
            story.append(Paragraph(f"No validation data found for table '{tgt_table}'.", TEXT_STYLE))
            story.append(Spacer(1, 24))
            story.append(draw_horizontal_line())
            story.append(Spacer(1, 12))
            continue

        process_table_report(story, src_table, tgt_db, tgt_table,
                             val_records,
                             filter_jobs_by_table(arch_jobs_all, tgt_table),
                             filter_jobs_by_table(val_jobs_all, tgt_table))

    # Build and upload summary report PDF
    pdf_buffer = BytesIO()
    doc = SimpleDocTemplate(pdf_buffer, pagesize=letter)
    doc.build(story, onFirstPage=empty_page_footer, onLaterPages=add_page_header_footer)
    summary_key = f"validation_summary_report/{timestamp}/{run_id}/validation_summary_report.pdf"
    pdf_buffer.seek(0)
    S3_CLIENT.put_object(Body=pdf_buffer, Bucket=BUCKET_NAME, Key=summary_key, ContentType="application/pdf")

    # Generate failure report
    generate_failure_report(run_id, timestamp, arch_jobs_all, val_jobs_all)
    failure_key = f"validation_final_report/{timestamp}/{run_id}/validation_final_report.pdf"

    # Insert metadata in Athena
    insert_report_metadata(run_id, timestamp, summary_key, failure_key)

    return {
        "validation_report": f"s3://{BUCKET_NAME}/{failure_key}",
        "validation_summary_report": f"s3://{BUCKET_NAME}/{summary_key}"
    }
