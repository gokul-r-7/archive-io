import json
import re
import logging
import boto3
import yaml
from urllib.parse import quote_plus
from sqlalchemy import create_engine, inspect

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def load_yaml_from_s3(bucket, key):
    s3 = boto3.client('s3')
    logger.info(f"Loading YAML from s3://{bucket}/{key}")
    response = s3.get_object(Bucket=bucket, Key=key)
    config = yaml.safe_load(response['Body'])
    return config


def get_glue_connection_details(connection_name):
    glue = boto3.client('glue')
    logger.info(f"Fetching Glue connection details for '{connection_name}'")
    resp = glue.get_connection(Name=connection_name)
    props = resp['Connection']['ConnectionProperties']
    jdbc = props["JDBC_CONNECTION_URL"]

    if jdbc.startswith("jdbc:sqlserver://"):
        pattern = r"jdbc:sqlserver://(?P<host>[^:;]+):(?P<port>\d+);database=(?P<db>[^;]+)"
    else:
        pattern = (
            r"jdbc:(?P<dialect>[^:]+)://"
            r"(?P<host>[^:/]+):(?P<port>\d+)[/:](?P<db1>[A-Za-z0-9_]+)"
            r"(?:;database=(?P<db2>[A-Za-z0-9_]+))?"
        )

    match = re.match(pattern, jdbc)
    if not match:
        raise ValueError(f"Unable to parse JDBC URL: {jdbc}")

    groupdict = match.groupdict()
    host = groupdict.get("host")
    port = int(groupdict.get("port"))
    database = groupdict.get("db2") or groupdict.get("db1") or groupdict.get("db")

    return {
        "username": props.get("USERNAME"),
        "password": props.get("PASSWORD"),
        "host": host,
        "port": port,
        "database": database,
        "url": jdbc
    }



def build_sqlalchemy_engine(conn_details):
    jdbc = conn_details['url']

    if jdbc.startswith("jdbc:mysql://"):
        dialect = "mysql+pymysql"
    elif jdbc.startswith("jdbc:postgresql://"):
        dialect = "postgresql+psycopg2"
    elif jdbc.startswith("jdbc:sqlserver://"):
        dialect = "mssql+pymssql"
    elif jdbc.startswith("jdbc:oracle://"):
        dialect = "oracle+oracledb"
    else:
        raise ValueError(f"Unsupported JDBC URL: {jdbc}")

    url = (
        f"{dialect}://{conn_details['username']}:{quote_plus(conn_details['password'])}"
        f"@{conn_details['host']}:{conn_details['port']}/{conn_details['database']}"
    )
    return create_engine(url)


def discover_tables(connection_name, included_schemas, table_regex):
    conn_details = get_glue_connection_details(connection_name)
    engine = build_sqlalchemy_engine(conn_details)
    inspector = inspect(engine)
    matched_tables = []

    for schema in inspector.get_schema_names():
        if included_schemas and schema not in included_schemas:
            continue

        for table_name in inspector.get_table_names(schema=schema):
            if re.fullmatch(table_regex, table_name, re.IGNORECASE):
                full_table_name = f"{schema}.{table_name}"
                matched_tables.append({
                    "datastore_type": "sqlserver" if engine.name == "mssql" else engine.name,
                    "database_name": conn_details["database"],
                    "table_name": full_table_name,
                    "connection_name": connection_name,
                    "iceberg_db": conn_details["database"].lower(),
                    "iceberg_table": f"{table_name.lower()}_iceberg"
                })

    return matched_tables


def lambda_handler(event, context):
    try:
        # Replace with actual S3 bucket/key or get from event
        config = load_yaml_from_s3("my-bucket-99433", "yaml_files/archive.yaml")
        iceberg_warehouse = config.get('iceberg_warehouse')
        connection_mapping = config.get('connections', {})
        job_args_list = []

        for source in config.get('sources', []):
            connection_key = source['connection']
            glue_connection_name = connection_mapping.get(connection_key)
            if not glue_connection_name:
                continue

            included_schemas = source.get('include', {}).get('schemas', [])
            table_pattern = source.get('include', {}).get('tables', ["*"])[0]

            matched_tables = discover_tables(
                glue_connection_name,
                included_schemas,
                table_pattern
            )

            # Extract months from '6m'
            retention_str = source.get("retention_policy", "6m")
            retention_months = int(retention_str.replace("m", ""))
            legal_hold = str(source.get("legal_hold", "false")).lower()

            for table in matched_tables:
                job_args = {
                    "--datastore_type": table["datastore_type"],
                    "--database_name": table["database_name"],
                    "--table_name": table["table_name"],
                    "--connection_name": table["connection_name"],
                    "--iceberg_db": table["iceberg_db"],
                    "--iceberg_table": table["iceberg_table"],
                    "--iceberg_warehouse": iceberg_warehouse,
                    "--retention_months": str(retention_months),
                    "--legal_hold": legal_hold
                }
                job_args_list.append(job_args)

        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": "Discovery complete",
                "job_args": job_args_list
            }),
            "headers": {
                "Content-Type": "application/json"
            }
        }

    except Exception as e:
        logger.error("Error occurred", exc_info=True)
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)}),
            "headers": {"Content-Type": "application/json"}
        }