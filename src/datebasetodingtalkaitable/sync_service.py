"""
同步服务：从 SqlServer 执行 SQL，按映射全量插入钉钉 AI 表格。
"""
from typing import Any

from .db_client import build_connection_string, execute_query
from .dingtalk_client import DingTalkClient, DingTalkClientError
from .record_value import rows_to_records


def run_sync(
    conn_str: str,
    sql: str,
    app_key: str,
    app_secret: str,
    base_id: str,
    sheet_id: str,
    operator_id: str,
    mapping: list[dict[str, Any]],
    fields_schema: list[dict[str, Any]],
) -> dict[str, Any]:
    """
    执行一次全量同步：查库 → 转换记录值 → 分批插入钉钉。

    :return: { "success": n, "fail": n, "errors": [...], "skippedRows": n }
    """
    columns, rows = execute_query(conn_str, sql, limit=None)
    records, convert_errors = rows_to_records(
        rows,
        mapping,
        fields_schema,
        on_required_empty="error",
    )
    if not records and convert_errors:
        return {
            "success": 0,
            "fail": len(rows),
            "errors": convert_errors,
            "skippedRows": len(convert_errors),
        }

    client = DingTalkClient(app_key, app_secret)
    success, fail, insert_errors = client.insert_records_batch(
        base_id, sheet_id, records, operator_id
    )
    all_errors = convert_errors + insert_errors
    return {
        "success": success,
        "fail": fail + len(convert_errors),
        "errors": all_errors,
        "skippedRows": len(convert_errors),
    }
