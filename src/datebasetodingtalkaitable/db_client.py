"""
SqlServer 数据库连接与自定义 SQL 查询。
"""
from typing import Any

import pyodbc


class DbClientError(Exception):
    """数据库相关异常。"""
    pass


def build_connection_string(
    server: str,
    database: str,
    user: str,
    password: str,
    port: int | None = None,
    driver: str | None = None,
) -> str:
    """构建 SqlServer 连接字符串。"""
    if port:
        server_part = f"{server},{port}"
    else:
        server_part = server
    # Windows 常用驱动: ODBC Driver 17 for SQL Server / SQL Server
    if not driver:
        drivers = [c for c in pyodbc.drivers() if "SQL Server" in c or "ODBC Driver" in c]
        driver = drivers[0] if drivers else "ODBC Driver 17 for SQL Server"
    return (
        f"DRIVER={{{driver}}};"
        f"SERVER={server_part};"
        f"DATABASE={database};"
        f"UID={user};"
        f"PWD={password};"
    )


def test_connection(conn_str: str) -> dict[str, Any]:
    """
    测试连接。返回 { "ok": True } 或 { "ok": False, "error": "..." }。
    """
    try:
        conn = pyodbc.connect(conn_str)
        conn.close()
        return {"ok": True}
    except Exception as e:
        return {"ok": False, "error": str(e)}


def execute_query(
    conn_str: str,
    sql: str,
    limit: int | None = None,
) -> tuple[list[str], list[dict[str, Any]]]:
    """
    执行只读 SQL，返回 (列名列表, 行列表)。
    每行为字典：列名 -> 值。若 limit 有值则最多返回 limit 行（用于预览）；同步时可不传 limit 以拉全量。
    """
    conn = pyodbc.connect(conn_str)
    try:
        cursor = conn.cursor()
        cursor.execute(sql)
        columns = [d[0] for d in cursor.description]
        rows = []
        for row in cursor.fetchall():
            rows.append(dict(zip(columns, row)))
            if limit is not None and len(rows) >= limit:
                break
        return columns, rows
    finally:
        conn.close()
