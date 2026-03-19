"""
Flask 应用：提供数据库测试、SQL 预览、钉钉数据表/字段、同步等 API，并托管前台页面。
"""
import os

from flask import Flask, jsonify, request, send_from_directory

from .db_client import build_connection_string, execute_query, test_connection
from .dingtalk_client import DingTalkClient, DingTalkClientError
from .sync_service import run_sync

app = Flask(
    __name__,
    static_folder=os.path.join(os.path.dirname(__file__), "static"),
    static_url_path="",
)


# ---------- 数据库 ----------
@app.route("/api/db/test", methods=["POST"])
def api_db_test():
    """测试数据库连接。body: { server, database, user, password, port? }"""
    data = request.get_json() or {}
    server = data.get("server", "").strip()
    database = data.get("database", "").strip()
    user = data.get("user", "").strip()
    password = data.get("password", "")
    port = data.get("port") or 1433
    if not all([server, database, user]):
        return jsonify({"ok": False, "error": "缺少 server / database / user"}), 400
    conn_str = build_connection_string(server, database, user, password, port=port)
    result = test_connection(conn_str)
    if result["ok"]:
        return jsonify(result)
    return jsonify(result), 400


@app.route("/api/db/query", methods=["POST"])
def api_db_query():
    """执行 SQL 预览。body: { server, database, user, password, port?, sql, limit? }"""
    data = request.get_json() or {}
    server = data.get("server", "").strip()
    database = data.get("database", "").strip()
    user = data.get("user", "").strip()
    password = data.get("password", "")
    port = data.get("port") or 1433
    sql = (data.get("sql") or "").strip()
    limit = data.get("limit")
    if limit is not None:
        limit = int(limit)
    if not all([server, database, user, sql]):
        return jsonify({"error": "缺少 server / database / user / sql"}), 400
    conn_str = build_connection_string(server, database, user, password, port=port)
    try:
        columns, rows = execute_query(conn_str, sql, limit=limit or 100)
        return jsonify({"columns": columns, "rows": rows})
    except Exception as e:
        return jsonify({"error": str(e)}), 400


# ---------- 钉钉 ----------
def _dingtalk_client() -> DingTalkClient | tuple[dict, int]:
    """从当前请求 body 取 appKey/appSecret 并返回客户端，失败返回 (json_response, status)。"""
    data = request.get_json() or {}
    app_key = (data.get("appKey") or data.get("app_key") or "").strip()
    app_secret = data.get("appSecret") or data.get("app_secret") or ""
    if not app_key or not app_secret:
        return jsonify({"error": "缺少 appKey 或 appSecret"}), 400
    return DingTalkClient(app_key, app_secret)


@app.route("/api/dingtalk/token", methods=["POST"])
def api_dingtalk_token():
    """获取 access_token（用于前端显示或调试）。"""
    client = _dingtalk_client()
    if isinstance(client, tuple):
        return client
    try:
        token = client.get_access_token(force_refresh=True)
        return jsonify({"ok": True, "access_token": token[:20] + "..."})
    except DingTalkClientError as e:
        return jsonify({"ok": False, "error": str(e), "code": e.code}), 400


@app.route("/api/dingtalk/sheets", methods=["POST"])
def api_dingtalk_sheets():
    """获取 AI 表格下所有数据表。body: { appKey, appSecret, datasheetId, operatorId }"""
    client = _dingtalk_client()
    if isinstance(client, tuple):
        return client
    data = request.get_json() or {}
    base_id = (data.get("datasheetId") or data.get("datasheet_id") or "").strip()
    operator_id = (data.get("operatorId") or data.get("operator_id") or "").strip()
    if not base_id:
        return jsonify({"error": "缺少 datasheetId（AI 表格 ID）"}), 400
    if not operator_id:
        return jsonify({"error": "缺少 operatorId（操作人 unionId），见接口文档"}), 400
    try:
        sheets = client.get_all_sheets(base_id, operator_id)
        return jsonify({"sheets": sheets})
    except DingTalkClientError as e:
        return jsonify({"error": str(e), "code": getattr(e, "code", None)}), 400


@app.route("/api/dingtalk/fields", methods=["POST"])
def api_dingtalk_fields():
    """获取指定数据表的所有字段。body: { appKey, appSecret, datasheetId, sheetId, operatorId }"""
    client = _dingtalk_client()
    if isinstance(client, tuple):
        return client
    data = request.get_json() or {}
    base_id = (data.get("datasheetId") or data.get("datasheet_id") or "").strip()
    sheet_id = (data.get("sheetId") or data.get("sheet_id") or "").strip()
    operator_id = (data.get("operatorId") or data.get("operator_id") or "").strip()
    if not base_id or not sheet_id:
        return jsonify({"error": "缺少 datasheetId 或 sheetId"}), 400
    if not operator_id:
        return jsonify({"error": "缺少 operatorId（操作人 unionId）"}), 400
    try:
        fields = client.get_all_fields(base_id, sheet_id, operator_id)
        return jsonify({"fields": fields})
    except DingTalkClientError as e:
        return jsonify({"error": str(e), "code": getattr(e, "code", None)}), 400


# ---------- 同步 ----------
@app.route("/api/sync", methods=["POST"])
def api_sync():
    """
    执行全量同步。
    body: {
      server, database, user, password, port?,
      sql,
      appKey, appSecret,
      datasheetId, sheetId,
      mapping: [ { dbColumn, fieldId } ],
      fieldsSchema: [ { id, type, required } ]  // 钉钉字段列表，用于必填校验与类型
    }
    """
    data = request.get_json() or {}
    server = data.get("server", "").strip()
    database = data.get("database", "").strip()
    user = data.get("user", "").strip()
    password = data.get("password", "")
    port = data.get("port") or 1433
    sql = (data.get("sql") or "").strip()
    app_key = (data.get("appKey") or data.get("app_key") or "").strip()
    app_secret = data.get("appSecret") or data.get("app_secret") or ""
    base_id = (data.get("datasheetId") or data.get("datasheet_id") or "").strip()
    sheet_id = (data.get("sheetId") or data.get("sheet_id") or "").strip()
    operator_id = (data.get("operatorId") or data.get("operator_id") or "").strip()
    mapping = data.get("mapping") or []
    fields_schema = data.get("fieldsSchema") or data.get("fields_schema") or []

    if not all([server, database, user, sql]):
        return jsonify({"error": "缺少数据库参数或 sql"}), 400
    if not app_key or not app_secret:
        return jsonify({"error": "缺少 appKey 或 appSecret"}), 400
    if not base_id or not sheet_id:
        return jsonify({"error": "缺少 datasheetId 或 sheetId"}), 400
    if not operator_id:
        return jsonify({"error": "缺少 operatorId（操作人 unionId）"}), 400
    if not mapping:
        return jsonify({"error": "缺少 mapping"}), 400

    conn_str = build_connection_string(server, database, user, password, port=port)
    try:
        result = run_sync(
            conn_str,
            sql,
            app_key,
            app_secret,
            base_id,
            sheet_id,
            operator_id,
            mapping,
            fields_schema,
        )
        return jsonify(result)
    except DingTalkClientError as e:
        return jsonify({"error": str(e), "success": 0, "fail": 0}), 400
    except Exception as e:
        return jsonify({"error": str(e), "success": 0, "fail": 0}), 500


# ---------- 前台页面 ----------
@app.route("/")
def index():
    """返回前台操作页。"""
    return send_from_directory(app.static_folder, "index.html")

