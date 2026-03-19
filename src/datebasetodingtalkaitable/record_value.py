"""
将数据库行数据转换为钉钉《记录值格式》：
https://open.dingtalk.com/document/development/record-value-format

支持：文本(Text)、数字(Number)、日期时间(DateTime) 等简单类型，统一转为文本或文档约定格式。
必填字段无值时按需求报错。
"""
from datetime import date, datetime
from decimal import Decimal
from typing import Any

# 钉钉字段类型常用值（与官方文档一致）
TYPE_TEXT = "Text"
TYPE_NUMBER = "Number"
TYPE_DATE = "DateTime"  # 或 Date，以文档为准


def _cell_value_to_record_value(
    value: Any,
    field_type: str,
    field_id: str,
    required: bool,
) -> Any:
    """
    将单元格值转为钉钉记录值格式。
    若必填且 value 为 None/空，抛出 ValueError。
    """
    if value is None or (isinstance(value, str) and value.strip() == ""):
        if required:
            raise ValueError(f"必填字段 {field_id} 无值")
        return None

    if field_type in (TYPE_NUMBER, "Number"):
        if isinstance(value, (int, float, Decimal)):
            return value
        try:
            return float(value) if "." in str(value) else int(value)
        except (ValueError, TypeError):
            return str(value)

    if field_type in ("DateTime", "Date", TYPE_DATE):
        if isinstance(value, (datetime, date)):
            return value.strftime("%Y-%m-%d %H:%M:%S")
        return str(value)

    # 默认按文本
    if isinstance(value, (datetime, date)):
        return value.strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(value, Decimal):
        return float(value)
    return str(value)


def row_to_record_fields(
    row: dict[str, Any],
    mapping: list[dict[str, Any]],
    fields_schema: list[dict[str, Any]],
) -> dict[str, Any]:
    """
    将一行数据（SQL 结果）按映射转为钉钉 records[].fields 对象。

    :param row: 键为数据库列名的一行
    :param mapping: [ {"dbColumn": "colA", "fieldId": "fldxxx"} ]
    :param fields_schema: 钉钉字段列表，每项含 id, type, required 等
    :return: { "fldxxx": <record-value>, ... }
    :raises ValueError: 必填字段无值时
    """
    schema_by_id = {}
    for f in fields_schema:
        fid = f.get("id") or f.get("fieldId") or f.get("field_id")
        if fid:
            schema_by_id[fid] = f
    result: dict[str, Any] = {}
    for m in mapping:
        db_col = m.get("dbColumn") or m.get("db_column")
        field_id = m.get("fieldId") or m.get("field_id") or m.get("id")
        if not field_id or db_col not in row:
            continue
        field_info = schema_by_id.get(field_id, {})
        field_type = field_info.get("type", TYPE_TEXT)
        required = field_info.get("required", False)
        val = _cell_value_to_record_value(
            row.get(db_col),
            field_type,
            field_id,
            required,
        )
        if val is not None:
            result[field_id] = val
    return result


def rows_to_records(
    rows: list[dict[str, Any]],
    mapping: list[dict[str, Any]],
    fields_schema: list[dict[str, Any]],
    on_required_empty: str = "error",
) -> tuple[list[dict[str, Any]], list[str]]:
    """
    将多行转为钉钉 records 列表，并校验必填。

    :param on_required_empty: "error" 表示必填无值时报错并记录该条错误，跳过该条继续
    :return: (records, errors)  records 为 [ {"fields": { ... } }, ... ]，errors 为错误信息列表
    """
    records = []
    errors = []
    for i, row in enumerate(rows):
        try:
            fields = row_to_record_fields(row, mapping, fields_schema)
            records.append({"fields": fields})
        except ValueError as e:
            errors.append(f"第 {i + 1} 行: {e}")
            if on_required_empty == "error":
                # 跳过该条，不加入 records
                continue
    return records, errors
