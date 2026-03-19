"""
钉钉开放平台 API 客户端：Token、获取数据表、获取字段、批量插入记录。
接口说明见：https://open.dingtalk.com/document/development/
"""
import time
from typing import Any

import requests

from .config import (
    DINGTALK_ACCESS_TOKEN_HEADER,
    DINGTALK_API_BASE,
    DINGTALK_GET_TOKEN_URL,
    INSERT_RECORDS_BATCH_SIZE,
)


class DingTalkClientError(Exception):
    """钉钉 API 调用异常。"""
    def __init__(self, message: str, code: str | None = None, body: Any = None):
        super().__init__(message)
        self.code = code
        self.body = body


class DingTalkClient:
    """钉钉 API 客户端（企业内部应用 + 多维表格/AI 表格）。"""

    def __init__(self, app_key: str, app_secret: str):
        self.app_key = app_key
        self.app_secret = app_secret
        self._access_token: str | None = None
        self._token_expires_at: float = 0

    def get_access_token(self, force_refresh: bool = False) -> str:
        """获取 access_token（带缓存，过期前 5 分钟刷新）。"""
        if not force_refresh and self._access_token and time.time() < self._token_expires_at - 300:
            return self._access_token
        resp = requests.get(
            DINGTALK_GET_TOKEN_URL,
            params={"appkey": self.app_key, "appsecret": self.app_secret},
            timeout=15,
        )
        data = resp.json()
        if data.get("errcode") != 0:
            raise DingTalkClientError(
                data.get("errmsg", "get token failed"),
                code=str(data.get("errcode", "")),
                body=data,
            )
        self._access_token = data["access_token"]
        # 默认 7200 秒，提前 5 分钟刷新
        self._token_expires_at = time.time() + int(data.get("expires_in", 7200))
        return self._access_token

    def _headers(self) -> dict[str, str]:
        return {
            "Content-Type": "application/json",
            DINGTALK_ACCESS_TOKEN_HEADER: self.get_access_token(),
        }

    def _request(
        self,
        method: str,
        path: str,
        json_body: dict | None = None,
        params: dict | None = None,
    ) -> dict[str, Any]:
        url = f"{DINGTALK_API_BASE.rstrip('/')}{path}"
        resp = requests.request(
            method,
            url,
            headers=self._headers(),
            json=json_body,
            params=params,
            timeout=30,
        )
        data = resp.json() if resp.text else {}
        if resp.status_code >= 400:
            raise DingTalkClientError(
                data.get("message", data.get("errmsg", resp.text or f"HTTP {resp.status_code}")),
                code=data.get("code", str(resp.status_code)),
                body=data,
            )
        # 旧版 gettoken 的 errcode 在 body 里已在上层处理；新网关错误可能在 data.code
        if isinstance(data.get("code"), str) and data.get("code") not in ("0", "200", ""):
            raise DingTalkClientError(
                data.get("message", data.get("msg", "request failed")),
                code=data.get("code"),
                body=data,
            )
        return data

    def get_all_sheets(self, base_id: str, operator_id: str) -> list[dict[str, Any]]:
        """
        获取 AI 表格下的所有数据表（Sheet）列表。
        官方文档：GET /v1.0/notable/bases/{baseId}/sheets?operatorId=String
        """
        result = self._request(
            "GET",
            f"/v1.0/notable/bases/{base_id}/sheets",
            params={"operatorId": operator_id},
        )
        # 文档返回：value 为数据表列表，每项含 id、name
        if isinstance(result, list):
            return result
        return result.get("value", result.get("result", []))

    def get_all_fields(
        self, base_id: str, sheet_id: str, operator_id: str
    ) -> list[dict[str, Any]]:
        """
        获取指定数据表的所有字段。
        按「获取所有数据表」同类规范，使用 notable/bases 路径并传 operatorId。
        """
        result = self._request(
            "GET",
            f"/v1.0/notable/bases/{base_id}/sheets/{sheet_id}/fields",
            params={"operatorId": operator_id},
        )
        if isinstance(result, list):
            return result
        if "fields" in result:
            return result["fields"]
        return result.get("value", result.get("result", []))

    def insert_records(
        self,
        base_id: str,
        sheet_id: str,
        records: list[dict[str, Any]],
        operator_id: str,
    ) -> dict[str, Any]:
        """
        批量插入记录。每条 record 格式：{ "fields": { "fieldId": <record-value-format> } }
        按 notable/bases 路径并传 operatorId。
        """
        if not records:
            return {"success": 0, "fail": 0, "records": []}
        batch = records[:INSERT_RECORDS_BATCH_SIZE]
        body = {"records": [{"fields": r["fields"]} for r in batch]}
        result = self._request(
            "POST",
            f"/v1.0/notable/bases/{base_id}/sheets/{sheet_id}/records",
            json_body=body,
            params={"operatorId": operator_id},
        )
        return result

    def insert_records_batch(
        self,
        base_id: str,
        sheet_id: str,
        records: list[dict[str, Any]],
        operator_id: str,
        batch_size: int | None = None,
    ) -> tuple[int, int, list[str]]:
        """分批插入全部记录，返回 (成功数, 失败数, 错误信息列表)。"""
        size = batch_size or INSERT_RECORDS_BATCH_SIZE
        success, fail, errors = 0, 0, []
        for i in range(0, len(records), size):
            chunk = records[i : i + size]
            try:
                self.insert_records(base_id, sheet_id, chunk, operator_id)
                success += len(chunk)
            except DingTalkClientError as e:
                fail += len(chunk)
                errors.append(f"第 {i // size + 1} 批: {e}")
        return success, fail, errors
