"""配置常量：钉钉 API 地址等（以官方文档为准可在此修改）。"""
import os

# 钉钉 企业内部应用 gettoken（旧版网关）
DINGTALK_GET_TOKEN_URL = "https://oapi.dingtalk.com/gettoken"

# 钉钉 开放平台新网关（多维表格 / AI 表格）
DINGTALK_API_BASE = os.environ.get("DINGTALK_API_BASE", "https://api.dingtalk.com")
# 新网关鉴权 Header 名（常见为 x-acs-dingtalk-access-token 或 Authorization: Bearer）
DINGTALK_ACCESS_TOKEN_HEADER = "x-acs-dingtalk-access-token"

# 插入记录单批条数上限（按钉钉文档限制设置，未明确时先用 100）
INSERT_RECORDS_BATCH_SIZE = 100
