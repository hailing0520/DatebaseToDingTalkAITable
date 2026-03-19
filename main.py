"""
启动「数据库同步钉钉 AI 表格」本地服务。
浏览器打开 http://127.0.0.1:5000 使用前台操作页面。
"""
import sys
from pathlib import Path

# 保证可无安装直接运行：把 src 加入 path
_root = Path(__file__).resolve().parent
_src = _root / "src"
if str(_src) not in sys.path:
    sys.path.insert(0, str(_src))

from datebasetodingtalkaitable.app import app


def main():
    print("数据库同步钉钉 AI 表格 - 本地服务")
    print("浏览器打开: http://127.0.0.1:5000")
    app.run(host="127.0.0.1", port=5000, debug=True)


if __name__ == "__main__":
    main()
