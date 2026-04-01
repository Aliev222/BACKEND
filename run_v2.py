"""
Запуск новой архитектуры API v2 на отдельном порту.
Старый api.py продолжает работать на порту 8000.

Использование:
  python run_v2.py              # порт 8001
  python run_v2.py --port 9000  # кастомный порт
"""

import os
import sys
import argparse
import uvicorn

parser = argparse.ArgumentParser()
parser.add_argument("--port", type=int, default=8001)
parser.add_argument("--host", type=str, default="0.0.0.0")
parser.add_argument("--reload", action="store_true")
args = parser.parse_args()

os.environ.setdefault("APP_ENV", "development")

print(f"=" * 50)
print(f"  SPIRIT API v2 — порт {args.port}")
print(f"  Старый API — порт 8000 (не тронут)")
print(f"  Health: http://localhost:{args.port}/health")
print(f"  Docs:   http://localhost:{args.port}/docs")
print(f"=" * 50)

uvicorn.run(
    "main_v2:app",
    host=args.host,
    port=args.port,
    reload=args.reload,
    log_level="info",
)
