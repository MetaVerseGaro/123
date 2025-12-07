# nohup 启动说明（使用 JSON 配置）

- 单实例（使用 `botA.json`，env 文件在 JSON 中指定）：
  ```bash
  LOG_FILE_PREFIX=botA_ LOG_RETENTION_DAYS=1 \
  nohup python3 runbot.py botA.json > botA.out 2>&1 &
  ```

- 多实例：复制上一行，替换输出文件/日志前缀/配置文件（如 `botB.json`），并确保各自的 env 文件不同。

- 查看/排障：
  ```bash
  tail -f botA.out        # 实时日志
  ps aux | grep runbot    # 查看进程
  ```

## 查看 webhook_server 进程
- 查看占用端口的进程（默认 8080，自定义请替换端口）：
  ```bash
  sudo lsof -i :8080
  sudo netstat -tlnp | grep 8080
  ```
- 按进程名筛选：
  ```bash
  ps -ef | grep zigzag_webhook_server.py | grep -v grep
  ```
- 结束进程（示例 PID=12345）：
  ```bash
  sudo kill 12345
  # 或强制：sudo kill -9 12345
  ```
____________________________________ ERROR collecting test_core_services.py ____________________________________
ImportError while importing test module '/home/lighter/123/tests/test_core_services.py'.
Hint: make sure your test modules/packages have valid Python names.
Traceback:
/usr/lib/python3.12/importlib/__init__.py:90: in import_module
    return _bootstrap._gcd_import(name[level:], package, level)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
test_core_services.py:9: in <module>
    from core.data_feeds import AsyncCache, SharedBBOStore, PivotFileWatcher
E   ModuleNotFoundError: No module named 'core'
