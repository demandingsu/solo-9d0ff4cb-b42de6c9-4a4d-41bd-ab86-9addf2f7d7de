"""进程内按班次的写串行化锁。

同一班次的登记/补登写流程经 ``shift_write_lock`` 串行执行，保证：

* 同一班两笔并发复测都完整走完“读取全部已入库复测 → 取交集 →
  诊断 → 同事务落库”，不会各自只看到自己的一笔；
* 重新登记与补登复测并发时，后执行的一方按已提交的最新登记核对
  （管段已移除则整批退回，不写任何读数与诊断）；
* 不同班次使用不同的锁，一班诊断计算期间其他班仍可正常写入。

锁只覆盖进程内并发（Compose 默认单 uvicorn 进程）；SQLite 侧另以
WAL + busy_timeout + BEGIN IMMEDIATE 兜底写事务。
"""
from __future__ import annotations

import threading
from contextlib import contextmanager

_locks_guard = threading.Lock()
_shift_locks: dict[str, threading.RLock] = {}


@contextmanager
def shift_write_lock(shift: str):
    with _locks_guard:
        lock = _shift_locks.get(shift)
        if lock is None:
            lock = threading.RLock()
            _shift_locks[shift] = lock
    with lock:
        yield
