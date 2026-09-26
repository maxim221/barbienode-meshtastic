# Сервисы Orange Pi

Оба приложения используют только стандартную библиотеку Python.

- `web_server.py` — статический SPA-сервер и allowlist reverse proxy к плате.
- `nightbot_archive.py` — сбор JSONL в SQLite и локальная страница экспорта.
- `systemd/` — sandboxed unit-файлы с общей конфигурацией
  `/etc/barbienode/barbienode.env`.

Проверка без установки:

```bash
python3 -m py_compile web_server.py nightbot_archive.py
```

