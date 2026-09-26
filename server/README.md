# Сервисы Orange Pi

Оба приложения используют только стандартную библиотеку Python.

- `web_server.py` — статический SPA-сервер, allowlist reverse proxy к плате и
  локальное API снимка NodeDB (`GET/POST /node-cache.json`).
- `nightbot_archive.py` — сбор JSONL в SQLite и локальная страница экспорта.
- `systemd/` — sandboxed unit-файлы с общей конфигурацией
  `/etc/barbienode/barbienode.env`.

Проверка без установки:

```bash
python3 -m py_compile web_server.py nightbot_archive.py
```

Unit веб-сервера использует `StateDirectory=barbienode-web`, поэтому снимок
NodeDB переживает перезапуск службы и обновление статических файлов. Размер
принимаемого JSON ограничен 4 MiB, число нод — 2000; запись выполняется атомарно.
