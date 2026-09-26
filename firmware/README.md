# Firmware overlay

Target для ESP32-S3 N16R8 + E22-900M22S и изменения Meshtastic firmware:

- безопасное переключение Meshtastic `app0` / RNode `app1`;
- проверяемое обновление соседнего application slot;
- защищённая переносная Wi-Fi AP и автоматический fallback через 60 секунд;
- локальный кольцевой JSONL-архив;
- строгий rate-limited ответ на `Ping`;
- RGB-индикатор непрочитанного.

Собранные образы не публикуются: их следует получать из проверенного upstream
commit по инструкции в `docs/firmware.md`.

