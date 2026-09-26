# Сборка прошивки

Перед первой подачей питания соберите и проверьте аппаратную часть по
[подробной инструкции по распайке](wiring.md).

## Аппаратная схема

| E22-900M22S | ESP32-S3 GPIO |
| --- | ---: |
| BUSY | 4 |
| NRST | 5 |
| RXEN | 6 |
| TXEN | 7 |
| NSS / CS | 10 |
| MOSI | 11 |
| SCK | 12 |
| MISO | 13 |
| DIO1 | 14 |

Модуль объявлен как SX1262 с TCXO 1,8 В. Адресный RGB-светодиод находится на
GPIO48. Максимум, зашитый в target, — 11 dBm; фактическую мощность задаёт и
ограничивает сохранённая конфигурация.

## Воспроизводимая сборка

```bash
git clone --recursive https://github.com/meshtastic/firmware.git meshtastic-firmware
cd meshtastic-firmware
git checkout 608ff51c867da2004178f8c281397ffbc5079f6e
python3 /path/to/barbienode-meshtastic/firmware/apply-dualboot.py "$PWD"
pio run -e e22-s3-n16r8
```

Скрипт копирует overlay, добавляет HTTP endpoints и Wi-Fi fallback. Он
идемпотентен и останавливается, если ожидаемые anchors исходной версии не
совпали. Это намеренно: применять патч к другой версии без ревью опасно.

## Настраиваемые строки

Перед личной сборкой проверьте в `overlay/ReplyBotModule.cpp`:

- `PING_CHANNEL_NAME`;
- `PING_LOCATION`;
- тексты ответов и cooldown;
- `ENABLE_SCHEDULED_BEACON` — оставляйте `false`, если нет явной причины и
  согласования периодического трафика.

Пароли, channel PSK и координаты в исходный код добавлять не нужно.
