#pragma once

#define USE_SX1262

#define LORA_SCK 12
#define LORA_MISO 13
#define LORA_MOSI 11
#define LORA_CS 10
#define LORA_RESET 5
#define LORA_DIO1 14

#define SX126X_CS LORA_CS
#define SX126X_SCK LORA_SCK
#define SX126X_MISO LORA_MISO
#define SX126X_MOSI LORA_MOSI
#define SX126X_RESET LORA_RESET
#define SX126X_DIO1 LORA_DIO1
#define SX126X_BUSY 4

// The E22-900M22S RF switch is driven by two ESP32-S3 GPIOs.
#define SX126X_RXEN 6
#define SX126X_TXEN 7

#define SX126X_DIO3_TCXO_VOLTAGE 1.8
// EBYTE E22-900M22S rated maximum. The saved LoRa txPower setting may select
// any lower value; the radio driver still enforces the SX1262/module ceiling.
#define SX126X_MAX_POWER 22
// The build environment defines BARBIENODE_ALLOW_REGION_POWER_OVERRIDE so it
// is visible to every translation unit, including the regional power resolver.

// On-board addressable RGB LED used for the autonomous unread-message heartbeat.
#define NOTIFICATION_NEOPIXEL_PIN 48

// Compile the one-night autonomous text logger/reply bot overlay.
#undef MESHTASTIC_EXCLUDE_REPLYBOT
#define MESHTASTIC_EXCLUDE_REPLYBOT 0

#define BUTTON_PIN 0
#define HAS_SCREEN 0
#define HAS_GPS 0

#define I2C_SDA 8
#define I2C_SCL 9
#define UART_TX 43
#define UART_RX 44
