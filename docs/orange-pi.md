# Установка на Orange Pi

Пример рассчитан на Debian/Ubuntu/Armbian с Python 3 и systemd. Выберите
статический DHCP lease для Orange Pi и платы или используйте mDNS.

## Файлы

```bash
sudo install -d -m 0755 /opt/barbienode-web /opt/nightbot-archive /etc/barbienode
sudo cp web/dist/* /opt/barbienode-web/
sudo cp server/web_server.py /opt/barbienode-web/
sudo cp server/nightbot_archive.py /opt/nightbot-archive/
sudo cp server/systemd/*.service /etc/systemd/system/
```

Для MeshCore нужен отдельный локальный TCP-мост и виртуальное окружение;
Bluetooth-контроллер Orange Pi не требуется:

```bash
sudo install -d -m 0755 /opt/barbienode-meshcore
sudo install -m 0755 server/meshcore_bridge.py /opt/barbienode-meshcore/
sudo python3 -m venv /opt/barbienode-meshcore/venv
sudo /opt/barbienode-meshcore/venv/bin/pip install -r server/requirements-meshcore.txt
sudo systemctl enable --now barbienode-meshcore
```

TCP-мост ничего не передаёт при запуске: он читает профиль, каналы, контакты и
накопленную очередь сообщений. Первый сохранённый публичный ключ становится
локальной привязкой к BarbieNode. Для повторной привязки к другой плате нельзя
просто подменять файл: сначала следует проверить устройство и отдельно принять
смену идентичности. Companion TCP на ноде принимает соединения только от
фиксированного адреса Orange Pi `192.168.1.19`; BLE остаётся телефону.

Для RNode установите headless MeshChatX:

```bash
sudo install -d -m 0755 /opt/barbienode-meshchatx
sudo install -d -m 0700 -o netwatch-admin -g netwatch-admin /var/lib/barbienode-meshchatx/{storage,reticulum}
sudo install -m 0600 -o netwatch-admin -g netwatch-admin server/reticulum/meshchatx-config /var/lib/barbienode-meshchatx/reticulum/config
sudo python3 -m venv /opt/barbienode-meshchatx/venv
sudo /opt/barbienode-meshchatx/venv/bin/pip install reticulum-meshchatx==4.9.3
sudo systemctl enable --now barbienode-meshchatx
```

Эта конфигурация не содержит интернет-хабов Reticulum. Web UI слушает домашний
адрес `https://192.168.1.19:9337` и требует пароль. Автоанонсы и периодическая
радиосинхронизация выключены. Пока загружен другой режим, серверная база
остаётся доступна, но `RNodeInterface` показывает offline. В режиме RNode она подключает радио с
профилем `868.825 MHz / BW125 / SF10 / CR7 / 18 dBm`; повышать мощность без
отдельного решения нельзя.

Создайте `/etc/barbienode/barbienode.env`:

```dotenv
DEVICE_URL=http://meshtastic.local
BIND_HOST=192.168.1.19
MESHCORE_TCP_HOST=192.168.1.31
MESHCORE_TCP_PORT=5000
```

Замените адрес Orange Pi на свой. Файл не содержит паролей, если плата уже
настроена и доступна по HTTP в локальной сети.

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now barbienode-web nightbot-archive
curl http://192.168.1.19:8082/
curl http://192.168.1.19:8081/healthz
```

В командах проверки также замените `192.168.1.19` на свой адрес Orange Pi.
Открывайте `http://<адрес-orange-pi>:8082`. Архив доступен на порту `8081`.
Не публикуйте эти порты в Интернет; для удалённого доступа используйте VPN с
ограниченными маршрутами и firewall. Для обычного сценария поездки удалённый
доступ к Orange Pi не нужен: телефон работает непосредственно с точкой доступа
платы.
