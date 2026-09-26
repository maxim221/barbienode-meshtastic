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

Создайте `/etc/barbienode/barbienode.env`:

```dotenv
DEVICE_URL=http://meshtastic.local
BIND_HOST=192.168.1.19
```

Замените адрес Orange Pi на свой. Файл не содержит паролей, если плата уже
настроена и доступна по HTTP в локальной сети.

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now barbienode-web nightbot-archive
curl http://127.0.0.1:8082/
curl http://127.0.0.1:8081/healthz
```

Открывайте `http://<адрес-orange-pi>:8082`. Архив доступен на порту `8081`.
Не публикуйте эти порты в Интернет; для удалённого доступа используйте VPN с
ограниченными маршрутами и firewall.

