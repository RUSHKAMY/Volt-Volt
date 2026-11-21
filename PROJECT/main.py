import gc
import network
import socket
import time
import ujson
import uos
from machine import I2C, PWM, Pin
import ssd1306

try:
    import uselect
except ImportError:
    import select as uselect


# --- НАСТРОЙКИ СЕТИ И MQTT ---
WIFI_SSID = "Wokwi-GUEST"
WIFI_PASS = ""
MQTT_BROKER = "broker.hivemq.com"
MQTT_PORT = 1883

# Единый топик состояния (ESP32 -> Сайт) и топик команд (Сайт -> ESP32)
TOPIC_STATE = b"locker_project/state"
TOPIC_CMD = b"locker_project/cmd"

# --- ПИНЫ ЖЕЛЕЗА ---
PIN_SDA = 21
PIN_SCL = 22
PIN_SERVO = 13
PIN_RELAY = 27
PIN_BTN = 16

TIMEOUT_SEC = 5400  # 1.5 часа (90 минут)

# --- ИНИЦИАЛИЗАЦИЯ ПЕРИФЕРИИ ---
i2c = I2C(0, scl=Pin(PIN_SCL), sda=Pin(PIN_SDA))
oled = ssd1306.SSD1306_I2C(128, 64, i2c)

relay = Pin(PIN_RELAY, Pin.OUT)
btn = Pin(PIN_BTN, Pin.IN, Pin.PULL_UP)
servo = PWM(Pin(PIN_SERVO), freq=50)


def set_servo(angle):
    duty = int(1638 + (angle / 180.0) * (8192 - 1638))
    servo.duty_u16(duty)


def show_screen(line1="", line2="", line3=""):
    try:
        oled.fill(0)
        if line1:
            oled.text(line1, 0, 10)
        if line2:
            oled.text(line2, 0, 28)
        if line3:
            oled.text(line3, 0, 46)
        oled.show()
    except Exception as e:
        print("[OLED ERROR]", e)


# --- СОСТОЯНИЯ СТЕНДА ---
STATE_FREE = 0
STATE_OPEN_TO_PUT = 1
STATE_CHARGING = 2
STATE_OPEN_TO_TAKE = 3
STATE_AWAITING_PICKUP = 4

state = STATE_FREE
local_key = None
start_time = 0
state_seq = 0

last_btn_state = 1
last_debounce_time = 0
last_display_update = 0
last_ping_time = 0
last_reconnect_try = 0
last_wifi_try = 0

mqtt_connected = False
mqtt = None
need_sync = False
cached_broker_addr = None
connect_fail_count = 0


def _encode_len(length):
    out = bytearray()
    while True:
        byte = length % 128
        length //= 128
        if length > 0:
            byte |= 0x80
        out.append(byte)
        if length == 0:
            break
    return out


class ReliableMQTT:
    """MQTT 3.1.1 клиент с QoS 1 (PUBACK), контролем PINGRESP и защитой от утечки сокетов."""

    def __init__(self, client_id, broker, port=1883, keepalive=25, timeout=2.5):
        self.client_id = client_id
        self.broker = broker
        self.port = port
        self.keepalive = keepalive
        self.timeout = timeout
        self.sock = None
        self.poller = None
        self.cb = None
        self.pid = 0
        self.ping_waiting = False
        self.ping_sent_ms = 0
        self.last_rx_ms = time.ticks_ms()

    def set_callback(self, cb):
        self.cb = cb

    def _send_all(self, data):
        total = len(data)
        sent = 0
        while sent < total:
            n = self.sock.send(data[sent:])
            if not n:
                raise OSError("Socket send failed")
            sent += n

    def _recv_exact(self, n):
        buf = bytearray()
        while len(buf) < n:
            chunk = self.sock.recv(n - len(buf))
            if not chunk:
                raise OSError("Socket EOF")
            buf.extend(chunk)
        return bytes(buf)

    def _read_one_packet(self):
        first = self._recv_exact(1)[0]
        mult = 1
        rem_len = 0
        while True:
            b = self._recv_exact(1)[0]
            rem_len += (b & 0x7F) * mult
            if (b & 0x80) == 0:
                break
            mult *= 128

        body = self._recv_exact(rem_len) if rem_len > 0 else b""
        self.last_rx_ms = time.ticks_ms()
        op = first & 0xF0
        pkt_pid = 0

        if op == 0xD0:
            # PINGRESP — соединение живо
            self.ping_waiting = False
        elif op in (0x40, 0x90) and len(body) >= 2:
            # PUBACK (0x40) или SUBACK (0x90)
            pkt_pid = (body[0] << 8) | body[1]
        elif op == 0x30 and len(body) >= 2:
            # Входящий PUBLISH
            tlen = (body[0] << 8) | body[1]
            topic = body[2 : 2 + tlen]
            idx = 2 + tlen
            qos = (first & 0x06) >> 1
            if qos > 0 and idx + 2 <= len(body):
                pkt_pid = (body[idx] << 8) | body[idx + 1]
                idx += 2
                if qos == 1:
                    try:
                        self._send_all(b"\x40\x02" + bytes([(pkt_pid >> 8) & 0xFF, pkt_pid & 0xFF]))
                    except Exception:
                        pass
            msg = body[idx:]
            if self.cb is not None:
                self.cb(topic, msg)

        return op, pkt_pid

    def connect(self):
        global cached_broker_addr, connect_fail_count
        self.close()

        if cached_broker_addr is None or connect_fail_count >= 2:
            cached_broker_addr = socket.getaddrinfo(self.broker, self.port)[0][-1]
            connect_fail_count = 0

        s = socket.socket()
        try:
            s.settimeout(self.timeout)
            s.connect(cached_broker_addr)
            self.sock = s
            self.poller = uselect.poll()
            self.poller.register(s, uselect.POLLIN)

            cid = self.client_id.encode()
            vh = b"\x00\x04MQTT\x04\x02" + bytes([(self.keepalive >> 8) & 0xFF, self.keepalive & 0xFF])
            pl = bytes([(len(cid) >> 8) & 0xFF, len(cid) & 0xFF]) + cid
            pkt = b"\x10" + _encode_len(len(vh) + len(pl)) + vh + pl
            self._send_all(pkt)

            resp = self._recv_exact(4)
            if resp[0] != 0x20 or resp[3] != 0:
                raise OSError("Bad CONNACK")

            self.ping_waiting = False
            self.last_rx_ms = time.ticks_ms()
        except Exception:
            try:
                if self.poller is not None:
                    self.poller.unregister(s)
            except Exception:
                pass
            try:
                s.close()
            except Exception:
                pass
            self.sock = None
            self.poller = None
            connect_fail_count += 1
            raise

    def subscribe(self, topic):
        self.pid = (self.pid % 65535) + 1
        pid = self.pid
        vh = bytes([(pid >> 8) & 0xFF, pid & 0xFF])
        pl = bytes([(len(topic) >> 8) & 0xFF, len(topic) & 0xFF]) + topic + b"\x00"
        pkt = b"\x82" + _encode_len(len(vh) + len(pl)) + vh + pl
        self._send_all(pkt)

        deadline = time.ticks_add(time.ticks_ms(), int(self.timeout * 1000))
        while time.ticks_diff(deadline, time.ticks_ms()) > 0:
            events = self.poller.poll(200)
            if not events:
                continue
            _, ev = events[0]
            err_mask = getattr(uselect, "POLLERR", 8) | getattr(uselect, "POLLHUP", 16)
            if ev & err_mask:
                raise OSError("Socket closed on SUBACK")
            if ev & uselect.POLLIN:
                op, pkt_pid = self._read_one_packet()
                if op == 0x90 and pkt_pid == pid:
                    return
        raise OSError("SUBACK timeout")

    def publish_qos1(self, topic, payload, retain=True):
        if isinstance(payload, str):
            payload = payload.encode()
        self.pid = (self.pid % 65535) + 1
        pid = self.pid
        header = 0x33 if retain else 0x32
        body = (
            bytes([(len(topic) >> 8) & 0xFF, len(topic) & 0xFF])
            + topic
            + bytes([(pid >> 8) & 0xFF, pid & 0xFF])
            + payload
        )
        pkt = bytes([header]) + _encode_len(len(body)) + body
        self._send_all(pkt)

        deadline = time.ticks_add(time.ticks_ms(), int(self.timeout * 1000))
        while time.ticks_diff(deadline, time.ticks_ms()) > 0:
            events = self.poller.poll(200)
            if not events:
                continue
            _, ev = events[0]
            err_mask = getattr(uselect, "POLLERR", 8) | getattr(uselect, "POLLHUP", 16)
            if ev & err_mask:
                raise OSError("Socket closed on PUBACK")
            if ev & uselect.POLLIN:
                op, pkt_pid = self._read_one_packet()
                if op == 0x40 and pkt_pid == pid:
                    return
        raise OSError("PUBACK timeout")

    def ping(self):
        self._send_all(b"\xc0\x00")
        self.ping_waiting = True
        self.ping_sent_ms = time.ticks_ms()

    def check_msg(self):
        if self.poller is None:
            return False
        events = self.poller.poll(0)
        if not events:
            return False
        _, ev = events[0]
        err_mask = getattr(uselect, "POLLERR", 8) | getattr(uselect, "POLLHUP", 16)
        if ev & err_mask:
            raise OSError("Socket closed by peer")
        if ev & uselect.POLLIN:
            self._read_one_packet()
            return True
        return False

    def close(self):
        if self.sock is not None:
            try:
                if self.poller is not None:
                    self.poller.unregister(self.sock)
            except Exception:
                pass
            try:
                self.sock.close()
            except Exception:
                pass
        self.sock = None
        self.poller = None
        self.ping_waiting = False


def close_mqtt_socket():
    global mqtt, mqtt_connected
    mqtt_connected = False
    if mqtt is not None:
        mqtt.close()
        mqtt = None
    gc.collect()


def connect_mqtt():
    global mqtt, mqtt_connected, last_ping_time, need_sync
    close_mqtt_socket()
    client_id = "esp32_vv_" + uos.urandom(4).hex()
    client = ReliableMQTT(client_id, MQTT_BROKER, port=MQTT_PORT, keepalive=25, timeout=2.5)
    client.set_callback(on_mqtt_message)
    try:
        client.connect()
        client.subscribe(TOPIC_CMD)
    except Exception:
        client.close()
        gc.collect()
        raise
    mqtt = client
    mqtt_connected = True
    last_ping_time = time.ticks_ms()
    need_sync = True
    print("[MQTT] Подключен:", client_id)


def ensure_mqtt(force=False):
    global last_reconnect_try
    if mqtt_connected and mqtt is not None:
        return True
    if not wlan.isconnected():
        return False
    now = time.ticks_ms()
    if not force and time.ticks_diff(now, last_reconnect_try) < 2500:
        return False
    last_reconnect_try = now
    try:
        connect_mqtt()
        return True
    except Exception as err:
        close_mqtt_socket()
        print("[MQTT ERROR] Подключение не удалось:", err)
        return False


def get_state_snapshot():
    if state == STATE_FREE:
        return "FREE", "CLOSED", "-", 0
    if state == STATE_OPEN_TO_PUT:
        return "OPEN_TO_PUT", "OPEN", local_key or "-", TIMEOUT_SEC
    if state == STATE_CHARGING:
        elapsed = time.ticks_diff(time.ticks_ms(), start_time) // 1000
        rem = max(0, TIMEOUT_SEC - elapsed)
        return "IN_USE", "CLOSED", local_key or "-", rem
    if state == STATE_OPEN_TO_TAKE:
        return "OPEN_TO_TAKE", "OPEN", local_key or "-", 0
    return "AWAITING_PICKUP", "CLOSED", local_key or "-", 0


def sync_state_to_mqtt():
    global need_sync, state_seq
    if not need_sync:
        return
    if not ensure_mqtt(force=False):
        return

    state_seq += 1
    st_str, door_str, key_str, rem_sec = get_state_snapshot()
    payload_json = ujson.dumps({
        "seq": state_seq,
        "status": st_str,
        "door": door_str,
        "key": key_str,
        "rem": rem_sec,
    })

    # Сбрасываем флаг ДО ожидания PUBACK, чтобы входящая команда успела снова выставить True
    need_sync = False
    try:
        mqtt.publish_qos1(TOPIC_STATE, payload_json, retain=True)
        print(f"[MQTT ACK] #{state_seq} STATE={st_str} DOOR={door_str} KEY={key_str}")
    except Exception as e:
        print("[MQTT] Нет PUBACK, переподключаем...", e)
        close_mqtt_socket()
        need_sync = True
        if ensure_mqtt(force=True):
            need_sync = False
            try:
                mqtt.publish_qos1(TOPIC_STATE, payload_json, retain=True)
                print(f"[MQTT ACK RETRY] #{state_seq} STATE={st_str}")
            except Exception as err:
                close_mqtt_socket()
                need_sync = True
                print("[MQTT ERROR] Повторная отправка не удалась:", err)


def generate_key():
    return "{:06d}".format(int.from_bytes(uos.urandom(3), "big") % 1000000)


def on_mqtt_message(topic, msg):
    global state, local_key, start_time, last_display_update, need_sync
    if topic != TOPIC_CMD:
        return

    val = msg.decode().strip()
    upper = val.upper()
    print(f"[MQTT CMD <-] {val}")

    if upper == "SYNC":
        need_sync = True

    elif upper.startswith("KEY:"):
        new_key = val.split(":", 1)[1].strip()
        if new_key and new_key != "-":
            local_key = new_key
            if state == STATE_FREE:
                state = STATE_OPEN_TO_PUT
                set_servo(90)
                relay.value(0)
                show_screen("KEY: " + local_key, "DOOR OPENED", "PUT YOUR PHONE")
            elif state in (STATE_OPEN_TO_PUT, STATE_CHARGING, STATE_AWAITING_PICKUP):
                show_screen("KEY UPDATED:", local_key)
        else:
            local_key = None
        need_sync = True

    elif upper == "OCCUPY" and state == STATE_FREE:
        set_servo(90)
        relay.value(0)
        state = STATE_OPEN_TO_PUT
        show_screen("DOOR OPENED", "PUT YOUR PHONE", "CLOSE TO START")
        need_sync = True

    elif upper == "CLOSE_DOOR" and state == STATE_OPEN_TO_PUT:
        set_servo(0)
        relay.value(1)
        start_time = time.ticks_ms()
        last_display_update = start_time
        state = STATE_CHARGING
        show_screen("IN USE", "TIME LEFT:", "01:30:00")
        need_sync = True

    elif upper == "FREE":
        set_servo(0)
        relay.value(0)
        local_key = None
        state = STATE_FREE
        show_screen("STATUS:", "BOX FREE")
        need_sync = True


# --- ПОДКЛЮЧЕНИЕ К WI-FI И MQTT ---
show_screen("CONNECTING", "WIFI...")
wlan = network.WLAN(network.STA_IF)
wlan.active(True)
wlan.connect(WIFI_SSID, WIFI_PASS)

while not wlan.isconnected():
    time.sleep_ms(200)

show_screen("CONNECTING", "MQTT...")
ensure_mqtt(force=True)

# --- СТАРТОВАЯ ИНИЦИАЛИЗАЦИЯ ---
relay.value(0)
set_servo(0)
state = STATE_FREE
local_key = None
show_screen("STATUS:", "BOX FREE")
need_sync = True
sync_state_to_mqtt()

# --- ОСНОВНОЙ ЦИКЛ (БЕСКОНЕЧНАЯ РАБОТА) ---
while True:
    now_ms = time.ticks_ms()

    # 1. Мгновенно обрабатываем кнопку с антидребезгом 200 мс (независимо от сети)
    btn_val = btn.value()
    btn_pressed = False
    if btn_val == 0 and last_btn_state == 1:
        if time.ticks_diff(now_ms, last_debounce_time) > 200:
            btn_pressed = True
            last_debounce_time = now_ms
    last_btn_state = btn_val

    if btn_pressed:
        print("[BTN] Нажатие в состоянии:", state)
        if state == STATE_FREE:
            # 1-е нажатие: генерация кода, открытие замка (таймер ждёт закрытия дверцы)
            local_key = generate_key()
            relay.value(0)
            set_servo(90)
            state = STATE_OPEN_TO_PUT
            show_screen("KEY: " + local_key, "DOOR OPENED", "PUT YOUR PHONE")
            need_sync = True

        elif state == STATE_OPEN_TO_PUT:
            # 2-е нажатие: закрытие двери, старт таймера и зарядки
            set_servo(0)
            relay.value(1)
            start_time = time.ticks_ms()
            last_display_update = start_time
            state = STATE_CHARGING
            show_screen("IN USE", "TIME LEFT:", "01:30:00")
            need_sync = True

        elif state in (STATE_CHARGING, STATE_AWAITING_PICKUP):
            # 3-е нажатие: отключение питания и открытие двери
            relay.value(0)
            set_servo(90)
            state = STATE_OPEN_TO_TAKE
            show_screen("KEY VERIFIED", "DOOR OPENED", "TAKE PHONE")
            need_sync = True

        elif state == STATE_OPEN_TO_TAKE:
            # 4-е нажатие: закрытие пустой ячейки, очистка ключа и отправка FREE
            set_servo(0)
            relay.value(0)
            local_key = None
            state = STATE_FREE
            show_screen("STATUS:", "BOX FREE")
            need_sync = True

    # 2. Автовосстановление Wi-Fi при обрыве
    if not wlan.isconnected():
        if mqtt_connected:
            close_mqtt_socket()
        if time.ticks_diff(now_ms, last_wifi_try) >= 5000:
            last_wifi_try = now_ms
            try:
                wlan.connect(WIFI_SSID, WIFI_PASS)
            except Exception:
                pass

    # 3. Гарантированная отправка состояния с подтверждением PUBACK
    if need_sync:
        sync_state_to_mqtt()

    # 4. Неблокирующая проверка входящих команд и PINGRESP
    if mqtt_connected and mqtt is not None:
        try:
            for _ in range(4):
                if not mqtt.check_msg():
                    break
        except Exception as e:
            print("[MQTT] Сокет закрыт, переподключаем...", e)
            close_mqtt_socket()
    else:
        ensure_mqtt(force=False)

    # 5. Контроль «зомби-сокета»: PING каждые 10 сек, ждём PINGRESP максимум 4 сек
    if mqtt_connected and mqtt is not None:
        if mqtt.ping_waiting:
            if time.ticks_diff(now_ms, mqtt.ping_sent_ms) >= 4000:
                print("[MQTT] Таймаут PINGRESP — сброс зависшего сокета")
                close_mqtt_socket()
        elif time.ticks_diff(now_ms, last_ping_time) >= 10000:
            last_ping_time = now_ms
            try:
                mqtt.ping()
            except Exception:
                close_mqtt_socket()

    # 6. Обновление экрана и таймера во время зарядки
    if state == STATE_CHARGING:
        if time.ticks_diff(now_ms, last_display_update) >= 1000:
            last_display_update = now_ms
            elapsed = time.ticks_diff(now_ms, start_time) // 1000

            if elapsed >= TIMEOUT_SEC:
                relay.value(0)
                state = STATE_AWAITING_PICKUP
                need_sync = True
                show_screen("CHARGING STOPPED", "AWAITING PICKUP", "POWER: OFF")
            else:
                rem = TIMEOUT_SEC - elapsed
                m, s = divmod(rem, 60)
                h, m = divmod(m, 60)
                time_str = "{:02d}:{:02d}:{:02d}".format(h, m, s)
                show_screen("IN USE", "TIME LEFT:", time_str)

    time.sleep_ms(20)