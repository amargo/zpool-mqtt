import json
import logging
import os
import signal
import sys
import time
import paho.mqtt.client as mqtt

from dataclasses import dataclass
from math import ceil
from pathlib import Path
from subprocess import run, CalledProcessError
from typing import Any, Dict, List, Optional

ZPOOL_CMD = os.getenv("ZPOOL_CMD", "/usr/sbin/zpool")
ZPOOL_ARGS = ["list", "-Hp"]

MQTT_TOPIC_BASE = "zpool"
UPDATE_INTERVAL_DEFAULT = 600

MODULE_FILE = Path(__file__)
_LOGGER = logging.getLogger(MODULE_FILE.name)

def configure_logging(debug: bool) -> None:
    fmt = "%(asctime)s %(levelname)s %(name)s: %(message)s"
    level = logging.DEBUG if debug else logging.INFO
    logging.basicConfig(stream=sys.stdout, format=fmt, level=level)

# ---------- ZFS read ----------

def _check_zpool_binary() -> None:
    if not Path(ZPOOL_CMD).exists():
        raise FileNotFoundError(f"zpool binary not found at {ZPOOL_CMD}")

def get_zpool_dict(timeout_sec: int = 5) -> Dict[str, Dict[str, Any]]:
    """Run `zpool list -Hp` and parse into a dict keyed by pool name."""
    _check_zpool_binary()
    try:
        proc = run([ZPOOL_CMD, *ZPOOL_ARGS], capture_output=True, text=True, timeout=timeout_sec, check=True)
    except CalledProcessError as e:
        _LOGGER.error("zpool command failed: rc=%s stderr=%s", e.returncode, e.stderr.strip())
        return {}
    except Exception as e:
        _LOGGER.exception("zpool command exception: %s", e)
        return {}

    lines = [ln for ln in proc.stdout.splitlines() if ln.strip()]
    if not lines:
        _LOGGER.warning("zpool output is empty")
        return {}

    columns = ["name", "size", "alloc", "free", "ckpoint", "expandsz", "frag", "cap", "dedup", "health", "altroot"]
    health_map = {"ONLINE": 0, "DEGRADED": 11, "OFFLINE": 21, "UNAVAIL": 22, "FAULTED": 23, "REMOVED": 24}

    out: Dict[str, Dict[str, Any]] = {}
    for ln in lines:
        parts = ln.split("\t")
        if len(parts) != len(columns):
            _LOGGER.warning("unexpected zpool line format (got %d cols): %r", len(parts), ln)
            continue
        pool = dict(zip(columns, parts))

        # Parse numerics
        for key in ["size", "alloc", "free", "frag", "cap"]:
            val = pool.get(key, "-")
            if val not in ("-", ""):
                try:
                    pool[key] = int(val)
                except ValueError:
                    _LOGGER.debug("non-int %s=%r", key, val)

        ded = pool.get("dedup", "-")
        try:
            pool["dedup"] = float(ded) if ded not in ("-", "") else 1.0
        except ValueError:
            _LOGGER.debug("non-float dedup=%r", ded)
            pool["dedup"] = 1.0

        raw_h = pool.get("health", "UNAVAIL")
        pool["health_text"] = raw_h
        pool["health"] = health_map.get(raw_h, 99)  # 99 = unknown

        out[pool["name"]] = pool
    return out

# ---------- HA Discovery helper ----------

@dataclass(frozen=True)
class SensorDef:
    key: str
    name: str
    unit: Optional[str] = None
    device_class: Optional[str] = None
    state_class: Optional[str] = "measurement"
    icon: Optional[str] = None

# Default sensors mapped from zpool keys
DEFAULT_SENSORS: List[SensorDef] = [
    SensorDef("size",  "Size",  "B",  "data_size"),
    SensorDef("alloc", "Allocated", "B", "data_size"),
    SensorDef("free",  "Free",  "B",  "data_size"),
    SensorDef("frag",  "Fragmentation", "%"),
    SensorDef("cap",   "Capacity", "%"),
    SensorDef("dedup", "Dedup Ratio"),
    # We'll publish health (numeric code) + health_text as attribute
    SensorDef("health", "Health Code"),
]

def build_sensor_payload(pool: str, state_topic: str, avail_topic: str, sd: SensorDef) -> Dict[str, Any]:
    """Create a Home Assistant MQTT Discovery payload for a single sensor."""
    unique = f"zpool_{pool}_{sd.key}"
    payload: Dict[str, Any] = {
        "name": f"{pool} {sd.name}",
        "unique_id": unique,
        "state_topic": state_topic,
        "availability_topic": avail_topic,
        "json_attributes_topic": state_topic,
        "value_template": f"{{{{ value_json.{sd.key} }}}}",
        "expire_after": ceil(1.5 * UPDATE_INTERVAL_DEFAULT),
        "device": {
            "identifiers": [f"zpool_{pool}"],
            "manufacturer": "zpool",
            "model": "list",
            "name": pool,
        },
    }
    if sd.unit:
        payload["unit_of_measurement"] = sd.unit
    if sd.device_class:
        payload["device_class"] = sd.device_class
    if sd.state_class:
        payload["state_class"] = sd.state_class
    if sd.icon:
        payload["icon"] = sd.icon
    return payload

# ---------- MQTT client ----------

class HaMqttClient:
    """Persistent MQTT client with Last Will retained to availability topic."""
class HaMqttClient:
    def __init__(self, base_topic: str, host: str, port: int, auth: Optional[dict]):
        self._base = base_topic
        self._status = f"{self._base}/availability"

        client_id = f"zpool-mqtt-{base_topic.replace('/', '-')}"
        self._client = mqtt.Client(client_id=client_id, protocol=mqtt.MQTTv311)

        if auth:
            self._client.username_pw_set(auth.get("username"), auth.get("password"))
        self._client.will_set(self._status, "offline", retain=True)
        self._client.connect(host, port, keepalive=30)
        self._client.loop_start()

    def publish_online(self) -> None:
        self._client.publish(self._status, "online", retain=True)

    def publish_offline(self) -> None:
        self._client.publish(self._status, "offline", retain=True)

    def publish_json(self, rel: str, obj: Any, retain: bool = False) -> None:
        topic = f"{self._base}/{rel}" if rel else self._base
        self._client.publish(topic, json.dumps(obj, sort_keys=True), retain=retain)

    def publish_discovery(self, sensors: List[Dict[str, Any]]) -> None:
        for topic, payload in sensors:
            self._client.publish(topic, json.dumps(payload, sort_keys=True), retain=True)

    def stop(self) -> None:
        try:
            self.publish_offline()
        finally:
            self._client.loop_stop()
            self._client.disconnect()

# ---------- Main ----------

def main():
    debug = os.getenv("DEBUG", "0") == "1"
    configure_logging(debug)

    mqtt_host = os.getenv("MQTT_HOST")
    mqtt_port = int(os.getenv("MQTT_PORT", "1883"))
    mqtt_user = os.getenv("MQTT_USER")
    mqtt_password = os.getenv("MQTT_PASSWORD")
    update_interval = int(os.getenv("ZPOOL_INTERVAL", str(UPDATE_INTERVAL_DEFAULT)))

    if not mqtt_host:
        _LOGGER.error("MQTT_HOST is required")
        sys.exit(2)

    mqtt_auth = {"username": mqtt_user, "password": mqtt_password} if mqtt_user and mqtt_password else None

    # Initial read
    pools = get_zpool_dict()
    if not pools:
        _LOGGER.warning("No zpools yet; will retry in loop")

    # Maintain persistent client per pool
    clients: Dict[str, HaMqttClient] = {}

    def _cleanup(*_):
        _LOGGER.info("Stopping...")
        for c in list(clients.values()):
            try: c.stop()
            except Exception: pass
        sys.exit(0)

    signal.signal(signal.SIGINT, _cleanup)
    signal.signal(signal.SIGTERM, _cleanup)

    # Bootstrap existing pools: publish discovery
    for pool_name in pools.keys():
        base = f"{MQTT_TOPIC_BASE}/{pool_name}"
        cli = HaMqttClient(base, mqtt_host, mqtt_port, mqtt_auth)
        state_topic = f"{base}/zpool"
        avail_topic = f"{base}/availability"

        discovery_msgs = []
        for sd in DEFAULT_SENSORS:
            disc_topic = f"homeassistant/sensor/zpool_{pool_name}/{sd.key}/config"
            discovery_msgs.append((disc_topic, build_sensor_payload(pool_name, state_topic, avail_topic, sd)))

        cli.publish_discovery(discovery_msgs)
        cli.publish_online()
        clients[pool_name] = cli

    # Main loop
    while True:
        data = get_zpool_dict()
        if not data:
            _LOGGER.warning("No zpool data (check /dev/zfs device & permissions); retrying...")
            time.sleep(5)
            continue        

        # Remove disappeared pools
        for pool_name in list(clients.keys()):
            if pool_name not in data:
                _LOGGER.info("Pool disappeared: %s", pool_name)
                try:
                    clients[pool_name].publish_offline()
                    clients[pool_name].stop()
                finally:
                    del clients[pool_name]

        # Add new pools + discovery
        for pool_name in data.keys():
            if pool_name not in clients:
                _LOGGER.info("New pool detected: %s", pool_name)
                base = f"{MQTT_TOPIC_BASE}/{pool_name}"
                cli = HaMqttClient(base, mqtt_host, mqtt_port, mqtt_auth)
                state_topic = f"{base}/zpool"
                avail_topic = f"{base}/availability"
                discovery_msgs = []
                for sd in DEFAULT_SENSORS:
                    disc_topic = f"homeassistant/sensor/zpool_{pool_name}/{sd.key}/config"
                    discovery_msgs.append((disc_topic, build_sensor_payload(pool_name, state_topic, avail_topic, sd)))
                cli.publish_discovery(discovery_msgs)
                cli.publish_online()
                clients[pool_name] = cli

        # Publish states
        for pool_name, values in data.items():
            base = f"{MQTT_TOPIC_BASE}/{pool_name}"
            cli = clients.get(pool_name)
            if not cli:
                continue
            # Publish the full JSON once; HA sensors use value_template to pick fields
            cli.publish_json("zpool", values, retain=False)
            cli.publish_online()

        # Sleep w/ responsive exit
        for _ in range(update_interval * 2):
            time.sleep(0.5)

# ---------- Entrypoint ----------

if __name__ == "__main__":
    main()
