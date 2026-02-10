import asyncio
import logging
from typing import Optional

import aiohttp

from wb_welrok import config
from wb_welrok.mqtt_client import DEFAULT_BROKER_URL, MQTTClient
from wb_welrok.wb_mqtt_device import MQTTDevice

MQTT_PUBLISH_TIMEOUT = 5  # seconds
HTTP_REREQUEST_TIMEOUT = 20  # seconds
HTTP_FAILURE_THRESHOLD = 3
HTTP_INIT_RETRIES = 3
HTTP_PERIODIC_RETRIES = 1
HTTP_REQUEST_TIMEOUT = 20


logger = logging.getLogger(__name__)


class WelrokDevice:
    def __init__(self, properties):
        self._id = properties.get("device_id", "unknown_id")
        self._title = properties.get("device_title", "unknown_title")
        self._sn = properties.get("serial_number", "unknown_sn")
        self._ip = properties.get("device_ip", "unknown_ip")
        self._mqtt_enable = properties.get("mqtt_enable", True)
        self._mqtt_server_uri = properties.get("mqtt_server_uri", DEFAULT_BROKER_URL)
        self._url = (
            f"""http://{properties.get("device_ip")}/api.cgi""" if properties.get("device_ip") else None
        )
        self._wb_mqtt_device = None
        self._mqtt = MQTTClient(self._title, self._mqtt_server_uri)
        self._mqtt.user_data_set(self)
        self._subscribed_topics = set()
        self._mqtt_pub_base_topic = f"""{properties.get("inner_mqtt_pubprefix", "")}{properties.get("inner_mqtt_client_id", "")}/set/"""
        self._mqtt_sub_base_topic = f"""{properties.get("inner_mqtt_subprefix", "")}{properties.get("inner_mqtt_client_id", "")}/get/"""
        self._mqtt_data_topics = config.data_topics
        self._mqtt_settings_topics = config.settings_topics
        # Counters for consecutive failures (used to reduce noisy warnings)
        self._params_failures = 0
        self._telemetry_failures = 0
        self._session: Optional[aiohttp.ClientSession] = None

        logger.debug("Add device with id " + self._id + " and sn " + self._sn)

    def __repr__(self):
        return (
            f"WelrokDevice(id={self._id}, title={self._title}, "
            f"serial_number={self._sn}, ip={self._ip}, url={self._url}, "
            f"mqtt_pub_base_topic={self._mqtt_pub_base_topic}, "
            f"mqtt_sub_base_topic={self._mqtt_sub_base_topic}, "
            f"mqtt_data_topics={self._mqtt_data_topics}, "
            f"mqtt_settings_topics={self._mqtt_settings_topics})"
        )

    async def _create_session(self):
        if self._session is None or self._session.closed:
            timeout = aiohttp.ClientTimeout(total=HTTP_REREQUEST_TIMEOUT)
            connector = aiohttp.TCPConnector(limit_per_host=2, force_close=True, ssl=False)
            self._session = aiohttp.ClientSession(timeout=timeout, connector=connector, trust_env=True)

    async def close_session(self):
        if self._session and not self._session.closed:
            await self._session.close()

    @property
    def id(self):  # pylint: disable=C0103
        return self._id

    @property
    def sn(self):
        return self._sn

    @property
    def title(self):
        return self._title

    @property
    def ip(self):
        return self._ip

    def set_mqtt_device(self, mqtt_device: MQTTDevice):
        self._wb_mqtt_device = mqtt_device
        logger.debug("Set WB MQTT device for Welrok %s", self._id)

    def _get_session(self):
        """Create a new aiohttp session with timeout and connection limits"""
        timeout = aiohttp.ClientTimeout(total=HTTP_REREQUEST_TIMEOUT)
        connector = aiohttp.TCPConnector(
            limit_per_host=2,  # Max 2 simultaneous connections per host
            force_close=True,  # Force close connections for unstable devices
            enable_cleanup_closed=True,
            ssl=False,  # Explicitly disable SSL for HTTP connections
        )
        return aiohttp.ClientSession(timeout=timeout, connector=connector, trust_env=True)

    def parse_temperature_response(self, data: dict) -> dict:
        current_temp = {}
        try:
            for code, name in config.TEMP_CODES.items():
                key = f"t.{code}"
                if key in data:
                    val = int(data[key])
                    if code == 1:
                        current_temp[name] = str(round(val / 16, 2))
                    else:
                        current_temp[name] = str(round(val / 16))
            for code in range(3, 7):
                sensor = 1 if code in (3, 4) else 2
                key = f"f.{code}"
                if data.get(key) == "1":
                    current_temp[config.TEMP_CODES[sensor]] = "КЗ или обрыв цепи"
        except Exception as e:
            logger.exception(f"Error parsing temperature response: {e}")
        return current_temp

    def parse_response(self, raw_response, param):
        for par in raw_response["par"]:
            if par[0] == param:
                return par[2]
        return None

    def parse_device_params_state(self, data: dict) -> dict:
        state = {}
        try:
            for par in data.get("par", []):
                code = par[0]
                if code in config.PARAMS_CODES:
                    key = config.PARAMS_CODES[code]
                    val = par[2]
                    if key == "setTemp":
                        state[key] = config.PARAMS_CHOISE[key](par)
                    else:
                        state[key] = config.PARAMS_CHOISE[key](val)
            logger.debug(f"Parsed device params state: {state}")
        except Exception as e:
            logger.exception(f"Error parsing device params state: {e}")
        return state

    async def get_device_state(self, cmd: int, retries: int = 2, retry_delay: float = 0.5) -> Optional[dict]:
        await self._create_session()
        attempt = 0
        while attempt < retries:
            attempt += 1
            try:
                async with self._session.post(self._url, json={"cmd": cmd}) as resp:
                    if resp.status == 200:
                        res = await resp.json()
                        logger.debug(f"Device {self._id} get_device_state ({res})")
                        return res
                    else:
                        logger.error(f"HTTP error {resp.status} for device {self._id}")
                        return None
            except (asyncio.TimeoutError, aiohttp.ClientError) as e:
                logger.warning(f"Attempt {attempt} failed for device {self._id}: {e}")
            if attempt < retries:
                await asyncio.sleep(retry_delay * (2 ** (attempt - 1)))
        return None

    async def set_current_temp(self, current_temp: dict):
        for key, value in current_temp.items():
            display_value = value if "open" in str(value).lower() else f"{value} \u00b0C"
            logger.debug(f"Welrok device {self._id} setting readonly temp {key} = {display_value}")
            if self._wb_mqtt_device:
                self._wb_mqtt_device.set_readonly(key, display_value)

    async def set_current_control_state(self, current_states: dict):
        for key, value in current_states.items():
            logger.debug(f"Welrok device {self._id} updating control {key} with value {value}")
            if self._wb_mqtt_device:
                self._wb_mqtt_device.update(key, str(value))

    def _on_mqtt_connect(self, client, userdata, flags, rc):
        if rc != 0:
            logger.warning("MQTT connect failed with rc=%s for device %s", rc, self._id)
            return

        logger.info("MQTT connected for device %s, subscribing topics", self._id)
        for topic in self._mqtt_data_topics:
            full = self._mqtt_sub_base_topic + topic
            try:
                client.message_callback_add(full, self.mqtt_data_callback)
                result, mid = client.subscribe(full, qos=1)
                if result == 0:
                    self._subscribed_topics.add(full)
                    logger.debug("Subscribed to %s (mid=%s) for %s", full, mid, self._id)
                else:
                    logger.warning("Subscribe returned %s for %s", result, full)
            except Exception:
                logger.exception("Failed to subscribe to %s for device %s", full, self._id)

    async def run(self):
        mqtt_started = False
        try:
            if not mqtt_started:
                self._mqtt.on_connect = self._on_mqtt_connect
                self._mqtt.on_disconnect = self._on_mqtt_disconnect
                self._mqtt.start()
                mqtt_started = True

            while True:
                try:
                    params_response = await self.get_device_state(
                        config.CMD_CODES["params"], retries=HTTP_PERIODIC_RETRIES
                    )
                    if params_response is not None:
                        self._params_failures = 0
                        device_controls_state = self.parse_device_params_state(params_response) or {}
                        if self._wb_mqtt_device:
                            control_states = {
                                "Power": int(device_controls_state.get("powerOff", 0)),
                                "Bright": int(device_controls_state.get("bright", 0) * 10),
                                "Set temperature": int(device_controls_state.get("setTemp", 0)),
                                "Set temperature value": int(device_controls_state.get("setTemp", 0)),
                            }
                            self._wb_mqtt_device.set_readonly(
                                "Current mode",
                                config.MODE_NAMES_TRANSLATE.get(device_controls_state.get("mode", ""), ""),
                            )
                            await self.set_current_control_state(control_states)
                    else:
                        self._params_failures = min(self._params_failures + 1, HTTP_FAILURE_THRESHOLD)
                        if self._params_failures == HTTP_FAILURE_THRESHOLD:
                            logger.warning(
                                f"Device {self._id} params unavailable (failed {self._params_failures} times)"
                            )
                        else:
                            logger.debug(
                                f"Device {self._id} transient params error ({self._params_failures}/{HTTP_FAILURE_THRESHOLD})"
                            )

                    telemetry = await self.get_device_state(
                        config.CMD_CODES["telemetry"], retries=HTTP_PERIODIC_RETRIES
                    )
                    if telemetry is not None:
                        self._telemetry_failures = 0
                        if self._wb_mqtt_device:
                            self._wb_mqtt_device.set_readonly("Load", self.get_load(telemetry))
                            await self.set_current_temp(self.parse_temperature_response(telemetry))
                    else:
                        self._telemetry_failures = min(self._telemetry_failures + 1, HTTP_FAILURE_THRESHOLD)
                        if self._telemetry_failures == HTTP_FAILURE_THRESHOLD:
                            logger.warning(
                                f"Device {self._id} telemetry unavailable (failed {self._telemetry_failures} times)"
                            )
                        else:
                            logger.debug(
                                f"Device {self._id} transient telemetry error ({self._telemetry_failures}/{HTTP_FAILURE_THRESHOLD})"
                            )

                    await asyncio.sleep(10)

                except asyncio.CancelledError:
                    logger.info(f"Welrok device {self._id} run task cancelled")
                    break

                except Exception:
                    logger.exception(f"Error in device {self._id} run loop, restarting after delay")
                    try:
                        self._mqtt.stop()
                    except Exception:
                        logger.exception(f"Error stopping MQTT for device {self._id}")
                    await asyncio.sleep(10)

        finally:
            try:
                self.unsubscribe_all()
            except Exception:
                logger.exception(f"Exception when unsubscribing for device {self._id}")

            if self._mqtt is not None:
                try:
                    self._mqtt.stop()
                except Exception:
                    logger.exception(f"Error while stopping mqtt client for device {self._id}")

    def _on_mqtt_disconnect(self, client, userdata, rc):
        if rc != 0:
            logger.warning(f"Unexpected MQTT disconnect for device {self._id}, rc={rc}")
        else:
            logger.info(f"MQTT disconnected normally for device {self._id}")

    def get_load(self, telemetry: dict) -> str:
        try:
            load_val = telemetry.get("f.0", "0")
            return config.PARAMS_CHOISE["load"](load_val)
        except Exception as e:
            logger.exception(f"Error getting load from telemetry: {e}")
            return "off"

    def mqtt_data_callback(self, _, __, msg):
        if self._wb_mqtt_device is None:
            logger.warning("MQTT callback received but wb_mqtt_device is None for %s", self._id)
            return

        try:
            topic_name = [
                config.INNER_TOPICS.get(i) for i in msg.topic.split("/") if config.INNER_TOPICS.get(i)
            ]
            if len(topic_name) > 0:
                msg = msg.payload.decode("utf-8")
                if topic_name[0] == "Power":
                    msg = "0" if msg == "1" else "1"
                elif topic_name[0] == "Load":
                    msg = "Включено" if msg == "1" else "Выключено"
                elif "temperature" in topic_name[0]:
                    if "open" not in msg and "Set " not in topic_name[0]:
                        if topic_name[0] != "Floor temperature":
                            msg = str(round(float(msg)))
                        msg = f"{msg} \u00b0C"
                elif topic_name[0] == "Current mode":
                    msg = config.MODE_NAMES_TRANSLATE[msg]
                self._wb_mqtt_device.update(topic_name[0], msg)
        except Exception:
            logger.exception("Error in mqtt_data_callback for device %s", self._id)

    async def send_command_http(self, data: dict):
        if not self._url:
            logging.error(f"HTTP URL не задан для устройства {self._id}")
            return None

        timeout = aiohttp.ClientTimeout(total=HTTP_REQUEST_TIMEOUT)
        connector = aiohttp.TCPConnector(limit_per_host=2, force_close=True, ssl=False)

        try:
            async with aiohttp.ClientSession(timeout=timeout, connector=connector, trust_env=True) as session:
                async with session.post(self._url, json=data) as response:
                    if response.status == 200:
                        resp_json = await response.json()
                        logging.debug(f"HTTP команда для {self._id} выполнена успешно: {resp_json}")
                        return resp_json
                    else:
                        logging.error(f"HTTP ошибка {response.status} при отправке команды для {self._id}")
                        return None
        except asyncio.TimeoutError:
            logging.error(
                f"HTTP запрос таймаут для устройства {self._id} после {HTTP_REQUEST_TIMEOUT} секунд"
            )
        except aiohttp.ClientError as e:
            logging.error(f"HTTP ошибка клиента для устройства {self._id}: {e}")
        except Exception:
            logging.exception(f"Неожиданная ошибка при отправке HTTP команды для {self._id}")
        return None

    async def set_power(self, power: int):
        topic = self._mqtt_pub_base_topic + config.PARAMS_CODES[125]
        if self._mqtt_enable:
            try:
                await asyncio.wait_for(
                    asyncio.to_thread(self._mqtt.publish, topic, str(power)),
                    timeout=MQTT_PUBLISH_TIMEOUT,
                )
                logging.info(f"Power set to {power} on device {self._id} via MQTT")
            except asyncio.TimeoutError:
                logging.warning(f"MQTT publish timeout on set_power for device {self._id}")
            except Exception:
                logging.exception(f"Error publishing set_power for device {self._id}")
        else:
            command = {"sn": self._sn, "par": [[125, 7, str(power)]]}
            await self.send_command_http(command)

    async def set_temp(self, temp: int):
        if not (5 <= temp <= 45):
            logging.warning(f"Temperature {temp} out of range for device {self._id}")
            return
        topic_temp = self._mqtt_pub_base_topic + config.PARAMS_CODES[31]
        topic_mode = self._mqtt_pub_base_topic + config.PARAMS_CODES[2]
        manual_mode = str(config.MODE_CODES_REVERSE["Manual"])
        if self._mqtt_enable:
            try:
                await asyncio.wait_for(
                    asyncio.to_thread(self._mqtt.publish, topic_temp, str(temp)),
                    timeout=MQTT_PUBLISH_TIMEOUT,
                )
                await asyncio.wait_for(
                    asyncio.to_thread(self._mqtt.publish, topic_mode, manual_mode),
                    timeout=MQTT_PUBLISH_TIMEOUT,
                )
                logging.info(f"Temperature set to {temp} on device {self._id} via MQTT")
            except asyncio.TimeoutError:
                logging.warning(f"MQTT publish timeout on set_temp for device {self._id}")
            except Exception:
                logging.exception(f"Error publishing set_temp for device {self._id}")
        else:
            command = {
                "sn": self._sn,
                "par": [[2, 2, manual_mode], [31, 3, str(temp * 10)]],
            }
            await self.send_command_http(command)

    async def set_mode(self, new_mode: str):
        try:
            mode_list = [
                config.MODE_CODES_REVERSE.get(m)
                for m in new_mode.split("/")
                if config.MODE_CODES_REVERSE.get(m) is not None
            ]
            if not mode_list:
                logging.debug(f"No valid mode mapping for {new_mode} on device {self._id}")
                return
            mode = str(mode_list[0])
            topic = self._mqtt_pub_base_topic + config.PARAMS_CODES[2]
            if self._mqtt_enable:
                try:
                    await asyncio.wait_for(
                        asyncio.to_thread(self._mqtt.publish, topic, mode),
                        timeout=MQTT_PUBLISH_TIMEOUT,
                    )
                    logging.info(f"Mode set to {mode} on device {self._id} via MQTT")
                except asyncio.TimeoutError:
                    logging.warning(f"MQTT publish timeout on set_mode for device {self._id}")
                except Exception:
                    logging.exception(f"Error publishing set_mode for device {self._id}")
            else:
                command = {"sn": self._sn, "par": [[2, 2, mode]]}
                await self.send_command_http(command)
        except Exception:
            logging.exception(f"Error in set_mode for device {self._id}")

    async def set_bright(self, bright: int):
        if not (0 <= bright <= 10):
            logging.warning(f"Brightness {bright} out of range for device {self._id}")
            return
        topic = self._mqtt_pub_base_topic + config.PARAMS_CODES[23]
        if self._mqtt_enable:
            try:
                await asyncio.wait_for(
                    asyncio.to_thread(self._mqtt.publish, topic, str(bright)),
                    timeout=MQTT_PUBLISH_TIMEOUT,
                )
                logging.info(f"Brightness set to {bright} on device {self._id} via MQTT")
            except asyncio.TimeoutError:
                logging.warning(f"MQTT publish timeout on set_bright for device {self._id}")
            except Exception:
                logging.exception(f"Error publishing set_bright for device {self._id}")
        else:
            command = {"sn": self._sn, "par": [[23, 2, str(bright)]]}
            await self.send_command_http(command)

    def unsubscribe_all(self):
        if not hasattr(self, "_subscribed_topics") or self._mqtt is None:
            return
        for topic in list(self._subscribed_topics):
            try:
                self._mqtt.message_callback_remove(topic)
            except Exception:
                logger.exception(f"Failed to remove message callback for {topic} on device {self._id}")
            try:
                self._mqtt.unsubscribe(topic)
            except Exception:
                logger.exception(f"Failed to unsubscribe {topic} for device {self._id}")
            self._subscribed_topics.discard(topic)
