import asyncio
import json
import logging
import traceback
from typing import Optional, TYPE_CHECKING, Callable

import aiohttp

from wb_welrok import config
from wb_welrok.mqtt_client import MQTTClient
from wb_welrok.wb_mqtt_device import MQTTDevice

if TYPE_CHECKING:
    from wb_welrok.schemas import DeviceConfig

MQTT_PUBLISH_TIMEOUT = 5  # seconds
HTTP_REREQUEST_TIMEOUT = 20  # seconds
HTTP_FAILURE_THRESHOLD = 3
HTTP_INIT_RETRIES = 3
HTTP_PERIODIC_RETRIES = 1
HTTP_REQUEST_TIMEOUT = 20


logger = logging.getLogger(__name__)


class MsgProcessor:

    def __init__(self):
         self.config = config

    def Power(self, msg) -> str:
        return "0" if msg == "1" else "1"

    def Load(self, msg) -> str:
        return "Включено" if msg == "1" else "Выключено"

    def temperature(self, msg, topic) -> Optional[str]:
        if "open" not in msg and "Set " not in topic:
            if topic != "Floor temperature":
                msg = str(round(float(msg)))
            msg = f"{msg} \u00B0C"
            return msg

    def Current_mode(self, msg) -> Optional[str]:
        return self.config.MODE_NAMES_TRANSLATE.get(msg, msg)

    def process(self, topic, msg):
        method_name = topic.replace(" ", "_")
        if "temperature" in topic:
            return self.temperature(msg, topic)
        method = getattr(self, method_name, None)
        if method:
            return method(msg)
        return None

class WelrokDataParser:

    def __init__(self):
        self.msg_processor = MsgProcessor()
    
    def get_inner_topic(self, key):
        return config.INNER_TOPICS.get(key)
   
    def mqtt_msg_parse(self, mqtt_msg):
        topic_name = self.get_inner_topic(mqtt_msg.topic.split('/')[-1])
        if topic_name:
            mqtt_msg = mqtt_msg.payload.decode("utf-8")
            return topic_name, self.msg_processor.process(topic_name, mqtt_msg)
        return None, None

    def parse_load(self, telemetry):
        try:
            load_val = telemetry.get("f.0", "0")
            return config.PARAMS_CHOISE["load"](load_val)
        except Exception as e:
            logger.exception(f"Error getting load from telemetry: {e}")
            return "off"
        
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
    
    def format_command(self, param: config.ParamCode, value, http_code: config.HttpCode = None):
        mqtt_data = [(config.PARAMS_CODES[param.value], value)]
        if param == config.ParamCode.TEMP and isinstance(value, (int, float)):
            scaled_value = str(int(value * 10))
        else:
            scaled_value = str(value)
        http_params = [[param.value, http_code.value, scaled_value]] if http_code else []
        return mqtt_data, http_params 

    def format_bright(self, bright):
        if bright > 0:
            if bright < 10:
                bright = 1
            elif bright == 100:
                bright = 9
            else:
                bright = bright // 10
            return bright
        return 0

class WelrokDevice:
    def __init__(self, properties: 'DeviceConfig', mqtt_server_uri, root_mqtt, temp_range=(5,45), bright_range=(0,10)):
        self._root_mqtt = root_mqtt
        self._id = properties.device_id
        self._title = properties.device_title
        self._sn = properties.serial_number
        self._ip = properties.device_ip
        self._mqtt_enable = properties.mqtt_enable
        self._mqtt_server_uri = mqtt_server_uri
        self._url = f"http://{properties.device_ip}/api.cgi"
        self._wb_mqtt_device = None
        self._mqtt = MQTTClient(self._title, self._mqtt_server_uri)
        self._mqtt.user_data_set(self)
        self._subscribed_topics = set()
        self._mqtt_pub_base_topic = f"{properties.inner_mqtt_pubprefix}{properties.inner_mqtt_client_id}/set/"
        self._mqtt_sub_base_topic = f"{properties.inner_mqtt_subprefix}{properties.inner_mqtt_client_id}/get/"
        self._mqtt_data_topics = config.data_topics
        self._mqtt_settings_topics = config.settings_topics
        self._data_parser = WelrokDataParser()
        self._temp_check = lambda x: temp_range[0] <= x <= temp_range[1]
        self._bright_check = lambda x: bright_range[0] <= x <= bright_range[1]

        # Counters for consecutive failures (used to reduce noisy warnings)
        self._params_failures = 0
        self._telemetry_failures = 0
        self.__session: Optional[aiohttp.ClientSession] = None
        
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

    async def close_session(self):
        if self.__session and not self.__session.closed:
            await self.__session.close() 
    
    @property
    def _session(self):
        if self.__session is None or self.__session.closed:
            timeout = aiohttp.ClientTimeout(total=HTTP_REREQUEST_TIMEOUT)
            connector = aiohttp.TCPConnector(
                limit_per_host=2,
                force_close=True,
                enable_cleanup_closed=True,
                ssl=False,
            )
            self.__session = aiohttp.ClientSession(timeout=timeout, connector=connector, trust_env=True)
        return self.__session
    
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

    def set_mqtt_device(self, device_controls_state, telemetry):
        device_controls_state.update({
            "read_only_temp": self._data_parser.parse_temperature_response(telemetry),
            "load": self.get_load(telemetry),
        })
        self._wb_mqtt_device = MQTTDevice(self._root_mqtt, device_controls_state, self)
        logger.debug("Set WB MQTT device for Welrok %s", self._id)

    async def get_device_state(self, cmd: int, retries: int = 2, retry_delay: float = 0.5) -> Optional[dict]:
        attempt = 0
        while attempt < retries:
            attempt += 1
            try:
                async with self._session.post(self._url, json={"cmd": cmd}) as resp:
                    if resp.status == 200:
                        return await resp.json()
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
            display_value = value if "open" in str(value).lower() else f"{value} \u00B0C"
            logger.debug(f"Welrok device {self._id} setting readonly temp {key} = {display_value}")
            if self._wb_mqtt_device:
                self._wb_mqtt_device.set_readonly(key, display_value)

    async def set_current_control_state(self, current_states: dict):
        for key, value in current_states.items():
            logger.debug("Welrok device %s updating control %s with value %s", self.id, key, value)
            if self._wb_mqtt_device:
                self._wb_mqtt_device.update(key, str(value))

    def _on_mqtt_connect(self, client, _, __, rc):
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

    def control_states(self, device_controls_state):
        return {
            "Power": int(device_controls_state.get("powerOff", 0)),
            "Bright": int(device_controls_state.get("bright", 0) * 10),
            "Set temperature": int(device_controls_state.get("setTemp", 0)),
            "Set temperature value": int(device_controls_state.get("setTemp", 0)),
        }
    
    async def set_params(self, device_controls_state, telemetry):
        await self.set_current_control_state(self.control_states(device_controls_state))
        await self.set_current_temp(self._data_parser.parse_temperature_response(telemetry))
        self._wb_mqtt_device.set_readonly(
            "Current mode",
            config.MODE_NAMES_TRANSLATE.get(device_controls_state.get("mode", ""), ""),
        )
        self._wb_mqtt_device.set_readonly("Load", self.get_load(telemetry))

    async def run(self):
        try:
            self._mqtt.on_connect = self._on_mqtt_connect
            self._mqtt.on_disconnect = self._on_mqtt_disconnect
            self._mqtt.start()
            while True:
                try:
                    telemetry = await self.get_device_state(config.CMD_CODES["telemetry"], retries=HTTP_PERIODIC_RETRIES)
                    params_response = await self.get_device_state(config.CMD_CODES["params"], retries=HTTP_PERIODIC_RETRIES)
                    if params_response is not None and telemetry is not None:
                        self._params_failures = 0
                        self._telemetry_failures = 0
                        device_controls_state = self._data_parser.parse_device_params_state(params_response) or {}
                        if self._wb_mqtt_device:
                            await self.set_params(device_controls_state, telemetry)
                        else:
                            self.set_mqtt_device(device_controls_state, telemetry)
                    else:
                        self._params_failures = min(self._params_failures + 1, HTTP_FAILURE_THRESHOLD)
                        if self._params_failures == HTTP_FAILURE_THRESHOLD:
                            logger.warning(f"Device {self._id} params unavailable (failed {self._params_failures} times)")
                        else:
                            logger.debug(f"Device {self._id} transient params error ({self._params_failures}/{HTTP_FAILURE_THRESHOLD})")
                        self._telemetry_failures = min(self._telemetry_failures + 1, HTTP_FAILURE_THRESHOLD)
                        if self._telemetry_failures == HTTP_FAILURE_THRESHOLD:
                            logger.warning(f"Device {self._id} telemetry unavailable (failed {self._telemetry_failures} times)")
                        else:
                            logger.debug(f"Device {self._id} transient telemetry error ({self._telemetry_failures}/{HTTP_FAILURE_THRESHOLD})")

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

    def _on_mqtt_disconnect(self, _, __, rc):
        if rc != 0:
            logger.warning(f"Unexpected MQTT disconnect for device {self._id}, rc={rc}")
        else:
            logger.info(f"MQTT disconnected normally for device {self._id}") 


    def get_load(self, telemetry: dict) -> str:
        return self._data_parser.parse_load(telemetry)


    def mqtt_data_callback(self, _, __, msg):
        if self._wb_mqtt_device is None:
            logger.warning("MQTT callback received but wb_mqtt_device is None for %s", self._id)
            return
        try:
            topic_name, msg = self._data_parser.mqtt_msg_parse(msg)
            if topic_name and msg:
                self._wb_mqtt_device.update(topic_name, msg)
        except Exception:
            logger.exception("Error in mqtt_data_callback for device %s", self._id)

    async def send_command_http(self, data: dict):
        if not self._url:
            logger.error(f"HTTP URL не задан для устройства {self._id}")
            return None
        try:
            async with self._session.post(self._url, json=data) as response:
                if response.status == 200:
                    resp_json = await response.json()
                    logger.debug(f"HTTP команда для {self._id} выполнена успешно: {resp_json}")
                    return resp_json
                else:
                    logger.error(f"HTTP ошибка {response.status} при отправке команды для {self._id}")
                    return None
        except asyncio.TimeoutError:
            logger.error(f"HTTP запрос таймаут для устройства {self._id} после {HTTP_REQUEST_TIMEOUT} секунд")
        except aiohttp.ClientError as e:
            logger.error(f"HTTP ошибка клиента для устройства {self._id}: {e}")
        except Exception:
            logger.exception(f"Неожиданная ошибка при отправке HTTP команды для {self._id}")

    async def send_command_mqtt(self, topic, value):
        try:
            await asyncio.wait_for(
                asyncio.to_thread(self._mqtt.publish, topic, str(value)),
                timeout=MQTT_PUBLISH_TIMEOUT,
            )
            logger.info(f"{topic} set to {value} on device {self._id} via MQTT")
        except asyncio.TimeoutError:
            logger.warning(f"MQTT publish timeout on set_power for device {self._id}")
        except Exception:
            logger.exception(f"Error publishing set_power for device {self._id}")

    async def send_command(self, mqtt_data=None, http_params=None):
        try:
            if self._mqtt_enable and mqtt_data:
                for topic_suffix, value in mqtt_data:
                    topic = self._mqtt_pub_base_topic + topic_suffix
                    await self.send_command_mqtt(topic, value)
            elif http_params is not None:
                command = {"sn": self._sn, "par": http_params}
                await self.send_command_http(command)
            else:
                logger.error(f"Invalid command parameters for device {self._id}")
        except Exception:
             logger.exception(f"Error sending command for device {self._id}")

    async def set_power(self, power: int):
        mqtt_data, http_params = self._data_parser.format_command(config.ParamCode.POWER, power, config.HttpCode.POWER)
        await self.send_command(mqtt_data, http_params)  

    async def set_temp(self, temp: int):
        if not self._temp_check(temp):
            logger.warning(f"Temperature {temp} out of range for device {self._id}")
            return
        manual_mode = str(config.ModeCode.MANUAL.value)
        mqtt_data_mode, http_params_mode = self._data_parser.format_command(config.ParamCode.MODE, manual_mode, config.HttpCode.MODE)
        mqtt_data_temp, http_params_temp = self._data_parser.format_command(config.ParamCode.TEMP, temp, config.HttpCode.TEMP)
        mqtt_data = mqtt_data_mode + mqtt_data_temp
        http_params = http_params_mode + http_params_temp
        await self.send_command(mqtt_data=mqtt_data, http_params=http_params)

    async def set_mode(self, new_mode: str, topic):
        if "Manual/on" in topic:
            new_mode = "1"
        elif "Auto/on" in topic:
            new_mode = "0"
        mqtt_data, http_params = self._data_parser.format_command(config.ParamCode.MODE, new_mode, config.HttpCode.MODE)
        await self.send_command(mqtt_data=mqtt_data, http_params=http_params)

    async def set_bright(self, bright: int):
        if not self._bright_check(bright):
            logger.warning(f"Brightness {bright} out of range for device {self._id}")
            return
        mqtt_data, http_params = self._data_parser.format_command(config.ParamCode.BRIGHT, bright, config.HttpCode.BRIGHT)
        await self.send_command(mqtt_data, http_params)  

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
