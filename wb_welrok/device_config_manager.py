import json
import jsonschema
import logging
from typing import List

from wb_welrok.schemas import DeviceConfig
from wb_welrok.config import DEFAULT_BROKER_URL

logger = logging.getLogger(__name__)

class ConfigManager:

    def __init__(self, config_path: str, schema_path: str):
        self.config_path = config_path
        self.schema_path = schema_path
        self.devices: List[DeviceConfig] = []
        self.mqtt_server_uri: str = DEFAULT_BROKER_URL
        self.debug: bool = False

    def load_and_validate(self):
        try:
            with open(self.config_path, "r", encoding="utf-8") as f_config, \
                open(self.schema_path, "r", encoding="utf-8") as f_schema:
                config_data = json.load(f_config)
                schema = json.load(f_schema)
                jsonschema.validate(config_data, schema)

                id_list = [device["device_id"] for device in config_data.get("devices", [])]
                if len(id_list) != len(set(id_list)):
                    raise ValueError("Device ID must be unique")

                self.devices = [DeviceConfig(**d) for d in config_data.get("devices", [])]
                self.mqtt_server_uri = config_data.get("mqtt_server_uri", self.mqtt_server_uri)
                self.debug = config_data.get("debug", False)
            return self
        except (jsonschema.ValidationError, ValueError, FileNotFoundError) as e:
            logger.error(f"{'*'*200}Config validation failed: {e}")
            return None

    def __repr__(self):
        return (f"<ConfigManager devices={len(self.devices)} "
                f"mqtt_server_uri={self.mqtt_server_uri} debug={self.debug}>")  
