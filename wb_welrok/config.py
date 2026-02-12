from typing import Callable
from enum import Enum


CONFIG_FILEPATH = "/etc/wb-welrok.conf"
SCHEMA_FILEPATH = "/usr/share/wb-mqtt-confed/schemas/wb-mqtt-welrok.schema.json"
DEFAULT_BROKER_UNIX = "unix:///var/run/mosquitto/mosquitto.sock"
DEFAULT_BROKER_URL = "tcp://127.0.0.1:1883"

data_topics = ["floorTemp", "airTemp", "protTemp", "setTemp", "powerOff", "load"]
settings_topics = ["setTemp", "bright", "powerOff", "mode"]

TEMP_CODES = {0: "Overheat temperature", 1: "Floor temperature", 2: "Air temperature", 7: "MK temperature"}

TOPIC_NAMES_TRANSLATE = {
    "Overheat temperature": "Внутренняя температура устройства",
    "Floor temperature": "Температура пола",
    "Air temperature": "Температура воздуха",
    "MK temperature": "Температура процессора",
}

INNER_TOPICS = {
    "protTemp": "Overheat temperature",
    "floorTemp": "Floor temperature",
    "airTemp": "Air temperature",
    "setTemp": "Set temperature",
    "powerOff": "Power",
    "load": "Load",
}

CMD_CODES = {"params": 1, "telemetry": 4}

PARAMS_CODES = {125: "powerOff", 23: "bright", 31: "setTemp", 2: "mode"}
MODE_CODES = {
    0: "Auto",
    1: "Manual",
}

def set_temp_param_parse(parameter):
    if int(parameter[1]) == 3:
        return round(float(parameter[2]) / 10, 2)
    return round(float(parameter[2]), 2)

PARAMS_CHOISE: dict[str, Callable] = {
    "powerOff": lambda x: "1" if x == "0" else "0",
    "bright": lambda x: round(float(x), 2),
    "setTemp": set_temp_param_parse,
    "mode": lambda x: MODE_CODES[int(x)],
    "load": lambda x: "Выключено" if x == "0" else "Включено",
}

MODE_NAMES_TRANSLATE = {"Auto": "По расписанию", "Manual": "Ручной"}

MODE_CODES_REVERSE = {
    "Auto": 0,
    "Manual": 1,
}

class ParamCode(Enum):
    POWER = 125
    TEMP = 31
    MODE = 2
    BRIGHT = 23

class HttpCode(Enum):
    POWER = 7
    TEMP = 3
    MODE = 2
    BRIGHT = 2

class ModeCode(Enum):
    MANUAL = MODE_CODES_REVERSE["Manual"]

CONTROLS_CONFIG = {
    "Power": {
        "meta": {
            "title": "Включение / выключение",
            "title_en": "Power",
            "control_type": "switch",
            "order": 1,
            "read_only": False,
        },
    },
    "Bright": {
        "meta": {
            "title": "Яркость дисплея",
            "title_en": "Display Brightness",
            "units": "%",
            "control_type": "range",
            "order": 2,
            "read_only": False,
            "max_value": 100,
        },
    },
    "Set temperature": {
        "meta": {
            "title": "Установка",
            "title_en": "Set floor temperature",
            "units": "deg C",
            "control_type": "range",
            "order": 3,
            "read_only": False,
            "min_value": 5,
            "max_value": 45,
        },
    },
    "Modes": {
        "meta_template": {
            "control_type": "pushbutton",
            "read_only": False,
        },
        "order_start": 4,
    },
    "Readonly": {
        "Load": {
            "meta": {
                "title": "Нагрузка",
                "title_en": "Load",
                "control_type": "text",
                "order": None,
                "read_only": True,
            },
        },
        "Current mode": {
            "meta": {
                "title": "Текущий режим работы",
                "title_en": "Current mode",
                "control_type": "text",
                "order": None,
                "read_only": True,
            },
        },
        "Temps": {
            "meta_template": {
                "control_type": "text",
                "read_only": True,
            },
            "order_start": None,
        },
    },
}
