LOG_PATH = "/var/log/catarc/"
LOG_FILE = "app.log"
LOG_LEVEL = 'INFO'
FILE_PATH = "/usr/local/catarc/server/data/"

POSTGRESQL = {
    "user": "postgres",
    "password": "111111",
    "host": "localhost",
    "port": 5432,
    "database": "postgres"
}

class SampleCategory(object):
    CELL = "单体蓄电池"
    MOD = "蓄电池模块"
    PACK = "蓄电池系统"

ITEM_LIST = {
    "GBT 31484-2015": {
        "单体蓄电池": {
            "室温放电容量": "CellRtempDchgCapacity",
            "标准循环寿命": "CellStandardCycleLife"
        }
    },
    "GBT 31486-2015": {
        "单体蓄电池": {
            "尺寸质量": "DefaultItem",
            "外观极性": "DefaultItem",
            "室温放电容量": "CellRtempDchgCapacity"
        },
        "蓄电池模块": {
            "尺寸质量": "DefaultItem",
            "外观极性": "DefaultItem",
            "耐振动": "DefaultItem",
            "比功率": "ModSpecificPower",
            "储存": "ModReserve",
            "室温放电容量": "ModRtempDchgCapacity",
            "低温放电容量": "ModLtempDchgCapacity",
            "高温放电容量": "ModHtempDchgCapacity",
            "室温荷电保持": "ModRtempChargeMaintain",
            "高温荷电保持": "ModHtempChargeMaintain",
            "室温倍率放电容量": "ModRtempRateDchgCapacity",
            "室温倍率充电性能": "ModRtempRateChgPerformance"
        }
    },
    "GB 38031-2020": {
        "单体蓄电池": {
            "外部短路": "DefaultItem",
            "加热": "DefaultItem",
            "过放电": "DefaultItem",
            "过充电": "DefaultItem",
            "温度循环": "DefaultItem",
            "挤压": "DefaultItem",
            "室温放电容量": "CellRtempDchgCapacity"
        },
        "蓄电池系统": {
            "盐雾": "DefaultItem",
            "温度冲击": "DefaultItem",
            "外部火烧": "DefaultItem",
            "外部短路保护": "DefaultItem",
            "湿热循环": "DefaultItem",
            "浸水": "DefaultItem",
            "振动": "DefaultItem",
            "模拟碰撞": "DefaultItem",
            "挤压": "DefaultItem",
            "机械冲击": "DefaultItem",
            "过温保护": "DefaultItem",
            "过流保护": "DefaultItem",
            "过放电保护": "DefaultItem",
            "过充电保护": "DefaultItem",
            "高海拔": "DefaultItem",
            "室温放电容量": "PackRtempDchgCapacity"
        }
    },
    "中机函[2017]2号": {
        "蓄电池系统": {
            "能量密度": "PackEnergyDensity"
        }
    }
}

class DatabaseTable(object):
    TASK = "public.task_info"
    PRODUCT = "public.product_info"
    STAT = "public.stat_info"
    USER = "public.user_info"

class TestDataError(Exception):
    '''TestDataError'''

    def __init__(self, output):
        self.output = output