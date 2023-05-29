from enum import Enum


class Network(Enum):
    MVV = "mvv"
    SWM = "swm"
    DDB = "ddb"
    RBO = "rbo"
    RVO = "rvo"
    INV = "inv"
    SNP = "snp"

class Product(Enum):
    SBahn = "SBAHN"
    UBahn = "UBAHN"
    RegionalBus = "REGIONAL_BUS"
    RufTaxi = "RUFTAXI"
    Bus = "BUS"
    Tram = "TRAM"

