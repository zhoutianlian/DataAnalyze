import pymysql
import CONFIG.globalENV as gl
from log.log import logger


def connect_mysql_valuation():
    db_config = {
        'host': gl.get_name(),
        'port': 3306,
        'user': gl.get_user(),
        'password': gl.get_password(),
        'db':'valuation',
        'charset':'utf8mb4'
    }
    cms = None
    try:
        cms = pymysql.connect(**db_config)
    except Exception as e:
        logger(e)
    return cms


def connect_mysql_fuyoubank_security():
    db_config = {
        'host': gl.get_name(),
        'port': 3306,
        'user': gl.get_user(),
        'password': gl.get_password(),
        'db':'fuyoubank_security',
        'charset':'utf8mb4'
    }
    cms = None
    try:
        cms = pymysql.connect(**db_config)
    except Exception as e:
        logger(e)
    return cms

def connect_mysql_rdt_security():
    db_config = {
        'host': gl.get_name(),
        'port': 3306,
        'user': gl.get_user(),
        'password': gl.get_password(),
        'db': 'rdt_security',
        'charset': 'utf8mb4'
    }
    cms = None
    try:
        cms = pymysql.connect(**db_config)
    except Exception as e:
        logger(e)
    return cms


def connect_mysql_Require_Rate_of_Return():
    db_config = {
        'host': gl.get_name(),
        'port': 3306,
        'user': gl.get_user(),
        'password': gl.get_password(),
        'db': 'Require_Rate_of_Return',
        'charset': 'utf8mb4'
    }
    try:
        cms = pymysql.connect(**db_config)
    except Exception as e:
        logger(e)
    return cms

def connect_mysql_MM_Model():
    db_config = {
        'host': gl.get_name(),
        'port': 3306,
        'user': gl.get_user(),
        'password': gl.get_password(),
        'db': 'MM_Model',
        'charset': 'utf8mb4'
    }
    try:
        cms = pymysql.connect(**db_config)
    except Exception as e:
        logger(e)
    return cms


def connect_mysql_RIM_Indus():
    db_config = {
        'host': gl.get_name(),
        'port': 3306,
        'user': gl.get_user(),
        'password': gl.get_password(),
        'db': 'RIM_Indus',
        'charset': 'utf8mb4'
    }
    try:
        cms = pymysql.connect(**db_config)
    except Exception as e:
        logger(e)
    return cms

def connect_mysql_Stock3_valuation_result():
    db_config = {
        'host': gl.get_name(),
        'port': 3306,
        'user': gl.get_user(),
        'password': gl.get_password(),
        'db': 'Stock3_valuation_result',
        'charset': 'utf8mb4'
    }
    try:
        cms = pymysql.connect(**db_config)
    except Exception as e:
        logger(e)
    return cms

def connect_mysql_valuation_samuelson():
    db_config = {
        'host': gl.get_name(),
        'port': 3306,
        'user': gl.get_user(),
        'password': gl.get_password(),
        'db': 'valuation_samuelson',
        'charset': 'utf8mb4'
    }
    try:
        cms = pymysql.connect(**db_config)
    except Exception as e:
        logger(e)
    return cms


def connect_mysql_Next_Ten_Growth():
    db_config = {
        'host': gl.get_name(),
        'port': 3306,
        'user': gl.get_user(),
        'password': gl.get_password(),
        'db': 'Next_Ten_Growth',
        'charset': 'utf8mb4'
    }
    try:
        cms = pymysql.connect(**db_config)
    except Exception as e:
        logger(e)
    return cms


def connect_mysql_from_mongodb():
    db_config = {
        'host': gl.get_name(),
        'port': 3306,
        'user': gl.get_user(),
        'password': gl.get_password(),
        'db': 'from_mongodb',
        'charset': 'utf8mb4'
    }
    cms = None
    try:
        cms = pymysql.connect(**db_config)
    except Exception as e:
        logger(e)
    return cms


def connect_mysql_rdt_fintech():
    db_config = {
        'host': gl.get_name(),
        'port': 3306,
        'user': gl.get_user(),
        'password': gl.get_password(),
        'db': 'rdt_fintech',
        'charset': 'utf8mb4'
    }
    cms = None
    try:
        cms = pymysql.connect(**db_config)
    except Exception as e:
        logger(e)
    return cms

