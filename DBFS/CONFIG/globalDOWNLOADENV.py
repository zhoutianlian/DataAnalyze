from CONFIG.globalDOWNLOADPATH import PATH_CONFIG


class global_path:
    path = 'product'


# 对于每个全局变量，都需要定义get_value和set_value接口
def set_path(name):
    if name == 'product':
        global_path.path = PATH_CONFIG.PRODUCT
    elif name == 'test':
        global_path.path = PATH_CONFIG.TEST
    elif name == 'develop':
        global_path.path = PATH_CONFIG.DEVELOP


def get_path():
    return global_path.path
