import datetime
import os

BASE_DIR = os.path.abspath(os.path.dirname(os.path.dirname(__file__)))


def logger(errno):
    path = os.path.join(BASE_DIR, "setting/log.txt")
    with open(path, "a") as f:
        f.write(str(datetime.datetime.now()) + str(errno) + "\n")
