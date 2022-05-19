import sys
import json
import os


SITE_ROOT = os.path.abspath(os.path.dirname(__file__)+'/..')

with open(os.path.join(SITE_ROOT, 'config', 'sedmpyconfig.json')) as data_file:
    params = json.load(data_file)

print(params['path'])
sys.path.append(params["path"])

from db.SedmDb import SedmDB

print(params)


def dbconnect():
    return SedmDB(dbname=params['dbname'], host=params['host'],
                  port=params['port'])
    # , supply_pass=True, passwd=params['password'])


if __name__ == "__main__":
    pass
