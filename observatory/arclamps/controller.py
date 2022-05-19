import logging
from logging.handlers import TimedRotatingFileHandler
import json
import os
import time
import socket


SITE_ROOT = os.path.abspath(os.path.dirname(__file__)+'/../..')

with open(os.path.join(SITE_ROOT, 'config', 'logging.json')) as data_file:
    params = json.load(data_file)

logger = logging.getLogger("lampControllerLogger")
logger.setLevel(logging.DEBUG)
logging.Formatter.converter = time.gmtime
formatter = logging.Formatter("%(asctime)s--%(name)s--%(levelname)s--"
                              "%(module)s--%(funcName)s--%(message)s")

logHandler = TimedRotatingFileHandler(os.path.join(params['abspath'],
                                                   'lamp_controller.log'),
                                      when='midnight', utc=True, interval=1,
                                      backupCount=360)
logHandler.setFormatter(formatter)
logHandler.setLevel(logging.DEBUG)
logger.addHandler(logHandler)
logger.info("Starting Logger: Logger file is %s", 'lamp_controller.log')


# put this in Lamp class?
def connect_all():
    arc_lamps = ['hg', 'cd', 'xe']
    lamp_dict = {}
    for lmp in arc_lamps:
        lamp_dict[lmp] = Lamp(lmp)

    return lamp_dict


class Lamp:
    """Class script to handle functions of the arc lamps"""

    def __init__(self, lamp="", simulated=False):
        logger.info("Setting up the %s lamp", lamp)
        self.internal_lamps = ['hg', 'cd']
        self.external_lamps = ['xe']
        self.simulated = simulated
        print(SITE_ROOT, lamp)
        with open(os.path.join(SITE_ROOT, 'config', 'lamps.json')) as cfile:
            self.lamp_config = json.load(cfile)

        self.name = lamp

        self.host = self.lamp_config[lamp.lower()]['ip']
        self.port = int(self.lamp_config[lamp.lower()]['port'])
        self.wait = int(self.lamp_config[lamp.lower()]['wait'])
        self.plug = int(self.lamp_config[lamp.lower()]['outlet'])
        self.socket = None
        self.state = "UNKNOWN"

    def send_cmd(self, cmd=""):
        """
        Send a list of socket commands to the lamp controllers. Since we
        only connect twice per night at most we don't leave the socket
        connection open

        :param cmd: list of commands to run on the controller
                    pset #(plug number) #(state 0 off, 1 on)
        :return: Bool, output string
        """
        start = time.time()

        try:
            self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.socket.connect((self.host, self.port))
            logger.info("Sending command: %(cmd)s", {'cmd': cmd})

            cmd = cmd.encode('utf-8')
            self.socket.send(b"%s\r" % cmd)
            logger.info("Command set")
            time.sleep(.1)
            self.socket.send(b"logout\r")
            data = self.socket.recv(2048)
            logger.info("Recieved: %s", data)
            self.socket.close()
            logger.info("Socket closed")
            return {'elaptime': time.time()-start,
                    'data': data}
        except Exception as e:
            logger.error("Error sending command", exc_info=True)
            return {'elaptime': time.time() - start,
                    'error': str(e)}

    def on(self):
        self.state = "ON"
        logger.info("Turning on %s lamp at %s port %s plug %s" % (self.name,
                                                                  self.host,
                                                                  self.port,
                                                                  self.plug))
        return self.send_cmd("pset %s 1" % self.plug)

    def off(self):
        self.state = "OFF"
        logger.info("Turning off %s lamp at %s port %s plug %s" % (self.name,
                                                                   self.host,
                                                                   self.port,
                                                                   self.plug))
        return self.send_cmd("pset %s 0" % self.plug)

    def status(self, force_check=True):
        """

        :return:
        """
        start = time.time()

        if not force_check:
            return {'elaptime': time.time(), 'data': self.state}

        ret = self.send_cmd("pshow")
        if 'data' in ret:
            if isinstance(ret['data'], bytes):
                try:
                    data = ret['data'].decode('utf-8', errors="replace")
                except Exception as e:
                    logger.error("Error decoding return", exc_info=True)
                    return {'elaptime': time.time() - start, 'data': 'UNKNOWN'}
            else:
                data = str(ret['data'])
            print(data, "Lamp data")
            print(self.plug, "plig")
            if self.name.lower() == 'xe':
                search = "outlet%s" % self.plug
            else:
                search = "%s lamp" % self.name
            if search in data.lower():
                try:
                    splitstr = data.lower().split(
                        '%s |' % search)[-1].split('|')[0].rstrip().lstrip()
                    return {'elaptime': time.time()-start, 'data': splitstr}
                except Exception as e:
                    logger.error("Error parsing output", exc_info=True)
                    return {'elaptime': time.time()-start, 'data': 'UNKNOWN'}
            else:
                logger.error("Plug(%s) not detected in output", self.plug)
                return {'elaptime': time.time() - start, 'data': 'UNKNOWN'}
        else:
            return {'elaptime': time.time()-start, 'data': 'UNKNOWN'}


if __name__ == '__main__':
    # x = Lamp(lamp='cd')
    lamps = connect_all()
    print(lamps)
    xe = lamps['xe']
    hg = lamps['hg']
    cd = lamps['cd']
    print(hg.status())
    # print(hg.on())
    print(xe.status())
    # print(xe.off())
    print(cd.status())
