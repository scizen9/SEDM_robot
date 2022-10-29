import os
import sys
import socket
import json
import logging
from logging.handlers import TimedRotatingFileHandler
import time
import SEDM_robot_version as Version

with open(os.path.join(Version.CONFIG_DIR, 'logging.json')) as data_file:
    params = json.load(data_file)

logger = logging.getLogger("Winter Logger")
logger.setLevel(logging.DEBUG)
logging.Formatter.converter = time.gmtime
logHandler = TimedRotatingFileHandler(os.path.join(params['abspath'],
                                                   'winter.log'),
                                      when='midnight', utc=True, interval=1,
                                      backupCount=360)

formatter = logging.Formatter("%(asctime)s--%(levelname)s--%(module)s--"
                              "%(funcName)s--%(message)s")
logHandler.setFormatter(formatter)
logHandler.setLevel(logging.DEBUG)
logger.addHandler(logHandler)

console_formatter = logging.Formatter("%(asctime)s--%(message)s")
consoleHandler = logging.StreamHandler(sys.stdout)
consoleHandler.setFormatter(console_formatter)
logger.addHandler(consoleHandler)

logger.info("Starting Logger: Logger file is %s", 'winter.log')


class Winter:
    """Top level class to handle all the WINTER commands and to make sure they
    are properly formatted.  Commands return True and time to complete command
    if successful.  Otherwise False and an error message when a command fails
    """

    def __init__(self, simulated=False, wntaddress=None):

        self.simulated = simulated
        self.dome_states = ['OPEN', 'CLOSE']
        self.delimiter = "="
        self.weather = {}
        self.status = {}
        self.socket = None
        self.error_str = None
        self.error_tracker = 0
        with open(os.path.join(Version.CONFIG_DIR, 'winter.json')) as cfile:
            self.wnt_config = json.load(cfile)

        if not wntaddress:
            self.address = (self.wnt_config['winter_address'],
                            self.wnt_config['winter_port'])
        else:
            self.address = wntaddress

    def __connect(self):
        logger.info("Connecting to address:%s", self.address)
        try:
            self.socket = socket.socket()
            self.socket.connect(self.address)
        except Exception as e:
            logger.error("Error connecting to WINTER:%s", str(e),
                         exc_info=True)
            self.socket = None
            pass
        return self.socket
    
    # CONTROL COMMANDS:
    def send_command(self, cmd=""):
        """
        Send one of the WINTER commands to the server.
        :param cmd: Predefined "fast" or "slow" commands
        :return: Bool,time to complete command in seconds
        """
        # Start timer
        start = time.time()

        info = False
        # Check if the socket is open
        if not self.socket:
            logger.info("Socket not connected")
            self.socket = self.__connect()
            if not self.socket:
                return {"elaptime": time.time()-start,
                        "error": "Error connecting to the WINTER adderess"}

        # Make sure all commands are upper case
        # cmd = cmd.upper()

        # 1.It is a fast command
        self.socket.settimeout(60)
        logger.info("Sending fast command with 60s timeout")

        # 3. At this point we have the full command for the WINTER interface
        try:
            logger.info("Sending:%s", cmd)
            self.socket.send(b"%s \n" % cmd.encode('utf-8'))
        except Exception as e:
            logger.error("Error sending command: %s", str(e), exc_info=True)
            return {"elaptime": time.time() - start,
                    "error": "Error commamd:%s failed" % cmd}

        # 4. Get the return response
        try:
            # Slight delay added for the info command to print out
            time.sleep(.05)

            ret = self.socket.recv(2048)

            # Return the int code
            if ret:
                ret = ret.decode('utf-8')
            else:
                # Try one more time to get a return
                ret = self.socket.recv(2048)

            # If we still don't have a return then something has gone wrong.
            if not ret:
                logger.error("No response given back from the WINTER interface")
                return {"elaptime": time.time() - start,
                        "error": "No response from WINTER"}

            # Return the info product or return code
            # logger.info("Received: %s", ret)
            ret = ret.rstrip('\0').rstrip('\n')

            if isinstance(ret, str):
                ret = ret.replace('ON', '"ON"')
                ret = ret.replace('OFF', '"OFF"')
                return {"elaptime": time.time() - start,
                        "data": ret}
            else:
                logger.warning("bad return, command collision")
                return {"elaptime": time.time() - start,
                        "error": "bad return, command collision"}
        except Exception as e:
            logger.error("Unkown error", exc_info=True)
            return {"elaptime": time.time() - start, "error": str(e)}

    def get_weather(self):
        """
        Get the weather output and convert it to a dictionary

        :return: bool, status message
        """
        start = time.time()
        ret = self.send_command("status?")
        if "data" in ret:
            wthr_dict = json.loads(ret['data'])
            if wthr_dict:
                self.weather = json.loads(ret['data'])
            else:
                return {"elaptime": time.time() - start,
                        "error": "bad ?WEATHER return"}
        else:
            return ret

        return {"elaptime": time.time()-start,
                "data": self.weather}

    def get_status(self, redo=True):
        """
        Get the status output and convert it to a dictionary

        :return: bool, status message
        """
        if redo:
            pass
        start = time.time()
        ret = self.send_command("status?")
        if "data" in ret:
            stat_dict = json.loads(ret['data'])
            if stat_dict:
                self.status = json.loads(ret['data'])
            else:
                return {"elaptime": time.time() - start,
                        "error": "bad ?STATUS return"}
        else:
            return ret

        return {"elaptime": time.time() - start,
                "data": self.status}


if __name__ == "__main__":
    x = Winter()
    print(x.get_status())
