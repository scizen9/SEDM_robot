import os
import sys
import logging
import json
from logging.handlers import TimedRotatingFileHandler
import time
from observatory.arclamps import controller as lamps
from observatory.stages import controller as stages
from observatory.telescope import tcs
import socket
import threading
import SEDM_robot_version as Version

with open(os.path.join(Version.CONFIG_DIR, 'logging.json')) as data_file:
    params = json.load(data_file)

logger = logging.getLogger("ocsLogger")
logger.setLevel(logging.DEBUG)
logging.Formatter.converter = time.gmtime
logHandler = TimedRotatingFileHandler(os.path.join(params['abspath'],
                                                   'ocs_server.log'),
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

logger.info("Starting Logger: Logger file is %s", 'ocs_server.log')


class ocsServer:
    def __init__(self, hostname, port):
        self.hostname = hostname
        self.port = port
        self.socket = ""
        self.stages = None
        self.lamp_controller = None
        self.lamps_dict = None
        self.tcs = None

    def handle(self, connection, address):
        if address:
            pass
        while True:
            start = time.time()
            response = None
            try:

                data = connection.recv(2048)

                data = data.decode("utf8")
                logger.info("Received: %s", data)

                if not data:
                    break
                try:
                    data = json.loads(data)
                except Exception as e:
                    logger.error("Load error", exc_info=True)
                    error_dict = json.dumps({"error": "error message %s"
                                                      % str(e)})
                    connection.sendall(error_dict)
                    break

                if 'command' in data:
                    cmd = data['command'].upper()
                    if 'parameters' in data:
                        parameters = data['parameters']
                    else:
                        parameters = {}
                        
                    if cmd == 'INITIALIZE_ALL':
                        if not self.lamp_controller:
                            logger.info("Initializing Arc Lamps")
                            self.lamp_controller = True
                            self.lamps_dict = lamps.connect_all()
                            
                        if not self.stages:
                            logger.info("Initializing Stages")
                            self.stages = stages.Stage()
                        
                        if not self.tcs:
                            logger.info("Initializing Telescope")
                        self.tcs = tcs.Telescope()
                        
                        response = {'elaptime': time.time() - start,
                                    'data': 'OCS initialized'}
                    elif cmd == 'INITIALIZE_LAMPS':
                        if not self.lamp_controller:
                            logger.info("Initializing Arc Lamps")
                            self.lamp_controller = lamps.Lamp()
                            self.lamps_dict = self.lamp_controller.connect_all()
                            response = {'elaptime': time.time() - start,
                                        'data': 'Arc lamps initialized'}
                    elif cmd == 'INITIALIZE_STAGES':
                        if not self.stages:
                            logger.info("Initializing Stages")
                            self.stages = stages.Stage()
                            self.stages.initialize(stage_id=1)
                            self.stages.initialize(stage_id=2)
                            response = {'elaptime': time.time() - start,
                                        'data': 'Stages initialized'}
                    elif cmd == 'INITIALIZE_TCS':
                        if not self.tcs:
                            logger.info("Initializing Telescope")
                            self.tcs = tcs.Telescope()
                            response = {'elaptime': time.time() - start,
                                        'data': 'Telescope initialized'}
                    elif cmd.upper() == "OBSSTATUS":
                        response = self.tcs.get_status()
                    elif cmd.upper() == "OBSWEATHER":
                        response = self.tcs.get_weather()
                    elif cmd.upper() == "OBSPOS":
                        response = self.tcs.get_pos()
                    elif cmd.upper() == "TELMOVE":
                        response = self.tcs.tel_move_sequence(**parameters)
                    elif cmd.upper() == "TELOFFSET":
                        response = self.tcs.offset(**parameters)
                    elif cmd.upper() == "TELGOFOC":
                        response = self.tcs.gofocus(**parameters)
                    elif cmd.upper() == "TELOFFSETFOC":
                        response = self.tcs.incfocus(**parameters)
                    elif cmd.upper() == "TELFAULTS":
                        response = self.tcs.get_faults()
                    elif cmd.upper() == "TELX":
                        response = self.tcs.x()
                    elif cmd.upper() == "TAKECONTROL":
                        response = self.tcs.takecontrol()
                    elif cmd.upper() == "TELHALON":
                        response = self.tcs.halogens_on()
                    elif cmd.upper() == "TELHALOFF":
                        response = self.tcs.halogens_off()
                    elif cmd.upper() == "TELSTOW":
                        response = self.tcs.stow(**parameters)
                    elif cmd.upper() == "DOME":
                        response = self.tcs.dome(**parameters)
                    elif cmd.upper() == "SETRATES":
                        response = self.tcs.irates(**parameters)
                    elif cmd.upper() == "ARCLAMPON":
                        response = self.lamps_dict[parameters['lamp']].on()
                    elif cmd.upper() == "ARCLAMPOFF":
                        response = self.lamps_dict[parameters['lamp']].off()
                    elif cmd.upper() == "ARCLAMPSTATUS":
                        response = self.lamps_dict[parameters['lamp']].status(
                            parameters['force_check'])
                    elif cmd.upper() == "STAGEMOVE":
                        response = self.stages.move_focus(**parameters)
                    elif cmd.upper() == "STAGEPOSITION":
                        response = self.stages.get_position(**parameters)
                    elif cmd.upper() == "STAGESTATE":
                        response = self.stages.get_state(**parameters)
                    elif cmd.upper() == "STAGEHOME":
                        response = self.stages.home(**parameters)
                    elif cmd.upper() == "PING":
                        response = {"elaptime": time.time()-start,
                                    "data": "Pong"}
                    else:
                        response = {"elaptime": time.time()-start,
                                    "error": "Command not found"}
                else:
                    response = {'elaptime': time.time()-start,
                                'error': "Command not found"}
                logger.info("Response: %s", response)
                jsonstr = json.dumps(response)
                connection.sendall(jsonstr.encode('utf-8'))
            except Exception as e:
                logger.error("Big error", exc_info=True)
                jsonstr = json.dumps({'elaptime': time.time()-start,
                                      'error': str(e)})
                connection.sendall(jsonstr.encode('utf-8'))
                pass

    def start(self):
        logger.debug("OCS server now listening for connections on port:%s"
                     % self.port)
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.socket.settimeout(None)
        self.socket.bind((self.hostname, self.port))
        self.socket.listen(5)

        try:
            while True:
                conn, address = self.socket.accept()
                logger.debug("Got connection from %s:%s" % (conn, address))
                new_thread = threading.Thread(target=self.handle,
                                              args=(conn, address))
                new_thread.start()
                logger.debug("Started process")
        except KeyboardInterrupt:
            logger.info("Exiting ocs_server")


if __name__ == "__main__":
    #
    server = ocsServer("localhost", 5003)
    logger.info("Starting ocsServer")
    server.start()
    logger.info("All done")
