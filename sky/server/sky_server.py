import os
import sys
import logging
import json
from logging.handlers import TimedRotatingFileHandler
import time
import socket
import threading
from sky.astrometry import solver
from sky.scheduler import dbscheduler
from sky.sextractor import run
from sky.guider import rcguider
from sky.growth import marshal
import SEDM_robot_version as Version

with open(os.path.join(Version.CONFIG_DIR, 'logging.json')) as data_file:
    params = json.load(data_file)

logger = logging.getLogger("skyLogger")
logger.setLevel(logging.DEBUG)
logging.Formatter.converter = time.gmtime
logHandler = TimedRotatingFileHandler(os.path.join(params['abspath'],
                                                   'sky_server.log'),
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

logger.info("Starting Logger: Logger file is %s", 'sky_server.log')


class SkyServer:
    def __init__(self, hostname, port, do_connect=True):
        self.hostname = hostname
        self.port = port
        self.socket = ""
        self.cam = None
        self.sex = run.sextractor()
        self.do_connect = do_connect
        self.scheduler = dbscheduler.Scheduler()
        self.growth = marshal.Interface()
        self.guider = rcguider.guide(do_connect=do_connect)

    def handle(self, connection, address):
        if address is not None:
            pass
        while True:
            response = {'test': 'test'}
            try:
                start = time.time()
                data = connection.recv(2048)

                data = data.decode("utf8")
                logger.info("Received: %s", data)

                if not data:
                    break
                try:

                    data = json.loads(data)
                except Exception as e:
                    logger.error("Load error", exc_info=True)
                    error_dict = json.dumps({'elaptime': time.time()-start,
                                             "error": "error message %s" %
                                                      str(e)})
                    connection.sendall(error_dict)
                    break

                if 'command' in data:
                    if data['command'].upper() == 'GETOFFSETS':
                        response = solver.calculate_offset(**data['parameters'])
                    elif data['command'].upper() == 'REINT':
                        self.sex = run.sextractor()
                        self.scheduler = dbscheduler.Scheduler()
                        self.growth = marshal.Interface()
                        self.guider = rcguider.guide(do_connect=self.do_connect)
                        response = {'elaptime': time.time()-start,
                                    'data': 'System reinitialized'}
                    elif data['command'].upper() == 'GETCALIBREQUESTID':
                        response = self.scheduler.get_calib_request_id(
                            **data['parameters'])
                    elif data['command'].upper() == 'ADDOBJECT':
                        response = self.scheduler.add_object(
                            **data['parameters'])
                    elif data['command'].upper() == 'GETMANUALREQUESTID':
                        response = self.scheduler.get_manual_request_id(
                            **data['parameters'])
                    elif data['command'].upper() == "GETSTANDARD":
                        response = self.scheduler.get_standard(
                            **data['parameters'])
                    elif data['command'].upper() == "GETFOCUSCOORDS":
                        response = self.scheduler.get_focus_coords(
                            **data['parameters'])
                    elif data['command'].upper() == "GETRCFOCUS":
                        response = self.sex.run_loop(**data['parameters'])
                    elif data['command'].upper() == 'STARTGUIDER':
                        _ = self.guider.start_guider(**data['parameters'])
                        response = {"elaptime": time.time()-start,
                                    "data": "guider started"}
                    elif data['command'].upper() == 'GETTARGET':
                        response = self.scheduler.get_next_observable_target(
                            **data['parameters'])
                    elif data['command'].upper() == 'PING':
                        response = {'elaptime': time.time()-start,
                                    'data': 'PONG'}
                    elif data['command'].upper() == "UPDATEGROWTH":
                        response = self.growth.update_growth_status(
                            **data['parameters'])
                    elif data['command'].upper() == "UPDATEREQUEST":
                        response = self.scheduler.update_request(
                            **data['parameters'])
                    elif data['command'].upper() == "GETGROWTHID":
                        response = self.growth.get_marshal_id_from_dbhost(
                            **data['parameters'])
                    elif data['command'].upper() == 'GETTWILIGHTEXPTIME':
                        response = self.scheduler.get_twilight_exptime(
                            **data['parameters'])

                else:
                    response = {'elaptime': time.time()-start,
                                'error': "Command not found"}
                jsonstr = json.dumps(response)
                connection.sendall(jsonstr.encode('utf-8'))
            except Exception as e:
                logger.warning(str(e))
                logger.error("Big error", exc_info=True)
                pass

    def start(self):
        logger.debug("Sky server now listening for connections on port:%s" %
                     self.port)
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
            logger.info("Exiting sky_server")


if __name__ == "__main__":
    #
    server = SkyServer("localhost", 5004, do_connect=False)
    logger.info("Starting SkyServer")
    server.start()
    logger.info("All done")
