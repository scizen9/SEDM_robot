import os
import sys
import logging
import json
from logging.handlers import TimedRotatingFileHandler
import time
import socket
import threading
from cameras.andor import andor

SITE_ROOT = os.path.abspath(os.path.dirname(__file__)+'/../..')

with open(os.path.join(SITE_ROOT, 'config', 'logging.json')) as cfg_file:
    log_cfg = json.load(cfg_file)

with open(os.path.join(SITE_ROOT, 'config', 'cameras.json')) as cfg_file:
    cam_cfg = json.load(cfg_file)

logger = logging.getLogger("ifu_cameraLogger")
logger.setLevel(logging.DEBUG)
logging.Formatter.converter = time.gmtime
formatter = logging.Formatter("%(asctime)s--%(name)s--%(levelname)s--"
                              "%(module)s--%(funcName)s--%(message)s")
cam_log_dir = log_cfg['cam_abspath']
logHandler = TimedRotatingFileHandler(os.path.join(cam_log_dir,
                                                   'ifu_camera_server.log'),
                                      when='midnight', utc=True, interval=1,
                                      backupCount=360)
logHandler.setFormatter(formatter)
logHandler.setLevel(logging.DEBUG)
logger.addHandler(logHandler)
logger.info("Starting Logger: Logger file is %s", 'ifu_camera_server.log')

console_formatter = logging.Formatter("%(asctime)s: %(message)s")
consoleHandler = logging.StreamHandler(sys.stdout)
consoleHandler.setFormatter(console_formatter)
logger.addHandler(consoleHandler)

exp_start_file = os.path.join(cam_log_dir, "ifu_exposure_start.txt")


class CamServer:
    def __init__(self, hostname, port):
        self.hostname = hostname
        self.port = port
        self.socket = ""
        self.cam = None
        logger.info("Starting up cam server on %s port %s" % (self.hostname,
                                                              self.port))

    def handle(self, connection, address):

        if address is not None:
            pass

        while True:
            response = {'test': 'test'}
            try:
                start = time.time()
                data = connection.recv(2048)

                data = data.decode("utf8")
                logger.info("Received: %s", str(data))

                if not data:
                    break

                try:
                    data = json.loads(data)
                except Exception as e:
                    logger.error("Load error", exc_info=True)
                    error_dict = json.dumps(
                        {'elaptime': time.time()-start,
                         "error": "error message %s" % str(e)})
                    connection.sendall(error_dict)
                    break

                if 'command' in data:

                    if data['command'].upper() == 'INITIALIZE':
                        if not self.cam:
                            if self.port == cam_cfg['rc_port']:
                                cam_prefix = "rc"
                                send_to_remote = cam_cfg['rc_send_to_remote']
                                output_dir = cam_cfg['cam_image_dir']
                                set_temperature = cam_cfg['rc_set_temperature']
                                camera_handle = None
                                cam_ser_no = cam_cfg['rc_serial_number']
                                driver = cam_cfg['rc_driver']
                            else:
                                cam_prefix = "ifu"
                                send_to_remote = cam_cfg['ifu_send_to_remote']
                                output_dir = cam_cfg['cam_image_dir']
                                set_temperature = cam_cfg[
                                    'ifu_set_temperature']
                                camera_handle = cam_cfg['ifu_handle']
                                cam_ser_no = cam_cfg['ifu_serial_number']
                                driver = cam_cfg['ifu_driver']
                            if 'andor' in driver:
                                self.cam = andor.Controller(
                                    serial_number="", cam_prefix=cam_prefix,
                                    send_to_remote=send_to_remote,
                                    set_temperature=set_temperature,
                                    camera_handle=camera_handle,
                                    output_dir=output_dir,)

                                ret = self.cam.initialize()
                                # And now we check the correct serial number
                                logger.info("Do these match? %s %s" %
                                            (str(self.cam.serialNumber),
                                             str(cam_ser_no)))
                                if ret:
                                    response = {'elaptime': time.time()-start,
                                                'data': "Camera started"}
                                else:
                                    response = {'elaptime': time.time()-start,
                                                'error': self.cam.lastError}
                            else:
                                response = {'elaptime': time.time()-start,
                                            'error':
                                                "can only use andor driver!"}
                        else:
                            print(self.cam)
                            print(type(self.cam))
                            response = {'elaptime': time.time()-start,
                                        'data': "Camera already initialized"}

                    elif data['command'].upper() == 'TAKE_IMAGE':
                        with open(exp_start_file, 'w') as file:
                            file.write(time.strftime('%Y-%m-%d %H:%M:%S.%d',
                                                     time.gmtime()))
                        response = self.cam.take_image(**data['parameters'])
                        # if we run into a problem, we want to reconnect
                        if 'error' in response:
                            self.cam = None
                        logger.info(str(response))
                    elif data['command'].upper() == 'LOGROLLOVER':
                        logger.removeHandler(logHandler)
                        logHandler.doRollover()
                        logger.addHandler(logHandler)
                        logger.info("New IFU log")
                        logger.info("Log rollover")
                    elif data['command'].upper() == 'STATUS':
                        response = self.cam.get_status()
                    elif data['command'].upper() == 'GETTEMPSTATUS':
                        response = self.cam.get_temp_status()
                        logger.info(str(response))
                    elif data['command'].upper() == 'ACQSTATUS':
                        response = self.cam.get_acq_status()
                        logger.info(str(response))
                    elif data['command'].upper() == 'PING':
                        response = {'data': 'PONG'}
                    elif data['command'].upper() == 'GETPRESSURE':
                        last_line = open("C:/Users/SEDM-User/Desktop/"
                                         "SEDMv3Robot-master/utilities/"
                                         "chiller.txt").readlines()[-1]
                        response = {'elaptime': time.time() - start,
                                    'data': str(last_line)}
                    elif data['command'].upper() == "LASTERROR":
                        response = self.cam.lastError
                        logger.info(str(response))
                    elif data['command'].upper() == "LASTEXPOSED":
                        obs_time = open(exp_start_file).readlines()[0]
                        response = {'elaptime': time.time()-start,
                                    'data': str(obs_time)}
                    elif data['command'].upper() == "PREFIX":
                        response = {'elaptime': time.time()-start,
                                    'data': self.cam.camPrefix}
                    elif data['command'].upper() == "REINIT":
                        response = self.cam.opt.disconnect()
                        logger.info(str(response))
                    elif data['command'].upper() == "SHUTDOWN":
                        # _ = self.cam.opt.disconnect()
                        # _ = self.cam.opt.unloadLibrary()
                        _ = self.cam.opt.ShutDown()
                        self.cam = None
                        response = {'elaptime': time.time()-start,
                                    'data': "Camera shutdown"}
                        logger.info(str(response))
                else:
                    response = {'elaptime': time.time()-start,
                                'error': "Command not found"}

                jsonstr = json.dumps(response)
                connection.sendall(jsonstr.encode('utf-8'))
            except Exception as e:
                logger.error("Camera error: %s" % str(time.gmtime()))
                logger.error(str(e))
                logger.error("Big error", exc_info=True)
                time.sleep(60)

    def execute_warmup(self):
        """Execute a warmup sequence"""
        # set cooling mode to warmup
        self.cam.opt.SetCoolerMode(0)   # warm up to ambient
        self.cam.opt.CoolerOFF()        # turn off cooler
        logger.info("Executing warmup sequence, waiting for -20 C or above")

        try:
            while True:
                ret = self.cam.get_temp_status()
                logger.info(str(ret))
                if 'camtemp' in ret:
                    if ret['camtemp'] > -20.:
                        break
                time.sleep(5)
        except KeyboardInterrupt:
            pass
        else:
            logger.info("Warmup sequence complete.")

    def start(self):
        logger.debug("IFU server now listening for connections on %s port:%s",
                     self.hostname, self.port)
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM, 0)
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
            q = input("Warm up before shutdown "
                      "(only for long-term shutdown)? (N/y): ")
            if 'Y' in q.upper():
                self.execute_warmup()
                logger.info("Executing camera shutdown")
                self.cam.opt.ShutDown()
            else:
                logger.info("Executing camera shutdown")
                self.cam.opt.ShutDown()


if __name__ == "__main__":
    server = CamServer(cam_cfg['ifu_ip'], cam_cfg['ifu_port'])
    #
    logger.info("Starting IFU Server")
    server.start()
    logger.info("All done")
