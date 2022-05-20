import os
import sys
import logging
import json
from logging.handlers import TimedRotatingFileHandler
import time
import socket
import threading
from cameras.pixis import pixis

SITE_ROOT = os.path.abspath(os.path.dirname(__file__)+'/../..')

with open(os.path.join(SITE_ROOT, 'config', 'logging.json')) as cfg_file:
    log_cfg = json.load(cfg_file)

with open(os.path.join(SITE_ROOT, 'config', 'sedm.json')) as cfg_file:
    sedm_cfg = json.load(cfg_file)

logger = logging.getLogger("rc_cameraLogger")
logger.setLevel(logging.DEBUG)
logging.Formatter.converter = time.gmtime
formatter = logging.Formatter("%(asctime)s--%(name)s--%(levelname)s--"
                              "%(module)s--%(funcName)s--%(message)s")
cam_log_dir = log_cfg['cam_abspath']
logHandler = TimedRotatingFileHandler(os.path.join(cam_log_dir,
                                                   'rc_camera_server.log'),
                                      when='midnight', utc=True, interval=1,
                                      backupCount=360)
logHandler.setFormatter(formatter)
logHandler.setLevel(logging.DEBUG)
logger.addHandler(logHandler)
logger.info("Starting Logger: Logger file is %s", 'rc_camera_controller.log')

exp_start_file = os.path.join(cam_log_dir, "rc_exposure_start.txt")


class CamServer:
    def __init__(self, hostname, port):
        self.hostname = hostname
        self.port = port
        self.socket = ""
        self.cam = None
        print("Starting up cam server on %s port %s" % (self.hostname,
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
                logger.info("Received: %s", data)
                print(data)

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
                            if self.port == sedm_cfg['rc_port']:
                                cam_prefix = "rc"
                                send_to_remote = sedm_cfg['rc_send_to_remote']
                                output_dir = sedm_cfg['cam_image_dir']
                                set_temperature = sedm_cfg['rc_set_temperature']
                                cam_ser_no = sedm_cfg['rc_serial_number']
                            else:
                                cam_prefix = "ifu"
                                send_to_remote = sedm_cfg['ifu_send_to_remote']
                                output_dir = sedm_cfg['cam_image_dir']
                                set_temperature = sedm_cfg[
                                    'ifu_set_temperature']
                                cam_ser_no = sedm_cfg['ifu_serial_number']
                            # Trick pixis code into connecting to first camera
                            # in list by sending an empty serial number.
                            # This is why we start rc_cam_server first!
                            self.cam = pixis.Controller(
                                serial_number="",
                                cam_prefix=cam_prefix,
                                send_to_remote=send_to_remote,
                                set_temperature=set_temperature,
                                output_dir=output_dir)

                            # Initialize the camera
                            ret = self.cam.initialize()
                            # And now we can set the correct serial number
                            self.cam.serialNumber = cam_ser_no
                            if ret:
                                response = {'elaptime': time.time()-start,
                                            'data': "Camera started"}
                            else:
                                response = {'elaptime': time.time()-start,
                                            'error': self.cam.lastError}
                        else:
                            print(self.cam)
                            print(type(self.cam))
                            response = {'elaptime': time.time()-start,
                                        'data': "Camera already intiailzed"}

                    elif data['command'].upper() == 'TAKE_IMAGE':
                        with open(exp_start_file, 'w') as file:
                            file.write(time.strftime('%Y-%m-%d %H:%M:%S.%d',
                                                     time.gmtime()))
                        response = self.cam.take_image(**data['parameters'])
                        print(response)
                    elif data['command'].upper() == 'LOGROLLOVER':
                        logger.removeHandler(logHandler)
                        logHandler.doRollover()
                        logger.addHandler(logHandler)
                        logger.info("New RC log")
                        print("Log rollover")
                    elif data['command'].upper() == 'STATUS':
                        response = self.cam.get_status()
                    elif data['command'].upper() == 'GETTEMPSTATUS':
                        response = self.cam.get_temp_status()
                        print(response)
                    elif data['command'].upper() == 'PING':
                        response = {'data': 'PONG'}
                    elif data['command'].upper() == "LASTERROR":
                        response = self.cam.lastError
                        print(response)
                    elif data['command'].upper() == "LASTEXPOSED":
                        obs_time = open(exp_start_file).readlines()[0]
                        response = {'elaptime': time.time()-start,
                                    'data': str(obs_time)}
                    elif data['command'].upper() == "PREFIX":
                        response = {'elaptime': time.time()-start,
                                    'data': self.cam.camPrefix}
                    elif data['command'].upper() == "REINIT":
                        response = self.cam.opt.disconnect()
                        print(response)
                    elif data['command'].upper() == "SHUTDOWN":
                        _ = self.cam.opt.disconnect()
                        # ret = self.cam.opt.unloadLibrary()
                        self.cam = None
                        response = {'elaptime': time.time()-start,
                                    'data': "Camera shutdown"}
                        print(response)
                else:
                    response = {'elaptime': time.time()-start,
                                'error': "Command not found"}
                jsonstr = json.dumps(response)
                connection.sendall(jsonstr.encode('utf-8'))
            except Exception as e:
                print("Camera error:", time.gmtime())
                print(str(e))
                logger.error("Big error", exc_info=True)
                time.sleep(60)

    def start(self):
        logger.debug("RC server now listening for connections on port:%s" %
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
            sys.exit("Exiting")


if __name__ == "__main__":
    # server = CamServer("10.200.155.4", 5002)
    rc_ip = sedm_cfg['rc_ip']
    rc_port = sedm_cfg['rc_port']
    server = CamServer(rc_ip, rc_port)
    # try:
    logger.info("Starting RC Server")
    # server.cam = pixis.Controller(serial_number="", cam_prefix="rc",
    #                               send_to_remote=True, output_dir="C:/images")
    # server.cam.initialize()
    # server.cam.serialNumber = "04001312"
    server.start()
    # except Exception as e:
    #    print(str(e))
    #    logging.exception("Unexpected exception %s", str(e))
    # finally:
    #    logging.info("Shutting down IFU server")
    logger.info("All done")
