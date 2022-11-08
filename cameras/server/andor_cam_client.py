import socket
import os
import time
import json

SITE_ROOT = os.path.abspath(os.path.dirname(__file__)+'/../..')

with open(os.path.join(SITE_ROOT, 'config', 'cameras.json')) as cfg_file:
    cam_cfg = json.load(cfg_file)


class Camera:

    def __init__(self, address=cam_cfg['ifu_ip'], port=cam_cfg['ifu_port']):
        """

        :param address:
        :param port:
        """

        self.address = address
        self.port = port
        print(self.address, self.port)
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # print(self.socket)
        self.socket.connect((self.address, self.port))

    def __send_command(self, cmd="", parameters=None, timeout=300,
                       return_before_done=False):
        """

        :param cmd: string command to send to the camera socket
        :param parameters: list of parameters associated with cmd
        :param timeout: timeout in seconds for waiting for a command
        :return: Tuple (bool,string)
        """
        start = time.time()
        try:
            if timeout:
                self.socket.settimeout(timeout)

            if parameters:
                send_str = json.dumps({'command': cmd,
                                       'parameters': parameters})
            else:
                send_str = json.dumps({'command': cmd})

            self.socket.send(b"%s" % send_str.encode('utf-8'))

            if return_before_done:
                return {"elaptime": time.time() - start,
                        "data": "exiting the loop early"}

            data = self.socket.recv(2048)
            counter = 0
            while not data:
                time.sleep(.1)
                data = self.socket.recv(2048)
                counter += 1
                if counter > 100:
                    break
            return json.loads(data.decode('utf-8'))
        except Exception as e:
            return {'elaptime': time.time() - start,
                    'error': str(e)}

    def initialize(self):
        print('Initializing Camera')
        return self.__send_command(cmd="INITIALIZE")

    def shutdown(self):
        return self.__send_command(cmd="SHUTDOWN")

    def status(self):
        return self.__send_command(cmd="STATUS")

    def get_temp_status(self):
        return self.__send_command(cmd="GETTEMPSTATUS")

    def prefix(self):
        return self.__send_command(cmd="PREFIX")

    def take_image(self, shutter='normal', exptime=0.0, readout=2.0,
                   save_as="", return_before_done=False):

        parameters = {'shutter': shutter, "exptime": exptime,
                      "readout": readout, "save_as": save_as}
        return self.__send_command(cmd="TAKE_IMAGE", parameters=parameters,
                                   return_before_done=return_before_done)

    def listen(self):
        data = self.socket.recv(2048)
        counter = 0
        while not data:
            time.sleep(.1)
            data = self.socket.recv(2048)
            counter += 1
            if counter > 100:
                break
        return json.loads(data.decode('utf-8'))


if __name__ == '__main__':
    ifu = Camera(address=cam_cfg['ifu_ip'], port=cam_cfg['ifu_port'])
    print(ifu.initialize())
    # print(ifu.status())
    print(ifu.take_image(exptime=0, save_as='', readout=1.0,
                         return_before_done=False))
    ifu.shutdown()
    # print(ifu.status())
    # print(ifu.status())
    # print(ifu.shutdown())
