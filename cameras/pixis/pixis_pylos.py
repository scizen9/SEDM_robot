import time
import datetime
import os
from cameras.pixis import *
# import logging
# from logging.handlers import TimedRotatingFileHandler
import json
from astropy.io import fits
from utils.transfer_to_remote import transfer

SITE_ROOT = os.path.abspath(os.path.dirname(__file__)+'/../..')

with open(os.path.join(SITE_ROOT, 'config', 'logging.json')) as data_file:
    params = json.load(data_file)

with open(os.path.join(SITE_ROOT, 'config', 'cameras.json')) as data_file:
    cam_cfg = json.load(data_file)

_remote_config = cam_cfg["remote_config"]  # default remote config file

# logger = logging.getLogger("pixisLogger")
# logger.setLevel(logging.DEBUG)
# logging.Formatter.converter = time.gmtime
# formatter = logging.Formatter("%(asctime)s--%(name)s--%(levelname)s--"
#                              "%(module)s--%(funcName)s--%(message)s")

# logHandler = TimedRotatingFileHandler(os.path.join(params['abspath'],
#                                                   'pixis_controller.log'),
#                                      when='midnight', utc=True, interval=1,
#                                      backupCount=360)
# logHandler.setFormatter(formatter)
# logHandler.setLevel(logging.DEBUG)
# logger.addHandler(logHandler)
# logger.info("Starting Logger: Logger file is %s", 'pixis_controller.log')


class Controller:
    def __init__(self, cam_prefix="rc", serial_number="test", output_dir="",
                 force_serial=True, set_temperature=-50, send_to_remote=False,
                 remote_config=_remote_config):
        """
        Initialize the controller for the PIXIS camera and
        :param cam_prefix:
        :param serial_number:
        :param output_dir:
        :param force_serial:
        :param set_temperature:
        :param send_to_remote:
        :param remote_config:
        """

        self.camPrefix = cam_prefix
        self.serialNumber = serial_number
        self.outputDir = output_dir
        self.forceSerial = force_serial
        self.setTemperature = set_temperature
        self.opt = None
        self.ROI = [1, 2048, 1, 2048]
        self.ActiveWidth = 2048
        self.ActiveHeight = 2048
        self.ActiveLeftMargin = 54
        self.ActiveRightMargin = 50
        self.ActiveTopMargin = 7
        self.ActiveBottomMargin = 3
        self.AdcSpeed = 2.0
        self.AdcAnalogGain = "Medium"
        self.AdcQuality = "LowNoise"
        self.ExposureTime = 0
        self.lastExposed = None
        self.telescope = '60'
        self.gain = -999
        self.crpix1 = -999
        self.crpix2 = -999
        self.cdelt1 = -999
        self.cdelt2 = -999
        self.cdelt1_comment = ""
        self.cdelt2_comment = ""
        self.ctype1 = 'RA---TAN'
        self.ctype2 = 'DEC--TAN'
        self.send_to_remote = send_to_remote
        if self.send_to_remote:
            with open(os.path.join(SITE_ROOT, 'config',
                                   remote_config)) as parm_file:
                parms = json.load(parm_file)
                print(parms, "params")
            self.transfer = transfer(**parms)
        self.shutter_dict = {
            'normal': 'Normal',
            'closed': 'AlwaysClosed',
            'open': 'AlwaysOpen',
        }

        self.AdcSpeed_States = [.1, 2.0]
        self.AdcAnalogGain_States = ['Low', 'Medium', 'High']
        self.AdcQuality_States = ["LowNoise", "HighCapacity", "HighSpeed",
                                  "ElectronMultiplied"]
        self.lastError = ""

    def _set_output_dir(self):
        """
        Keep data seperated by utdate.  Unless saveas is defined all
        files will be saved in a utdate directory in the output directory.
        :return: str output directory path
        """
        return os.path.join(self.outputDir,
                            datetime.datetime.utcnow().strftime("%Y%m%d"))

    def _set_shutter(self, shutter):
        # Start off by setting the shutter mode
        # logger.info("Setting shutter to state:%s", shutter)
        # 1. Make sure shutter state is correct string format
        shutter = shutter.lower()
        shutter_list = []

        if shutter in self.shutter_dict:
            shutter_list.append(
                ['ShutterTimingMode',
                 PicamShutterTimingMode[self.shutter_dict[shutter]]])
            shutter_list.append(["ShutterClosingDelay", 0])
            return shutter_list
        else:
            # logger.error('%s is not a valid shutter state',
            # shutter, exc_info=True)
            self.lastError = '%s is not a valid shutter state' % shutter
            return False

    def _set_parameters(self, parameters, commit=True):
        """
        Set the parameters.  The return is the calculated readout time
        based on the active parameters.
        :return:
        """

        for param in parameters:
            self.opt.setParameter(param[0], param[1])

        if commit:
            self.opt.sendConfiguration()

        return self.opt.getParameter("ReadoutTimeCalculation")

    # def log_rollover(self):
        # logger.removeHandler(logHandler)
        # logHandler.doRollover()
        # logger.addHandler(logHandler)
        # logger.info("New log")
        # print("Log rollover")

    def initialize(self, path_to_lib="", wait_to_cool=False):
        """
        Initialize the library and connect the cameras.  When no camera
        is detected the system opens a demo cam up for testing.

        :param path_to_lib: Location of the dll or .so library
        :param wait_to_cool: If true then wait in this function until
        the camera is at it's set temperature
        :return:
        """

        # Initialize and load the PICAM library
        # logger.info("Loading PICAM libaray")
        try:
            self.opt = picam()
            self.opt.loadLibrary(path_to_lib)
        except Exception as e:
            print("Error loading the picam library:", str(e))
            self.lastError = str(e)
            # logger.error("Fatal error in main loop", exc_info=True)
            return False
        # logger.info("Finished loading libary")
        # logger.info("Getting available cameras")

        # Get the available cameras and try to select the one desired by the
        # serial number written on the back of the cameras themselves
        camera_list = self.opt.getAvailableCameras()
        camera_list = [camera.decode('utf-8') for camera in camera_list]
        # logger.info("Available Cameras:%s", camera_list)
        if self.serialNumber:
            try:
                pos = camera_list.index(self.serialNumber)
            except Exception as e:
                self.lastError = str(e)
                # logger.error("Camera %s is not in list", self.serialNumber,
                # exc_info=True)
                return False
        else:
            # logger.info("No serial number given, using demo cam")
            pos = None
            self.serialNumber = 'Demo'
        # logger.info("Connecting '%s' camera", self.serialNumber)

        # Connect the camera for operations
        try:
            self.opt.connect(pos)
        except Exception as e:
            print("ERROR")
            self.lastError = str(e)
            # logger.info("Unable to connect to camera:%s", self.serialNumber)
            # logger.error("Connection error", exc_info=True)
            return False
        print("Waiting")

        # Set default parameters
        try:
            self.opt.setParameter("ActiveWidth", self.ActiveWidth)
            self.opt.setParameter("ActiveHeight", self.ActiveHeight)
            self.opt.setParameter("ActiveLeftMargin", self.ActiveLeftMargin)
            self.opt.setParameter("ActiveRightMargin", self.ActiveRightMargin)
            self.opt.setParameter("ActiveTopMargin", self.ActiveTopMargin)
            self.opt.setParameter("ActiveBottomMargin", self.ActiveBottomMargin)
            self.opt.sendConfiguration()
        except Exception as e:
            self.lastError = str(e)
            # logger.error("Error setting default configuration", exc_info=True)
            return False

        # Set default Adc values
        try:
            self.opt.setParameter('AdcAnalogGain',
                                  PicamAdcAnalogGain[self.AdcAnalogGain])
            self.opt.setParameter('AdcQuality',
                                  PicamAdcQuality[self.AdcQuality])
            self.opt.setParameter('TimeStamps',
                                  PicamTimeStampsMask['ExposureStarted'])

            self.opt.sendConfiguration()
        except Exception as e:
            self.lastError = str(e)
            # logger.error("Error setting the Adc values", exc_info=True)
            return False

        # Make sure the base data directory exists:
        if self.outputDir:
            if not os.path.exists(self.outputDir):
                self.lastError = "Image directory does not exists"
                # logger.error("Image directory %s does not exists",
                # self.outputDir)
                return False

        # Set the camera properties
        if self.camPrefix == 'rc':
            self.crpix1 = 1293
            self.crpix2 = 1280
            self.cdelt1 = -0.00010944
            self.cdelt2 = -0.00010944
            self.cdelt1_comment = '.394"'
            self.cdelt2_comment = '.394"'
            self.gain = 1.77
        elif self.camPrefix == 'ifu':
            self.gain = 1.78
            self.crpix1 = 1075
            self.crpix2 = 974
            self.cdelt1 = -2.5767E-06
            self.cdelt2 = -2.5767E-06
            self.cdelt1_comment = ".00093"
            self.cdelt2_comment = ".00093"
        else:
            self.crpix1 = 1293
            self.crpix2 = 1280
            self.cdelt1 = -0.00010944
            self.cdelt2 = -0.00010944
            self.cdelt1_comment = '.394"'
            self.cdelt2_comment = '.394"'

        # Set the operating temperature and wait to cool the instrument
        # before continuing. We wait for this cooling to occur because
        # past experience has shown working with the cameras during the
        # cooling cycle can cause issues.
        # logger.info("Setting temperature to: %s", self.setTemperature)
        self.opt.setParameter("SensorTemperatureSetPoint",
                              self.setTemperature)
        self.opt.sendConfiguration()

        if wait_to_cool:
            temp = self.opt.getParameter("SensorTemperatureReading")
            lock = self.opt.getParameter("SensorTemperatureStatus")
            while temp != self.setTemperature:
                # logger.debug("Dector temp at %sC", temp)
                print(lock, temp)
                time.sleep(5)
                temp = self.opt.getParameter("SensorTemperatureReading")
                lock = self.opt.getParameter("SensorTemperatureStatus")

            while lock != 2:
                print(lock)
                # logger.debug("Wait for temperature lock to be set")
                lock = self.opt.getParameter("SensorTemperatureStatus")
                time.sleep(10)
            # logger.info("Camera temperature locked in place. Continuing "
            #            "initialization")

        return True

    def get_temp_status(self):
        """Return temperature and lock status"""
        locked = False
        temp = 0.
        try:
            temp = self.opt.getParameter("SensorTemperatureReading")
            lock = self.opt.getParameter("SensorTemperatureStatus")
            locked = (lock == 2)
            return {'camtemp': temp, 'templock': locked}
        except Exception as e:
            return {'error': str(e), 'camtemp': temp, 'templock': locked}

    def get_status(self):
        """Simple function to return camera information that can be displayed
         on the website"""
        try:
            status = {
                'camexptime': self.opt.getParameter("ExposureTime"),
                'camtemp': self.opt.getParameter("SensorTemperatureReading"),
                'camspeed': self.opt.getParameter("AdcSpeed"),
                'state': self.opt.getParameter("OutputSignal")
            }
            # logger.info(status)
            return status
        except Exception as e:
            # logger.error("Error getting the camera status", exc_info=True)
            return {
                "error": str(e), "camexptime": -9999,
                "camtemp": -9999, "camspeed": -999
            }

    def get_camera_state(self):
        self.opt.getParameter()

    def take_image(self, shutter='normal', exptime=0.0,
                   readout=2.0, save_as="", timeout=None):

        s = time.time()
        parameter_list = []
        readout_time = 5
        exptime_ms = 0

        print(self.opt.getParameter('TimeStamps'), 'timestamp')
        # 1. Set the shutter state
        shutter_return = self._set_shutter(shutter)
        if shutter_return:
            parameter_list += shutter_return
        else:
            return {'elaptime': time.time()-s,
                    'error': "Error setting shutter state"}

        # 2. Convert exposure time to ms
        try:
            exptime_ms = int(float(exptime) * 1000)
            # logger.info("Converting exposure time %(exptime)ss"
            #            " to %(exptime_ms)s"
            #            "milliseconds", {'exptime': exptime,
            #                             'exptime_ms': exptime_ms})
            parameter_list.append(['ExposureTime', exptime_ms])
        except Exception as e:
            self.lastError = str(e)
            # logger.error("Error setting exposure time", exc_info=True)

        # 3. Set the readout speed
        # logger.info("Setting readout speed to: %s", readout)
        if readout not in self.AdcSpeed_States:
            # logger.error("Readout speed '%s' is not valid", readout)
            return {'elaptime': time.time()-s,
                    'error': "%s not in AdcSpeed states" % readout}
        parameter_list.append(['AdcSpeed', readout])

        # 4. Set parameters and get readout time
        try:
            # logger.info("Sending configuration to camera")
            readout_time = self._set_parameters(parameter_list)
            # r = int(readout_time) / 1000
            # logger.info("Expected readout time=%ss", r)
        except Exception as e:
            self.lastError = str(e)
            # logger.error("Error setting parameters", exc_info=True)

        # 5. Set the timeout return for the camera
        if not timeout:
            timeout = int(int(readout_time) + exptime_ms + 100000)
        else:
            timeout = 100000000

        # 6. Get the exposure start time to use for the naming convention
        start_time = datetime.datetime.utcnow()

        self.lastExposed = str(start_time)
        # logger.info("Starting %(camPrefix)s exposure",
        #            {'camPrefix': self.camPrefix})
        try:
            frame_data = self.opt.readNFrames(N=1, timeout=timeout)[0][0]
            print(frame_data, "this is the response")
        except Exception as e:
            self.lastError = str(e)
            # logger.error("Unable to get camera data", exc_info=True)
            return {'elaptime': -1*(time.time()-s),
                    'error': "Failed to gather data from camera",
                    'send_alert': True}
        # logger.info("Readout completed")
        # logger.debug("Took: %s", time.time() - s)

        if not save_as:
            start_exp_time = start_time.strftime("%Y%m%d_%H_%M_%S")
            # Now make sure the utdate directory exists
            if not os.path.exists(os.path.join(self.outputDir,
                                               start_exp_time[:8])):
                # logger.info("Making directory: %s",
                # os.path.join(self.outputDir, start_exp_time[:8]))

                os.mkdir(os.path.join(self.outputDir, start_exp_time[:8]))

            save_as = os.path.join(self.outputDir, start_exp_time[:8],
                                   self.camPrefix+start_exp_time+'.fits')

        try:
            print("ti: writing to: ", save_as)
            datetimestr = start_time.isoformat()
            datestr, timestr = datetimestr.split('T')
            hli = fits.PrimaryHDU(frame_data, uint=False)
            hli.scale('int16', bzero=32768)
            hli.header.set("EXPTIME", float(exptime),
                           "Exposure Time in seconds")
            hli.header.set("ADCSPEED", readout, "Readout speed in MHz")
            hli.header.set("TEMP",
                           self.opt.getParameter("SensorTemperatureReading"),
                           "Detector temp in deg C")
            hli.header.set("GAIN_SET", 2, "Gain mode")
            hli.header.set("ADC", 1, "ADC Quality")
            hli.header.set("MODEL", 22, "Instrument Mode Number")
            hli.header.set("INTERFC", "USB", "Instrument Interface")
            hli.header.set("SNSR_NM", "E2V 2048 x 2048 (CCD 42-40)(B)",
                           "Sensor Name")
            hli.header.set("SER_NO", self.serialNumber, "Serial Number")
            hli.header.set("TELESCOP", self.telescope, "Telescope ID")
            hli.header.set("GAIN", self.gain, "Gain")
            hli.header.set("CAM_NAME", "%s Cam" % self.camPrefix.upper(),
                           "Camera Name")
            hli.header.set("INSTRUME", "SEDM-P60", "Camera Name")
            hli.header.set("UTC", start_time.isoformat(), "UT-Shutter Open")
            hli.header.set("END_SHUT", datetime.datetime.utcnow().isoformat(),
                           "Shutter Close Time")
            hli.header.set("OBSDATE", datestr, "UT Start Date")
            hli.header.set("OBSTIME", timestr, "UT Start Time")
            hli.header.set("CRPIX1", self.crpix1, "Center X pixel")
            hli.header.set("CRPIX2", self.crpix2, "Center Y pixel")
            hli.header.set("CDELT1", self.cdelt1, self.cdelt1_comment)
            hli.header.set("CDELT2", self.cdelt2, self.cdelt2_comment)
            hli.header.set("CTYPE1", self.ctype1)
            hli.header.set("CTYPE2", self.ctype2)
            hli.writeto(save_as, output_verify="fix", )
            # logger.info("%s created", save_as)
            if self.send_to_remote:
                print(save_as)
                ret = self.transfer.send(save_as)
                print(ret, 'transfer return')
                if 'data' in ret:
                    save_as = ret['data']
            return {'elaptime': time.time()-s, 'data': save_as}
        except Exception as e:
            self.lastError = str(e)
            print(str(e))
            # logger.error("Error writing data to disk", exc_info=True)
            return {'elaptime': time.time()-s,
                    'error': 'Error writing file to disk'}


if __name__ == "__main__":
    x = Controller(serial_number="", output_dir='C:/images',
                   send_to_remote=True)
    # y = Controller()
    print(x.initialize())
    if x.initialize():
        print("Camera initialized")
    else:
        print("I need to handle this error")
    for i in range(1):
        # print(y.take_image(exptime=0, readout=2.0))
        print(x.take_image(exptime=0, readout=2))
    print("I made it here")
    time.sleep(10)
