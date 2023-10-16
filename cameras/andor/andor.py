import time
import datetime
import os
from cameras.andor.andorLib import *
import logging
from logging.handlers import TimedRotatingFileHandler
import json
from astropy.io import fits
# from utils.transfer_to_remote import transfer
from utils.transfer_to_remote import transfer

SITE_ROOT = os.path.abspath(os.path.dirname(__file__) + '/../..')

with open(os.path.join(SITE_ROOT, 'config', 'logging.json')) as data_file:
    params = json.load(data_file)

# Andor Test Update to GitHub
logger = logging.getLogger("andorLogger")
logger.setLevel(logging.DEBUG)
logging.Formatter.converter = time.gmtime
formatter = logging.Formatter("%(asctime)s--%(name)s--%(levelname)s--"
                              "%(module)s--%(funcName)s--%(message)s")

logHandler = TimedRotatingFileHandler(os.path.join(params['abspath'],
                                                   'andor_controller.log'),
                                      when='midnight', utc=True, interval=1,
                                      backupCount=360)
logHandler.setFormatter(formatter)
logHandler.setLevel(logging.DEBUG)
logger.addHandler(logHandler)
logger.info("Starting Logger: Logger file is %s", 'andor_controller.log')


def status_msg(msg):
    now = datetime.utcnow()
    dt_string = now.strftime("%d-%m-%Y %H:%M:%S andor:")
    msg_return = f'{dt_string} {msg}'
    print(msg_return)


class Controller:
    def __init__(self, cam_prefix="ifu", serial_number="test",
                 camera_handle=None, output_dir="",
                 force_serial=True, set_temperature=-50, send_to_remote=False,
                 remote_config='nemea.config.json'):
        """
        Initialize the controller for the ANDOR camera and
        :param cam_prefix:
        :param serial_number:
        :param output_dir:
        :param force_serial:
        :param set_temperature:
        """

        self.camPrefix = cam_prefix
        self.serialNumber = serial_number
        self.cameraHandle = camera_handle
        self.outputDir = output_dir
        self.forceSerial = force_serial
        self.setTemperature = set_temperature
        self.opt = None
        self.ROI = [1, 1, 1, 2088, 1, 2048]

        self.AdcSpeed = 1.0  # Horizontal Shift Speed
        self.AdcAnalogGain = "High"  # Pre Amp Gain
        self.AdcQuality = "HighSensitivity"  # Output Amplification

        self.VerticalShiftSpeed = 77
        self.ReadMode = "Image"
        self.AcquisitionMode = "SingleScan"
        self.ShutterTTLMode = "high"
        self.ShutterMode = "normal"
        self.ShutterOpenTimeMs = 12
        self.ShutterCloseTimeMs = 14

        self.ExposureTime = 0
        self.lastExposed = None
        self.telescope = 'P60'
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
        # status variables
        self.exposip = False
        self.camexptime = 0.
        self.camspeed = 0.
        self.camtemp = 0
        # what to do with images
        if self.send_to_remote:
            with open(os.path.join(SITE_ROOT, 'config',
                                   remote_config)) as conf_data_file:
                tparams = json.load(conf_data_file)
                print(tparams, "params")
            self.transfer = transfer(**tparams)
        # possible values
        self.shutter_dict = {
            'normal': 0,  # Values are index as per andor SDK documentation
            'open': 1,  # Values are index as per andor SDK documentation
            'closed': 2,  # Values are index as per andor SDK documentation
        }

        self.shutter_ttl_modes = {
            'low': 0,   # Output TTL low signal to open shutter
            'high': 1,  # Output TTL high signal to open shutter
        }

        # Assuming this is the same as the Horizontal Shift Speed in Andor (MHz)
        self.AdcSpeed_States = {5.0: 0,
                                3.0: 1,
                                1.0: 2,
                                .05: 3}

        # Assuming this is the same as the pre amp gain in Andor
        self.AdcAnalogGain_States = {'Low': 0,  # Scales by 1.0
                                     'Medium': 1,  # Scales by 2,0
                                     'High': 2  # Scales by 4.0
                                     }

        # Assuming this is the same as the Output Amplification in Andor
        self.AdcQuality_States = {"HighSensitivity": 0}  # Only Output Amp Available for Andor

        self.VerticalShiftSpeed_States = {38: 0,
                                          77: 1}
        self.ReadModes = {"FVB": 0,
                          "Multi-Track": 1,
                          "Random-Track": 2,
                          "Single-Track": 3,
                          "Image": 4}

        self.AcquisitionModes = {"SingleScan": 1,
                                 "Accumulate": 2,
                                 "Kinetics": 3,
                                 "FastKinetics": 4,
                                 "RunTillAbort": 5}
        self.lastError = ""

    def _set_output_dir(self):
        """
        Keep data seperated by utdate.  Unless saveas is defined all
        files will be saved in a utdate directory in the output directory.
        :return: str output directory path
        """
        return os.path.join(self.outputDir,
                            datetime.datetime.utcnow().strftime("%Y%m%d"))

    # Modifications to this Function are Completed
    def _set_shutter(self, shutter):
        # Start off by setting the shutter mode
        logger.info("Setting shutter to state:%s", shutter)
        # 1. Make sure shutter state is correct string format
        shutter = shutter.lower()
        if shutter in self.shutter_dict:
            self.opt.SetShutter(self.shutter_ttl_modes[self.ShutterTTLMode],
                                self.shutter_dict[shutter],
                                self.ShutterOpenTimeMs,
                                self.ShutterCloseTimeMs)
            return True
        else:
            logger.error('%s is not a valid shutter state', shutter,
                         exc_info=True)
            self.lastError = '%s is not a valid shutter state' % shutter
            return False

    def _set_parameters(self, parameters, commit=True):  # To be changed (Most likely just delete this)
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

    def initialize(self, path_to_lib="", wait_to_cool=False):
        """
        Initialize the library and connect the cameras.  When no camera
        is detected the system opens a demo cam up for testing.
        :param path_to_lib: Location of the dll or .so library
        :param wait_to_cool: If true then wait in this function until
        the camera is at it's set temperature
        :return:
        """

        # Initialize and load the Andor library
        # may need to be given the path, in which case
        # path_to_lib = "/usr/local/lib/libandor.so"
        logger.info("Loading Andor SDK library")
        try:
            self.opt = Andor()
            self.opt.loadLibrary()
        except Exception as e:
            self.lastError = str(e)
            logger.error("Fatal error in main loop", exc_info=True)
            return False

        logger.info("Finished loading library")
        logger.info("Getting available cameras")

        # Get the available cameras and try to select the one desired by the
        # serial number written on the back of the cameras themselves
        # Handles are found in the config file
        connected_cams = self.opt.GetAvailableCameras()
        camera_list = []
        for cams in range(connected_cams):
            handles = self.opt.GetCameraHandle(cams)
            camera_list.append(handles)
            # camera_list.append(self.opt.GetCameraHandle(cams))

        logger.info("Available Cameras:%s", camera_list)
        if self.cameraHandle:
            try:
                pos = camera_list.index(self.cameraHandle)
                status_msg(f'Camera Handle Index Position: {pos}')
            except Exception as e:
                self.lastError = str(e)
                logger.error("Camera %s is not in list", self.cameraHandle,
                             exc_info=True)
                return False
        logger.info("Connecting '%s' camera", self.cameraHandle)

        # Connect the camera and initializes it for operations
        try:
            self.opt.SetCurrentCamera(camera_list[pos])
            self.opt.Initialize()
        except Exception as e:
            print("ERROR")
            self.lastError = str(e)

            logger.info("Unable to connect to camera:%s", self.cameraHandle)
            logger.error("Connection error", exc_info=True)
            return False

        # Set the operating temperature and wait to cool the instrument
        # before continuing. We wait for this cooling to occur because
        # past experience has shown working with the cameras during the
        # cooling cycle can cause issues.

        logger.info("Setting temperature to: %s", self.setTemperature)
        self.opt.SetTemperature(self.setTemperature)

        self.opt.IsCoolerOn()
        self.opt.CoolerON()
        if wait_to_cool:
            temp = self.opt.GetTemperature()[1]
            lock = self.opt.GetTemperature()[0]

            while lock != 'DRV_TEMP_STABILIZED':
                print(lock, temp)
                logger.debug("Wait for temperature lock to be set")
                temp = self.opt.GetTemperature()[1]
                lock = self.opt.GetTemperature()[0]
                time.sleep(10)
            logger.info("Camera temperature locked in place. Continuing "
                        "initialization")

        # Sets Default Parameters
        try:
            self.serialNumber = self.opt.GetCameraSerialNumber()
            self.opt.SetImageFlip(1, 1)     # Image flip on both axes
            self.opt.SetImageRotate(0)      # Image rotation disabled
            self.opt.SetBaselineClamp(0)    # Baseline clamp disabled
            self.opt.SetFanMode(2)      # set to 2 (OFF) for liquid cooling
            self.opt.SetADChannel(0)    # First (and only?) ADC channel
            self.opt.SetCoolerMode(1)   # Temperature maintained on shutdown
            self.opt.SetFrameTransferMode(0)    # Frame Transfer disabled
            self.opt.SetPhotonCounting(0)   # Photon counting disabled
            self.opt.SetKineticCycleTime(0.0)   # ?
            self.opt.SetVSAmplitude(0)  # Normal vert. clock voltage amplitude
            self.opt.SetVSSpeed(self.VerticalShiftSpeed_States[
                                    self.VerticalShiftSpeed])
            self.opt.SetReadMode(self.ReadModes[self.ReadMode])
            self.opt.SetAcquisitionMode(self.AcquisitionModes[
                                            self.AcquisitionMode])
            self.opt.SetShutter(self.shutter_ttl_modes[self.ShutterTTLMode],
                                self.shutter_dict[self.ShutterMode],
                                self.ShutterOpenTimeMs,
                                self.ShutterCloseTimeMs)
            self.opt.SetImage(hbin=self.ROI[0],
                              vbin=self.ROI[1],
                              hstart=self.ROI[2],
                              hend=self.ROI[3],
                              vstart=self.ROI[4],
                              vend=self.ROI[5]
                              )

        except Exception as e:
            self.lastError = str(e)
            logger.error("Error setting default configuration", exc_info=True)
            return False

        # Make sure the base data directory exists:
        if self.outputDir:
            if not os.path.exists(self.outputDir):
                self.lastError = "Image directory does not exists"
                logger.error("Image directory %s does not exists",
                             self.outputDir)
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
            self.gain = 0.9
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
            self.gain = 0.9
        return True

    def get_acq_status(self):
        status = self.opt.GetStatus()
        return status

    def get_status(self):
        """Simple function to return camera information that can be displayed
         on the website"""
        if self.exposip:
            status = {'camexptime': self.camexptime,
                      'camtemp': self.camtemp,
                      'camspeed': self.camspeed,
                      'state': 'exp'
                      }
            logger.info(status)
            return status
        else:
            try:
                exargs = self.opt.GetAcquisitionTimings()
                camexptime = exargs[0]
                self.camexptime = camexptime
                logger.info("Got camexptime")
                tmpargs = self.opt.GetTemperature()
                camtemp = tmpargs[1]
                self.camtemp = camtemp
                logger.info("Got camtemp")
                camspeed = self.opt.GetHSSpeed(0, self.AdcQuality_States[self.AdcQuality],
                                               self.AdcSpeed_States[self.AdcSpeed])
                self.camspeed = camspeed
                logger.info("Got camspeed")
                sttargs = self.opt.GetTemperatureRange()
                state = sttargs[0]
                logger.info("Got state")
                status = {
                    'camexptime': camexptime,
                    'camtemp': camtemp,
                    'camspeed': camspeed,
                    'state': 'idl'
                }
                logger.info(status)
                return status
            except Exception as e:
                logger.error("Error getting the camera status", exc_info=True)
                return {
                    "error": str(e), "camexptime": -9999,
                    "camtemp": -9999, "camspeed": -999
                }

    def get_temp_status(self):
        """Return temperature and lock status"""
        locked = False
        temp = 0.
        try:
            retargs = self.opt.GetTemperature()
            lock = retargs[0]
            temp = retargs[1]
            logger.info("status: %s", lock)
            locked = (lock == 'DRV_TEMP_STABILIZED')
            return {'camtemp': temp, 'templock': locked}
        except Exception as e:
            return {'error': str(e), 'camtemp': temp, 'templock': locked}

    def take_image(self, shutter='normal', exptime=0.0,
                   readout=1.0, save_as="", timeout=None):
        s = time.time()
        self.exposip = True
        self.camexptime = exptime
        self.camspeed = readout

        # 1. Set the shutter state
        shutter_return = self._set_shutter(shutter)
        if not shutter_return:
            self.exposip = False
            return {'elaptime': time.time() - s,
                    'error': "Error setting shutter state"}

        # 2. Andor exposure times are in seconds
        try:
            self.opt.SetExposureTime(exptime)
        except Exception as e:
            self.lastError = str(e)
            logger.error("Error setting exposure time", exc_info=True)

        # 3. Set the readout speed
        logger.info("Setting readout speed to: %s", readout)
        if readout not in self.AdcSpeed_States:
            logger.error("Readout speed '%s' is not valid", readout)
            self.exposip = False
            return {'elaptime': time.time() - s,
                    'error': "%s not in AdcSpeed states" % readout}
        self.opt.SetPreAmpGain(self.AdcAnalogGain_States[self.AdcAnalogGain])
        self.opt.SetHSSpeed(self.AdcQuality_States[self.AdcQuality],
                            self.AdcSpeed_States[readout])

        # 6. Get the exposure start time to use for the naming convention
        start_time = datetime.utcnow()

        self.lastExposed = start_time
        logger.info("Starting %(camPrefix)s exposure",
                    {'camPrefix': self.camPrefix})
        try:
            self.opt.StartAcquisition()
            acq_status = self.opt.GetStatus()
            while 'DRV_ACQUIRING' in acq_status:
                logger.warning("Still acquiring, wait 1s")
                time.sleep(1)
                acq_status = self.opt.GetStatus()
            logger.info("Ready to get data: %(acq_status)s",
                        {'acq_status': acq_status})
            imdata = []
            self.opt.GetAcquiredData16(imdata, width=self.ROI[3],
                                       height=self.ROI[5])
            end_time = datetime.utcnow()
        except Exception as e:
            self.lastError = str(e)
            logger.error("Unable to get camera data", exc_info=True)
            self.exposip = False
            return {'elaptime': -1 * (time.time() - s),
                    'error': "Failed to gather data from camera",
                    'send_alert': True}
        if len(imdata) <= 0:
            logger.error("GetAcquiredData16 produced empty array!",
                         exc_info=True)
            self.exposip = False
            return {'elaptime': -1 * (time.time() - s),
                    'error': "Failed to gather data from camera",
                    'send_alert': True}
        logger.info("Readout completed")
        logger.debug("Took: %s", time.time() - s)

        if not save_as:
            start_exp_time = start_time.strftime("%Y%m%d_%H_%M_%S")
            # Now make sure the utdate directory exists
            if not os.path.exists(os.path.join(self.outputDir,
                                               start_exp_time[:8])):
                logger.info("Making directory: %s",
                            os.path.join(self.outputDir, start_exp_time[:8]))

                os.mkdir(os.path.join(self.outputDir, start_exp_time[:8]))

            save_as = os.path.join(self.outputDir, start_exp_time[:8],
                                   self.camPrefix + start_exp_time + '.fits')

        try:
            datetimestr = start_time.isoformat()
            datestr, timestr = datetimestr.split('T')
            hdul = fits.PrimaryHDU(self.opt.imageArray, uint=True)
            hdul.scale('int16', bzero=32768)
            hdul.header.set("EXPTIME", float(exptime),
                            "Exposure Time in seconds")
            hdul.header.set("ADCSPEED", readout, "Readout speed in MHz")
            hdul.header.set("VSSPEED", self.VerticalShiftSpeed,
                            "VS Speed in um / pixel shift")
            hdul.header.set("PAGAIN", self.AdcAnalogGain, "Pre Amp Gain")
            hdul.header.set("TEMP",
                            self.opt.GetTemperature()[1],
                            "Detector temp in deg C")
            hdul.header.set("PSCANX0", 1, "Pre-scan column start")
            hdul.header.set("PSCANX1", 20, "Pre-scan column end")
            hdul.header.set("OSCANX0", 2069, "Over-scan column start")
            hdul.header.set("OSCANX1", 2088, "Over-scan column end")
            hdul.header.set("GAIN_SET", 2, "Gain mode")
            hdul.header.set("ADC", self.AdcQuality, "ADC Quality")
            hdul.header.set("SHUTMODE", shutter, "Shutter Mode")
            hdul.header.set("MODEL", 22, "Instrument Model Number")
            hdul.header.set("INTERFC", "USB", "Instrument Interface")
            hdul.header.set("SNSR_NM", "E2V 2048 x 2048 (CCD 42-40)(B)",
                            "Sensor Name")
            hdul.header.set("SER_NO", self.serialNumber, "Serial Number")
            hdul.header.set("GAIN", self.gain, "Gain")
            hdul.header.set("CAM_NAME", "%s Cam" % self.camPrefix.upper(),
                            "Camera Name")
            hdul.header.set("INSTRUME", "SEDM-P60", "Camera Name")
            hdul.header.set("TELESCOP", self.telescope, "Telescope ID")
            hdul.header.set("UTC", start_time.isoformat(), "UT-Shutter Open")
            hdul.header.set("END_SHUT", end_time.isoformat(),
                            "Shutter Close Time")
            hdul.header.set("END_READ", end_time.isoformat(),
                            "End of Readout Time")
            hdul.header.set("OBSDATE", datestr, "UT Start Date")
            hdul.header.set("OBSTIME", timestr, "UT Start Time")
            hdul.header.set("CRPIX1", self.crpix1, "Center X pixel")
            hdul.header.set("CRPIX2", self.crpix2, "Center Y pixel")
            hdul.header.set("CDELT1", self.cdelt1, self.cdelt1_comment)
            hdul.header.set("CDELT2", self.cdelt2, self.cdelt2_comment)
            hdul.header.set("CTYPE1", self.ctype1)
            hdul.header.set("CTYPE2", self.ctype2)
            hdul.writeto(save_as, output_verify="fix", )
            logger.info("%s created", save_as)
            if self.send_to_remote:
                ret = self.transfer.send(save_as)
                if 'data' in ret:
                    save_as = ret['data']
                elif 'error' in ret:
                    retries = 1
                    transfer_worked = False
                    while retries < 5 and not transfer_worked:
                        print(ret)
                        print("Transfer ERROR: wait 5s, try again")
                        time.sleep(5)
                        ret = self.transfer.send(save_as)
                        if 'error' in ret:
                            print("Transfer try %d failed" % retries)
                            retries += 1
                        else:
                            save_as = ret['data']
                            print("Transfer succeeded after %d retries"
                                  % retries)
                            transfer_worked = True
                    if not transfer_worked:
                        print("Unable to transfer andor file to remote")
                else:
                    print("Error transferring andor file to remote")
            self.exposip = False
            return {'elaptime': time.time() - s, 'data': save_as}
        except Exception as e:
            self.lastError = str(e)
            logger.error("Error transferring andor data to remote: %s"
                         % save_as, exc_info=True)
            self.exposip = False
            return {'elaptime': time.time() - s,
                    'error': 'Error transferring andor file to remote: %s' % save_as}


if __name__ == "__main__":
    x = Controller(serial_number="", output_dir='/home/sedm/images',
                   send_to_remote=False, camera_handle=100)
    if x.initialize():
        print("Camera initialized")
    else:
        print("I need to handle this error")
    # x.take_image(exptime=0.0, readout=1.0)
