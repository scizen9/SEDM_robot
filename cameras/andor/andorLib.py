from ctypes import *
from astropy.io import fits
from datetime import datetime
import time
import numpy as np
import platform

ERROR_CODES = {'DRV_ERROR_CODES': 20001,
               'DRV_SUCCESS': 20002,
               'DRV_VXDNOTINSTALLED': 20003,
               'DRV_ERROR_SCAN': 20004,
               'DRV_ERROR_CHECK_SUM': 20005,
               'DRV_ERROR_FILELOAD': 20006,
               'DRV_UNKNOWN_FUNCTION': 20007,
               'DRV_ERROR_VXD_INIT': 20008,
               'DRV_ERROR_ADDRESS': 20009,
               'DRV_ERROR_PAGELOCK': 20010,
               'DRV_ERROR_PAGEUNLOCK': 20011,
               'DRV_ERROR_BOARDTEST': 20012,
               'DRV_ERROR_ACK': 20013,
               'DRV_ERROR_UP_FIFO': 20014,
               'DRV_ERROR_PATTERN': 20015,
               'DRV_ACQUISITION_ERRORS': 20017,
               'DRV_ACQ_BUFFER': 20018,
               'DRV_ACQ_DOWNFIFO_FULL': 20019,
               'DRV_PROC_UNKONWN_INSTRUCTION': 20020,
               'DRV_ILLEGAL_OP_CODE': 20021,
               'DRV_KINETIC_TIME_NOT_MET': 20022,
               'DRV_ACCUM_TIME_NOT_MET': 20023,
               'DRV_NO_NEW_DATA': 20024,
               'KERN_MEM_ERROR': 20025,
               'DRV_SPOOLERROR': 20026,
               'DRV_SPOOLSETUPERROR': 20027,
               'DRV_FILESIZELIMITERROR': 20028,
               'DRV_ERROR_FILESAVE': 20029,
               'DRV_TEMP_CODES': 20033,
               'DRV_TEMP_OFF': 20034,
               'DRV_TEMP_NOT_STABILIZED': 20035,
               'DRV_TEMP_STABILIZED': 20036,
               'DRV_TEMP_NOT_REACHED': 20037,
               'DRV_TEMP_OUT_RANGE': 20038,
               'DRV_TEMP_NOT_SUPPORTED': 20039,
               'DRV_TEMP_DRIFT': 20040,
               'DRV_GENERAL_ERRORS': 20049,
               'DRV_INVALID_AUX': 20050,
               'DRV_COF_NOTLOADED': 20051,
               'DRV_FPGAPROG': 20052,
               'DRV_FLEXERROR': 20053,
               'DRV_GPIBERROR': 20054,
               'DRV_EEPROMVERSIONERROR': 20055,
               'DRV_DATATYPE': 20064,
               'DRV_DRIVER_ERRORS': 20065,
               'DRV_P1INVALID': 20066,
               'DRV_P2INVALID': 20067,
               'DRV_P3INVALID': 20068,
               'DRV_P4INVALID': 20069,
               'DRV_INIERROR': 20070,
               'DRV_COFERROR': 20071,
               'DRV_ACQUIRING': 20072,
               'DRV_IDLE': 20073,
               'DRV_TEMPCYCLE': 20074,
               'DRV_NOT_INITIALIZED': 20075,
               'DRV_P5INVALID': 20076,
               'DRV_P6INVALID': 20077,
               'DRV_INVALID_MODE': 20078,
               'DRV_INVALID_FILTER': 20079,
               'DRV_I2CERRORS': 20080,
               'DRV_I2CDEVNOTFOUND': 20081,
               'DRV_I2CTIMEOUT': 20082,
               'DRV_P7INVALID': 20083,
               'DRV_P8INVALID': 20084,
               'DRV_P9INVALID': 20085,
               'DRV_P10INVALID': 20086,
               'DRV_P11INVALID': 20087,
               'DRV_USBERROR': 20089,
               'DRV_IOCERROR': 20090,
               'DRV_VRMVERSIONERROR': 20091,
               'DRV_GATESTEPERROR': 20092,
               'DRV_USB_INTERRUPT_ENDPOINT_ERROR': 20093,
               'DRV_RANDOM_TRACK_ERROR': 20094,
               'DRV_INVALID_TRIGGER_MODE': 20095,
               'DRV_LOAD_FIRMWARE_ERROR': 20096,
               'DRV_DIVIDE_BY_ZERO_ERROR': 20097,
               'DRV_INVALID_RINGEXPOSURES': 20098,
               'DRV_BINNING_ERROR': 20099,
               'DRV_INVALID_AMPLIFIER': 20100,
               'DRV_INVALID_COUNTCONVERT_MODE': 20101,
               'DRV_USB_INTERRUPT_ENDPOINT_TIMEOUT': 20102,
               'DRV_ERROR_NOCAMERA': 20990,
               'DRV_NOT_SUPPORTED': 20991,
               'DRV_NOT_AVAILABLE': 20992,
               'DRV_ERROR_MAP': 20115,
               'DRV_ERROR_UNMAP': 20116,
               'DRV_ERROR_MDL': 20117,
               'DRV_ERROR_UNMDL': 20118,
               'DRV_ERROR_BUFFSIZE': 20119,
               'DRV_ERROR_NOHANDLE': 20121,
               'DRV_GATING_NOT_AVAILABLE': 20130,
               'DRV_FPGA_VOLTAGE_ERROR': 20131,
               'DRV_OW_CMD_FAIL': 20150,
               'DRV_OWMEMORY_BAD_ADDR': 20151,
               'DRV_OWCMD_NOT_AVAILABLE': 20152,
               'DRV_OW_NO_SLAVES': 20153,
               'DRV_OW_NOT_INITIALIZED': 20154,
               'DRV_OW_ERROR_SLAVE_NUM': 20155,
               'DRV_MSTIMINGS_ERROR': 20156,
               'DRV_OA_NULL_ERROR': 20173,
               'DRV_OA_PARSE_DTD_ERROR': 20174,
               'DRV_OA_DTD_VALIDATE_ERROR': 20175,
               'DRV_OA_FILE_ACCESS_ERROR': 20176,
               'DRV_OA_FILE_DOES_NOT_EXIST': 20177,
               'DRV_OA_XML_INVALID_OR_NOT_FOUND_ERROR': 20178,
               'DRV_OA_PRESET_FILE_NOT_LOADED': 20179,
               'DRV_OA_USER_FILE_NOT_LOADED': 20180,
               'DRV_OA_PRESET_AND_USER_FILE_NOT_LOADED': 20181,
               'DRV_OA_INVALID_FILE': 20182,
               'DRV_OA_FILE_HAS_BEEN_MODIFIED': 20183,
               'DRV_OA_BUFFER_FULL': 20184,
               'DRV_OA_INVALID_STRING_LENGTH': 20185,
               'DRV_OA_INVALID_CHARS_IN_NAME': 20186,
               'DRV_OA_INVALID_NAMING': 20187,
               'DRV_OA_GET_CAMERA_ERROR': 20188,
               'DRV_OA_MODE_ALREADY_EXISTS': 20189,
               'DRV_OA_STRINGS_NOT_EQUAL': 20190,
               'DRV_OA_NO_USER_DATA': 20191,
               'DRV_OA_VALUE_NOT_SUPPORTED': 20192,
               'DRV_OA_MODE_DOES_NOT_EXIST': 20193,
               'DRV_OA_CAMERA_NOT_SUPPORTED': 20194,
               'DRV_OA_FAILED_TO_GET_MODE': 20195,
               'DRV_OA_CAMERA_NOT_AVAILABLE': 20196,
               'DRV_PROCESSING_FAILED': 20211,
               }

ERROR_STRING = dict([(ERROR_CODES[key], key) for key in ERROR_CODES])

read_modes = {0: 'Full Vertical Binning',
              1: 'Multi-Track',
              2: 'Random-Track',
              3: 'Single-Track',
              4: 'Image'}

acq_modes = {1: 'Single Scan',
             2: 'Accumulate',
             3: 'Kinetics',
             4: 'Fast Kinetics',
             5: 'Run Till Abort'}

frame_transfer_modes = {0: 'OFF',
                        1: 'ON'}

photon_counting_modes = {0: 'OFF',
                         1: 'ON'}

baseline_clamp_states = {0: 'Disabled',
                         1: 'Enabled'}

vs_amplitudes = {0: 'Normal',
                 1: '+1',
                 2: '+2',
                 3: '+3',
                 4: '+4'}

image_flip_states = {0: 'Disabled',
                     1: 'Enabled'}

image_rotation_states = {0: 'None',
                         1: '90 Deg Clockwise',
                         2: '90 Deg Counter-Clockwise'}

fan_modes = {0: 'Full',
             1: 'Low',
             2: 'OFF'}

cooling_modes = {0: 'Maintain Current Temp',
                 1: 'Return to Ambient Temp'}

shutter_ttl_modes = {0: 'Low Signal',
                     1: 'High Signal'}

shutter_modes = {0: 'Auto',
                 1: 'Open',
                 2: 'Close'}

vertical_shift_speeds = {0: '38.55',
                         1: '76.95'}

horizontal_shift_speeds = {0: '5.0',
                           1: '3.0',
                           2: '1.0',
                           3: '0.05'}

pre_amp_gains = {0: '1.00',
                 1: '2.00',
                 2: '4.00'}


def check_call(status):
    if status != ERROR_CODES['DRV_SUCCESS']:
        raise ValueError(f'Driver return {status} ({ERROR_STRING[status]})')
    return status


def status_msg(msg):
    now = datetime.now()
    dt_string = now.strftime("%d-%m-%Y %H:%M:%S")
    msg_return = f'{dt_string} {msg}'
    print(msg_return)


class Andor:
    def __init__(self):
        status_msg('Starting Andor iKon L-936 Interface')
        self.lib = None
        self.totalCameras = None
        self.cameraHandle = None
        self.current_camera_handle = None
        self.serial = None
        self.detector_width = None
        self.detector_height = None

        self.adc_channel = None
        self.num_amps = None
        self.pre_amp_gain = None

        self.vs_speed = None
        self.vs_index = None

        self.hs_speed = None
        self.hs_index = None

        self.cooler_mode = None
        self.fan_mode = None

        self.hbin = 1
        self.vbin = 1
        self.hstart = 1
        self.hend = None
        self.vstart = 1
        self.vend = None

        self.read_mode = None
        self.acquisition_mode = None
        self.acquisition_timings = None
        self.photon_counting_state = None
        self.kinetic_cycle_time = None
        self.vertical_clock_voltage_amp = None
        self.baseline_clamp_state = None
        self.frame_transfer_mode = None
        self.image_flip_states = None
        self.image_rotate_state = None
        self.exp_time = None

        self.telescope = '60'
        self.imageArray = None

    def loadLibrary(self):
        if platform.system() == "Linux":
            status_msg(f'Device OS: {platform.system()}')
            pathToLib = "/usr/local/lib/libandor.so"
            status_msg(f'Loading Andor SDK from {pathToLib}')
            self.lib = cdll.LoadLibrary(pathToLib)

    def CoolerON(self):
        status = check_call(self.lib.CoolerON())
        return ERROR_STRING[status]

    def CoolerOFF(self):
        status = check_call(self.lib.CoolerOFF())
        return ERROR_STRING[status]

    def GetAcquiredData16(self, imageArray, width, height):
        status_msg(f'Getting Acquired Data')
        dim = int(width * height / 1 / 1)
        cimageArray = c_int16 * dim
        cimage = cimageArray()
        status = check_call(self.lib.GetAcquiredData16(pointer(cimage), dim))
        for i in range(len(cimage)):
            imageArray.append(cimage[i])
        self.imageArray = np.reshape(imageArray, (height, width))
        return ERROR_STRING[status]

    def GetAcquisitionTimings(self):
        exposure = c_float()
        accumulate = c_float()
        kinetic = c_float()
        check_call(self.lib.GetAcquisitionTimings(byref(exposure), byref(accumulate), byref(kinetic)))
        self.acquisition_timings = [exposure.value, accumulate.value, kinetic.value]
        status_msg(f'Acquisition Timings: {self.acquisition_timings}')
        return self.acquisition_timings

    def GetAvailableCameras(self):
        totalCameras = c_long()
        check_call(self.lib.GetAvailableCameras(byref(totalCameras)))
        n_cams = totalCameras.value
        status_msg(f"System Detected {n_cams} Andor Camera(s)")
        return n_cams

    def GetBaselineClamp(self):
        state = c_int()
        check_call(self.lib.GetBaselineClamp(byref(state)))
        return state.value

    def GetBitDepth(self, channel):
        depth = c_int()
        check_call(self.lib.GetBitDepth(c_int(channel), byref(depth)))
        return depth.value

    def GetCameraHandle(self, cameraIndex):
        status_msg(f'Getting Camera Handle')
        cameraHandle = c_long()
        check_call(self.lib.GetCameraHandle(cameraIndex, byref(cameraHandle)))
        return cameraHandle.value

    def GetCameraSerialNumber(self):
        serial = c_int()
        check_call(self.lib.GetCameraSerialNumber(byref(serial)))
        status_msg(f'Camera Serial Number: {serial.value}')
        return serial.value

    def GetCurrentCamera(self):
        cameraHandle = c_long()
        check_call(self.lib.GetCurrentCamera(byref(cameraHandle)))
        return cameraHandle.value

    def GetDetector(self):
        detector_width = c_int()
        detector_height = c_int()
        check_call(self.lib.GetDetector(byref(detector_width),
                                        byref(detector_height)))
        detector_dims = [detector_width.value, detector_height.value]
        return detector_dims

    def GetFastestRecommendedVSSpeed(self):
        fastest_vss_index = c_int()
        fastest_vss_speed = c_float()
        check_call(self.lib.GetFastestRecommendedVSSpeed(byref(
            fastest_vss_index), byref(fastest_vss_speed)))
        fastest_vss = [fastest_vss_index.value, fastest_vss_speed.value]
        return fastest_vss

    def GetHSSpeed(self, adc_channel, output_amp, hss_index):
        hs_speed = c_float()
        check_call(
            self.lib.GetHSSpeed(c_int(adc_channel), c_int(output_amp),
                                c_int(hss_index), byref(hs_speed)))
        hs_speed = hs_speed.value
        return hs_speed

    def GetImageFlip(self):
        iHFlip = c_int()
        iVFlip = c_int()
        check_call(self.lib.GetImageFlip(byref(iHFlip), byref(iVFlip)))
        image_flip_state = [iHFlip.value, iVFlip.value]
        return image_flip_state

    def GetImageRotate(self):
        iRotate = c_int()
        check_call(self.lib.GetImageRotate(byref(iRotate)))
        image_rotate_state = iRotate.value
        return image_rotate_state

    def GetMaximumExposure(self):
        MaxExp = c_float()
        check_call(self.lib.GetMaximumExposure(byref(MaxExp)))
        max_exp = MaxExp.value
        return max_exp

    def GetMinimumImageLength(self):
        MinImageLength = c_int()
        check_call(self.lib.GetMinimumImageLength(byref(MinImageLength)))
        min_image_length = MinImageLength.value
        return min_image_length

    def GetNumberADChannels(self):
        channels = c_int()
        check_call(self.lib.GetNumberADChannels(byref(channels)))
        num_adc_channels = channels.value
        return num_adc_channels

    def GetNumberAmp(self):
        amp = c_int()
        check_call(self.lib.GetNumberAmp(byref(amp)))
        num_amps = amp.value
        return num_amps

    def GetNumberHSSpeeds(self, channel, typ):
        speeds = c_int()
        check_call(self.lib.GetNumberHSSpeeds(c_int(channel), c_int(typ),
                                              byref(speeds)))
        num_hss = speeds.value
        return num_hss

    def GetNumberPreAmpGains(self):
        NoGains = c_int()
        check_call(self.lib.GetNumberPreAmpGains(byref(NoGains)))
        num_preamp_gains = NoGains.value
        return num_preamp_gains

    def GetNumberVSAmplitudes(self):
        number = c_int()
        check_call(self.lib.GetNumberVSAmplitudes(byref(number)))
        num_vs_amps = number.value
        return num_vs_amps

    def GetNumberVSSpeeds(self):
        speeds = c_int()
        check_call(self.lib.GetNumberVSSpeeds(byref(speeds)))
        num_vss = speeds.value
        return num_vss

    def GetPixelSize(self):
        xSize = c_float()
        ySize = c_float()
        check_call(self.lib.GetPixelSize(byref(xSize), byref(ySize)))
        pixel_dims = [xSize.value, ySize.value]
        return pixel_dims

    def GetPreAmpGain(self, index):
        gain = c_float()
        check_call(self.lib.GetPreAmpGain(c_int(index), byref(gain)))
        pre_amp_gain = gain.value
        return pre_amp_gain

    def GetSizeOfCircularBuffer(self):
        index = c_long()
        check_call(self.lib.GetSizeOfCircularBuffer(byref(index)))
        circ_buffer_size = index.value
        return circ_buffer_size

    def GetTemperature(self):
        temperature = c_int()
        status = self.lib.GetTemperature(byref(temperature))
        current_temp = [ERROR_STRING[status], temperature.value]
        return current_temp

    def GetTemperatureRange(self):
        mintemp = c_int()
        maxtemp = c_int()
        check_call(self.lib.GetTemperatureRange(byref(mintemp), byref(maxtemp)))
        temp_range = [mintemp.value, maxtemp.value]
        return temp_range

    def GetVSSpeed(self, index):
        speed = c_float()
        check_call(self.lib.GetVSSpeed(c_int(index), byref(speed)))
        vs_speed = speed.value
        return vs_speed

    def Initialize(self):
        status_msg('Initializing Camera')
        pathToDir = c_char()
        status = check_call(self.lib.Initialize(pathToDir))
        return ERROR_STRING[status]

    def saveFits(self):
        self.imageArray = np.reshape(self.imageArray, (self.detector_height,
                                                       self.detector_width))

        date = datetime.today().strftime('%Y%m%d')
        timestamp = f'{date}_{time.strftime("%I")}{time.strftime("%M")}'

        hdul = fits.PrimaryHDU(self.imageArray, uint=True)
        hdul.scale('int16', bzero=32768)
        hdul.header.set("EXPTIME", float(self.exp_time),
                        "Exposure Time in seconds")
        hdul.header.set("ADCHANNEL", self.adc_channel, "A-D Channel")
        hdul.header.set("HSSPEED", self.GetHSSpeed(0, 0, self.hs_index),
                        "HS speed in MHz")
        hdul.header.set("VSSPEED", self.GetVSSpeed(self.vs_index),
                        "VS Speed in microseconds")
        hdul.header.set("TEMP", self.GetTemperature(), "Detector temp in deg C")
        hdul.header.set("INTERFC", "USB", "Instrument Interface")
        hdul.header.set("SNSR_NM", "E2V 2088 x 2048 (CCD 42-40)(B)",
                        "Sensor Name")
        hdul.header.set("SER_NO", self.serial, "Serial Number")
        hdul.header.set("TELESCOP", self.telescope, "Telescope ID")
        hdul.header.set("GAIN", self.GetPreAmpGain(self.pre_amp_gain), "Gain")
        hdul.header.set("INSTRUME", "SEDM-P60", "Camera Name")
        hdul.writeto(f'/home/alex/fits_images/savefits_tests/andortest_{self.pre_amp_gain}{self.vs_index}'
                     f'{self.hs_index}_{timestamp}.fits')

    def SetAcquisitionMode(self, mode):
        status_msg(f'Acquisition Mode Set to [{mode}, {acq_modes[mode]}]')
        self.acquisition_mode = mode
        status = check_call(self.lib.SetAcquisitionMode(c_int(mode)))
        return ERROR_STRING[status]

    def SetADChannel(self, channel):
        status_msg(f'AD Channel Set to [{channel}]')
        self.adc_channel = channel
        status = check_call(self.lib.SetADChannel(c_int(channel)))
        return ERROR_STRING[status]

    def SetBaselineClamp(self, state):
        status_msg(f'Baseline Clamp State Set to [{state}, {baseline_clamp_states[state]}]')
        self.baseline_clamp_state = state
        status = check_call(self.lib.SetBaselineClamp(c_int(state)))
        return ERROR_STRING[status]

    def SetCoolerMode(self, mode):
        status_msg(f'Cooler Mode Set to [{mode}, {cooling_modes[mode]}]')
        self.cooler_mode = mode
        status = check_call(self.lib.SetCoolerMode(c_int(mode)))
        return ERROR_STRING[status]

    def SetCurrentCamera(self, cameraHandle):
        status_msg(f"Setting Active Camera to Handle {cameraHandle}")
        self.cameraHandle = cameraHandle
        status = check_call(self.lib.SetCurrentCamera(c_long(cameraHandle)))
        return ERROR_STRING[status]

    def SetExposureTime(self, ExpTime):
        status_msg(f'Exposure Time Set: {ExpTime} seconds')
        self.exp_time = ExpTime
        status = check_call(self.lib.SetExposureTime(c_float(ExpTime)))
        return ERROR_STRING[status]

    def SetFanMode(self, mode):
        status_msg(f'Fan Mode Set to [{mode}, {fan_modes[mode]}]')
        self.fan_mode = mode
        status = check_call(self.lib.SetFanMode(c_int(mode)))
        return ERROR_STRING[status]

    def SetFrameTransferMode(self, mode):
        status_msg(f'Frame Transfer Mode Set to [{mode}, {frame_transfer_modes[mode]}]')
        self.frame_transfer_mode = mode
        status = check_call(self.lib.SetFrameTransferMode(c_int(mode)))
        return ERROR_STRING[status]

    def SetHSSpeed(self, typ, index):
        status_msg(f"Setting Horizontal Shift Speed Configuration")
        status_msg(f'> Output Amplifier to [{typ}, N/A]')
        status_msg(f'> Horizontal Shift Speed Index to [{index}, {horizontal_shift_speeds[index]}]')
        self.hs_index = index
        status = check_call(self.lib.SetHSSpeed(c_int(typ), c_int(index)))
        return ERROR_STRING[status]

    def SetImage(self, hbin, vbin, hstart, hend, vstart, vend):
        status = check_call(self.lib.SetImage(c_int(hbin), c_int(vbin),
                                              c_int(hstart), c_int(hend),
                                              c_int(vstart), c_int(vend)))
        return ERROR_STRING[status]

    def SetImageFlip(self, iHFlip, iVFlip):
        status_msg(f'Setting Image Flip Configuration')
        status_msg(f'> Horizontal Flip to [{iHFlip}, {image_flip_states[iHFlip]}]')
        status_msg(f'> Vertical Flip to [{iVFlip}, {image_flip_states[iVFlip]}]')
        self.image_flip_states = [iHFlip, iVFlip]
        status = check_call(self.lib.SetImageFlip(c_int(iHFlip), c_int(iVFlip)))
        return ERROR_STRING[status]

    def SetImageRotate(self, iRotate):
        status_msg(f'Image Rotate Set to [{iRotate}, {image_rotation_states[iRotate]}]')
        self.image_rotate_state = iRotate
        status = check_call(self.lib.SetImageRotate(c_int(iRotate)))
        return ERROR_STRING[status]

    def SetKineticCycleTime(self, KinCycTime):
        status_msg(f'Kinetic Cycle Time Set to: {KinCycTime}')
        self.kinetic_cycle_time = float(KinCycTime)
        status = check_call(self.lib.SetKineticCycleTime(c_float(KinCycTime)))
        return ERROR_STRING[status]

    def SetPhotonCounting(self, state):
        status_msg(f'Photon Counting State Set to [{state}, {photon_counting_modes[state]}]')
        self.photon_counting_state = state
        status = check_call(self.lib.SetPhotonCounting(c_int(state)))
        return ERROR_STRING[status]

    def SetPreAmpGain(self, index):
        status_msg(f'Setting Pre Amp Gain to [{index}, {pre_amp_gains[index]}]')
        self.pre_amp_gain = index
        status = check_call(self.lib.SetPreAmpGain(c_int(index)))
        return ERROR_STRING[status]

    def SetReadMode(self, mode):
        status_msg(f'Read Mode Set to [{mode}, {read_modes[mode]}]')
        self.read_mode = mode
        status = check_call(self.lib.SetReadMode(c_int(mode)))
        return ERROR_STRING[status]

    def SetShutter(self, typ, mode, closingtime, openingtime):
        status_msg(f'Setting Shutter Configuration')
        status_msg(f'> Shutter Output TTL to [{typ}, {shutter_ttl_modes[typ]}]')
        status_msg(f'> Shutter Mode to [{mode}, {shutter_modes[mode]}]')
        status_msg(f'> Shutter Closing Time to: {closingtime} ms')
        status_msg(f'> Shutter Opening Time to: {openingtime} ms')
        status = check_call(self.lib.SetShutter(c_int(typ), c_int(mode),
                                                c_int(closingtime),
                                                c_int(openingtime)))
        return ERROR_STRING[status]

    def SetTemperature(self, temperature):
        status = check_call(self.lib.SetTemperature(c_int(temperature)))
        return ERROR_STRING[status]

    def SetVSAmplitude(self, state):
        status_msg(f'Vertical Clock Voltage Amplitude State Set to [{state}, {vs_amplitudes[state]}]')
        self.vertical_clock_voltage_amp = state
        status = check_call(self.lib.SetVSAmplitude(c_int(state)))
        return ERROR_STRING[status]

    def SetVSSpeed(self, index):
        status_msg(f'Vertical Shift Speed Set to [{index}, {vertical_shift_speeds[index]}]')
        self.vs_index = index
        status = check_call(self.lib.SetVSSpeed(c_int(index)))
        return ERROR_STRING[status]

    def ShutDown(self):
        status = check_call(self.lib.ShutDown())
        return ERROR_STRING[status]

    def StartAcquisition(self):
        status = check_call(self.lib.StartAcquisition())
        self.lib.WaitForAcquisition()
        return ERROR_STRING[status]
