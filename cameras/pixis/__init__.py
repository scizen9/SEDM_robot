"""
.. module: drivers/pixis
   :platform: Windows
.. moduleauthor:: Daniel R. Dietze <daniel.dietze@berkeley.edu>

Basic interface to Princeton Instrument's PICam library. It supports most of the
standard features that are provided by PICam. I have decided not to implement a
non-blocking version of the image acquisition in order to keep things clear and
simple.

Here is some example code showing the necessary parameters to get 1 kHz readout
rates on a PIXIS100::

    from pixis import *

    # initialize camera class and connect to library, look for available camera
    # and connect to first one
    cam = pixis()
    cam.loadLibrary()
    cam.getAvailableCameras()
    cam.connect()

    # this will cool down CCD
    cam.setParameter("SensorTemperatureSetPoint", -75)

    # shortest expoure
    cam.setParameter("ExposureTime", 0)

    # readout mode
    cam.setParameter("ReadoutControlMode", PicamReadoutControlMode["FullFrame"])

    # custom chip settings
    cam.setROI(0, 1340, 1, 0, 100, 100)
    cam.setParameter("ActiveWidth", 1340)
    cam.setParameter("ActiveHeight", 100)
    cam.setParameter("ActiveLeftMargin", 0)
    cam.setParameter("ActiveRightMargin", 0)
    cam.setParameter("ActiveTopMargin", 8)
    cam.setParameter("ActiveBottomMargin", 8)
    cam.setParameter("VerticalShiftRate", 3.2)    # select fastest

    # set logic out to not ready
    cam.setParameter("OutputSignal", PicamOutputSignal["Busy"])

    # shutter delays; open before trigger corresponds to
    # shutter opening pre delay
    cam.setParameter("ShutterTimingMode", PicamShutterTimingMode["Normal"])
    cam.setParameter("ShutterClosingDelay", 0)

    # sensor cleaning
    cam.setParameter("CleanSectionFinalHeightCount", 1)
    cam.setParameter("CleanSectionFinalHeight", 100)
    cam.setParameter("CleanSerialRegister", False)
    cam.setParameter("CleanCycleCount", 1)
    cam.setParameter("CleanCycleHeight", 100)
    cam.setParameter("CleanUntilTrigger", True)

    # sensor gain settings
    # according to manual, Pixis supports 100kHz and 2MHz; select fastest
    cam.setParameter("AdcSpeed", 2.0)
    cam.setParameter("AdcAnalogGain", PicamAdcAnalogGain["Low"])
    cam.setParameter("AdcQuality", PicamAdcQuality["HighCapacity"])

    # trigger and timing settings
    cam.setParameter("TriggerDetermination",
                     PicamTriggerDetermination["PositivePolarity"])
    cam.setParameter("TriggerResponse",
                     PicamTriggerResponse["ReadoutPerTrigger"])

    # send configuration
    cam.sendConfiguration()

    # get readout speed
    print "Estimated readout time = %f ms" % cam.getParameter(
        "ReadoutTimeCalculation")

    cam.disconnect()
    cam.unloadLibrary()

..
   This file is part of the pyFSRS app.

   This program is free software: you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation, either version 3 of the License, or
   (at your option) any later version.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program. If not, see <http://www.gnu.org/licenses/>.

   Copyright 2014-2016 Daniel Dietze <daniel.dietze@berkeley.edu>.
"""
import numpy as np
from cameras.pixis.picam_types import *
import sys

# ##############################################################################


# helper functions
def ptr(x):
    """Shortcut to return a ctypes.pointer to object x.
    """
    return ctypes.pointer(x)


# ##############################################################################
# Camera Class
class picam():
    """Main class that handles all connectivity with library and cameras.
    """
    # +++++++++++ CONSTRUCTION / DESTRUCTION +++++++++++++++++++++++++++++++++++
    def __init__(self):
        # empty handle
        self.cam = None
        self.camIDs = None
        self.lib = None
        self.err = None
        self.ROIS = []
        self.roisPtr = []
        self.pulsePtr = []
        self.modPtr = []
        self.acqThread = None
        self.totalFrameSize = 0

    # load pixis.dll and initialize library
    def loadLibrary(self, path_to_lib=""):
        """Loads the pixis library ('Picam.dll') and initializes it.

        :param str path_to_lib: Path to the dynamic link library (optional).
                If empty, the library is loaded using the path given by the
                environment variabel *PicamRoot*, which is normally created by
                the PICam SDK installer.
        :returns: Prints the library version to stdout.
        """
        if path_to_lib is not None:
            pass
        if 'linux' in sys.platform:
            self.lib = ctypes.cdll.LoadLibrary('libpicam.so.0')
            print("Loaded linux library libpicam.so.0")
        else:
            self.lib = ctypes.cdll.LoadLibrary("Picam.dll")
            print("Loaded windows library Picam.dll")
        isconnected = pibln()
        self.status(self.lib.Picam_IsLibraryInitialized(ptr(isconnected)))
        if not isconnected.value:
            self.status(self.lib.Picam_InitializeLibrary())

        self.getLibraryVersion()

    # call this function to release any resources and free the library
    def unloadLibrary(self):
        """Call this function to release any resources and free the library.
        """
        # clean up all reserved memory that may be around
        for i in range(len(self.roisPtr)):
            self.status(self.lib.Picam_DestroyRois(self.roisPtr[i]))
        for i in range(len(self.pulsePtr)):
            self.status(self.lib.Picam_DestroyPulses(self.pulsePtr[i]))
        for i in range(len(self.modPtr)):
            self.status(self.lib.Picam_DestroyModulations(self.modPtr[i]))

        # disconnect from camera
        self.disconnect()

        if isinstance(self.camIDs, list):
            for ci in self.camIDs:
                self.status(self.lib.Picam_DisconnectDemoCamera(ptr(ci)))

        # free cam_id resources
        if self.camIDs is not None and not isinstance(self.camIDs, list):
            self.status(self.lib.Picam_DestroyCameraIDs(self.camIDs))
            self.camIDs = None

        # unload the library
        self.status(self.lib.Picam_UninitializeLibrary())
        print("Unloaded PICamSDK")

    # +++++++++++ CLASS FUNCTIONS ++++++++++++++++++++++++++++++++++++++++++++++
    # get version information
    def getLibraryVersion(self):
        """Returns the PICam library version string.
        """
        major = piint()
        minor = piint()
        distr = piint()
        released = piint()
        self.status(self.lib.Picam_GetVersion(ptr(major),
                                              ptr(minor), ptr(distr),
                                              ptr(released)))
        return "PICam Library Version %d.%d.%d.%d" % (major.value, minor.value,
                                                      distr.value,
                                                      released.value)

    # returns a list of camera IDs that are connected to the computer if no
    # physical camera is found, a demo camera is initialized - for debug only
    def getAvailableCameras(self):
        """Queries a list of IDs of cameras that are connected to the computer
         and prints some sensor information for each camera to stdout.

        If no physical camera is found, a demo camera is initialized -
                        *for debug only*.
        """
        if self.camIDs is not None and not isinstance(self.camIDs, list):
            self.status(self.lib.Picam_DestroyCameraIDs(self.camIDs))
            self.camIDs = None

        # get connected cameras
        self.camIDs = ptr(PicamCameraID())
        # self.firmwareIDS = ptr(PicamFirmwareDetail())
        # print self.firmwareIDS, 'firmware', type(self.firmwareIDS)

        id_count = piint()
        _ = self.status(self.lib.Picam_GetAvailableCameraIDs(ptr(self.camIDs),
                                                             ptr(id_count)))

        # if none are found, create a demo camera
        print("Available Cameras:")
        serial_number_list = []
        if id_count.value < 1:
            print("No cameras found, loading demo camera")
            self.status(self.lib.Picam_DestroyCameraIDs(self.camIDs))

            model_array = ptr(piint())
            model_count = piint()
            self.status(self.lib.Picam_GetAvailableDemoCameraModels(
                ptr(model_array), ptr(model_count))
            )

            model_id = PicamCameraID()
            # print(model_id, 'modelID')
            serial = ctypes.c_char_p(b"Demo Cam 7")
            self.status(self.lib.Picam_ConnectDemoCamera(model_array[53],
                                                         serial, ptr(model_id)))
            self.camIDs = [model_id]

            self.status(self.lib.Picam_DestroyModels(model_array))

            print('  Model is ', PicamModelLookup[model_id.model])
            print('  Computer interface is ', PicamComputerInterfaceLookup[
                model_id.computer_interface])
            print('  Sensor_name is ', model_id.sensor_name)
            print('  Serial number is', model_id.serial_number)
            # print('  Firmware Version', model_id.firmware_name)
            # print('\n')
            serial_number_list.append(model_id.serial_number)
        else:
            for i in range(id_count.value):

                print('  Model is ', PicamModelLookup[self.camIDs[i].model])
                print('  Computer interface is ', PicamComputerInterfaceLookup[
                    self.camIDs[i].computer_interface])
                print('  Sensor_name is ', self.camIDs[i].sensor_name)
                print('  Serial number is', self.camIDs[i].serial_number)
                # print('  Firmware Version', self.camIDs[i].firmware_name)
                # print('\n')
                serial_number_list.append(self.camIDs[i].serial_number)
        return serial_number_list

    # returns string associated with last error
    def getLastError(self):
        """Returns the identifier associated with the last error (*str*).
        """
        return PicamErrorLookup[self.err]

    def status(self, err):
        """Checks the return value of a pixis function for any error code.
           If an error occurred, it prints the error message to stdout.

        :param int err: Error code returned by any pixis function call.
        :returns: Error code (int) and if an error occurred,
                    prints error message.
        """
        errstr = PicamErrorLookup[err]

        if errstr != "None":
            print("ERROR: ", errstr)
        #    raise AssertionError(errstr)
        self.err = err
        return err

    # connect / disconnect camera
    # if no camera ID is given, connect to the first available camera
    # otherwise cam_id is an integer index into a list of valid camera IDs that
    # has been retrieved by getAvailableCameras()
    def connect(self, cam_id=None):
        """ Connect to camera.

        :param int cam_id: Number / index of camera to connect to (optional).
         It is an integer index into a list of valid camera IDs that has been
         retrieved by :py:func:`getAvailableCameras`. If camID is None, this
         functions connects pto the first available camera (default).
        """
        if cam_id is not None:
            pass
        # print(cam_id, type(cam_id), 'cam_id')
        if self.cam is not None:
            self.disconnect()
        if cam_id is None:
            self.cam = pivoid()
            self.status(self.lib.Picam_OpenFirstCamera(ptr(self.cam)))
        else:
            # print("HERE IN THE CAMERA ID")
            self.cam = pivoid()

            self.status(self.lib.Picam_OpenCamera(ptr(self.camIDs[cam_id]),
                                                  ctypes.addressof(self.cam)))
        # invoke commit parameters to validate all parameters for acquisition
        self.sendConfiguration()

    def disconnect(self):
        """Disconnect current camera.
        """
        if self.cam is not None:
            self.status(self.lib.Picam_CloseCamera(self.cam))
        self.cam = None

    def getCurrentCameraID(self):
        """Returns the current camera ID (:py:class:`PicamCameraID`).
        """
        id = PicamCameraID()
        self.status(self.lib.Picam_GetCameraID(self.cam, ptr(id)))

        return id

    # prints a list of parameters that are available
    def printAvailableParameters(self):
        """Prints an overview over the parameters to stdout that are available
           for the current camera and their limits.
        """
        parameter_array = ptr(piint())
        parameter_count = piint()
        self.lib.Picam_GetParameters(self.cam, ptr(parameter_array),
                                     ptr(parameter_count))

        for i in range(parameter_count.value):

            # read / write access
            access = piint()
            self.lib.Picam_GetParameterValueAccess(self.cam, parameter_array[i],
                                                   ptr(access))
            readable = PicamValueAccessLookup[access.value]

            # constraints
            contype = piint()
            self.lib.Picam_GetParameterConstraintType(self.cam,
                                                      parameter_array[i],
                                                      ptr(contype))

            constraint = ""
            if PicamConstraintTypeLookup[contype.value] == "None":
                constraint = "ALL"

            elif PicamConstraintTypeLookup[contype.value] == "Range":

                rc = ptr(PicamRangeConstraint())
                self.lib.Picam_GetParameterRangeConstraint(
                    self.cam, parameter_array[i],
                    PicamConstraintCategory['Capable'], ptr(rc)
                )

                constraint = "from %f to %f in steps of %f" % (rc[0].minimum,
                                                               rc[0].maximum,
                                                               rc[0].increment)

                self.lib.Picam_DestroyRangeConstraints(rc)

            elif PicamConstraintTypeLookup[contype.value] == "Collection":

                cc = ptr(PicamCollectionConstraint())
                self.lib.Picam_GetParameterCollectionConstraint(
                    self.cam, parameter_array[i],
                    PicamConstraintCategory['Capable'], ptr(cc)
                )

                constraint = ""
                for j in range(cc[0].values_count):
                    if constraint != "":
                        constraint += ", "
                    constraint += str(cc[0].values_array[j])

                self.lib.Picam_DestroyCollectionConstraints(cc)

            elif PicamConstraintTypeLookup[contype.value] == "Rois":
                constraint = "N.A."
            elif PicamConstraintTypeLookup[contype.value] == "Pulse":
                constraint = "N.A."
            elif PicamConstraintTypeLookup[contype.value] == "Modulations":
                constraint = "N.A."

            # print info
            print(PicamParameterLookup[parameter_array[i]])
            print(" value access:", readable)
            print(" allowed values:", constraint)
            print("\n")

        self.lib.Picam_DestroyParameters(parameter_array)

    def getAvailableParameters(self):
        """Prints an overview over the parameters to stdout that are available
           for the current camera and their limits.
        """
        parameter_array = ptr(piint())
        parameter_count = piint()
        self.lib.Picam_GetParameters(self.cam, ptr(parameter_array),
                                     ptr(parameter_count))
        param_list = []
        param_dict = {}
        for i in range(parameter_count.value):

            # read / write access
            access = piint()
            self.lib.Picam_GetParameterValueAccess(self.cam, parameter_array[i],
                                                   ptr(access))
            readable = PicamValueAccessLookup[access.value]

            # constraints
            contype = piint()
            self.lib.Picam_GetParameterConstraintType(self.cam,
                                                      parameter_array[i],
                                                      ptr(contype))

            constraint = ""
            if PicamConstraintTypeLookup[contype.value] == "None":
                constraint = "ALL"

            elif PicamConstraintTypeLookup[contype.value] == "Range":

                rc = ptr(PicamRangeConstraint())
                self.lib.Picam_GetParameterRangeConstraint(
                    self.cam, parameter_array[i],
                    PicamConstraintCategory['Capable'], ptr(rc)
                )

                constraint = "from %f to %f in steps of %f" % (rc[0].minimum,
                                                               rc[0].maximum,
                                                               rc[0].increment)

                self.lib.Picam_DestroyRangeConstraints(rc)

            elif PicamConstraintTypeLookup[contype.value] == "Collection":

                cc = ptr(PicamCollectionConstraint())
                self.lib.Picam_GetParameterCollectionConstraint(
                    self.cam, parameter_array[i],
                    PicamConstraintCategory['Capable'], ptr(cc)
                )

                constraint = ""
                for j in range(cc[0].values_count):
                    if constraint != "":
                        constraint += ", "
                    constraint += str(cc[0].values_array[j])

                self.lib.Picam_DestroyCollectionConstraints(cc)

            elif PicamConstraintTypeLookup[contype.value] == "Rois":
                constraint = "N.A."
            elif PicamConstraintTypeLookup[contype.value] == "Pulse":
                constraint = "N.A."
            elif PicamConstraintTypeLookup[contype.value] == "Modulations":
                constraint = "N.A."

            # print info

            try:
                param_list.append(PicamParameterLookup[parameter_array[i]])
                param_dict[PicamParameterLookup[parameter_array[i]]] = [
                    readable, constraint]
                print(PicamParameterLookup[parameter_array[i]])
                print(" value access:", readable)
                print(" allowed values:", constraint)

            except Exception as e:
                print(str(e), "Paramerr")
                pass
        self.lib.Picam_DestroyParameters(parameter_array)
        return param_list, param_dict

    # get / set parameters
    # name is a string specifying the parameter
    def getParameter(self, name):
        """Reads and returns the value of the parameter with given name. If
         there is no parameter of this name, the function returns None and
         prints a warning.

        :param str name: Name of the parameter exactly as stated in the
                PICam SDK manual.
        :returns: Value of this parameter with data type corresponding to the
                type of parameter.
        """
        prm = PicamParameter[name]

        exists = pibln()
        self.lib.Picam_DoesParameterExist(self.cam, prm, ptr(exists))
        if not exists.value:
            print("Ignoring parameter", name)
            print("  Parameter does not exist for current camera!")
            return

        # get type of parameter
        ptype = piint()
        self.lib.Picam_GetParameterValueType(self.cam, prm, ptr(ptype))

        if ptype.value not in PicamValueTypeLookup:
            print("Not a valid parameter type enumeration:", ptype.value)
            print("Ignoring parameter", name)
            return 0

        if PicamValueTypeLookup[ptype.value] in ["Integer", "Boolean",
                                                 "Enumeration"]:
            val = piint()

            # test whether we can read the value directly from hardware
            cr = pibln()
            self.lib.Picam_CanReadParameter(self.cam, prm, ptr(cr))
            if cr.value:
                if self.lib.Picam_ReadParameterIntegerValue(self.cam, prm,
                                                            ptr(val)) == 0:
                    return val.value
            else:
                if self.lib.Picam_GetParameterIntegerValue(self.cam, prm,
                                                           ptr(val)) == 0:
                    return val.value

        if PicamValueTypeLookup[ptype.value] == "LargeInteger":
            val = pi64s()
            if self.lib.Picam_GetParameterLargeIntegerValue(self.cam, prm,
                                                            ptr(val)) == 0:
                return val.value

        if PicamValueTypeLookup[ptype.value] == "FloatingPoint":
            val = piflt()

            # NEW
            # test whether we can read the value directly from hardware
            cr = pibln()
            self.lib.Picam_CanReadParameter(self.cam, prm, ptr(cr))
            if cr.value:
                if self.lib.Picam_ReadParameterFloatingPointValue(
                        self.cam, prm, ptr(val)) == 0:
                    return val.value
            else:
                if self.lib.Picam_GetParameterFloatingPointValue(
                        self.cam, prm, ptr(val)) == 0:
                    return val.value

        if PicamValueTypeLookup[ptype.value] == "Rois":
            val = ptr(PicamRois())
            if self.lib.Picam_GetParameterRoisValue(
                    self.cam, prm, ptr(val)) == 0:
                self.roisPtr.append(val)
                return val.contents

        if PicamValueTypeLookup[ptype.value] == "Pulse":
            val = ptr(PicamPulse())
            if self.lib.Picam_GetParameterPulseValue(
                    self.cam, prm, ptr(val)) == 0:
                self.pulsePtr.append(val)
                return val.contents

        if PicamValueTypeLookup[ptype.value] == "Modulations":
            val = ptr(PicamModulations())
            if self.lib.Picam_GetParameterModulationsValue(
                    self.cam, prm, ptr(val)) == 0:
                self.modPtr.append(val)
                return val.contents

        return None

    def setParameter(self, name, value):
        """Set parameter. The value is automatically typecast to the correct
           data type corresponding to the type of parameter.

        .. note:: Setting a parameter with this function does not automatically
         change the configuration in the camera. In order to apply all changes,
         :py:func:`sendConfiguration` has to be called.

        :param str name: Name of the parameter exactly as stated in the
                PICam SDK manual.
        :param mixed value: New parameter value. If the parameter value cannot
                be changed, a warning is printed to stdout.
        """
        prm = PicamParameter[name]

        exists = pibln()
        self.lib.Picam_DoesParameterExist(self.cam, prm, ptr(exists))
        if not exists:
            print("Ignoring parameter", name)
            print("  Parameter does not exist for current camera!")
            return

        access = piint()
        self.lib.Picam_GetParameterValueAccess(self.cam, prm, ptr(access))
        if PicamValueAccessLookup[access.value] not in ["ReadWrite",
                                                        "ReadWriteTrivial"]:
            print("Ignoring parameter", name)
            print("  Not allowed to overwrite parameter!")
            return
        if PicamValueAccessLookup[access.value] == "ReadWriteTrivial":
            print("WARNING: Parameter", name, " allows only one value!")

        # get type of parameter
        ptype = piint()
        self.lib.Picam_GetParameterValueType(self.cam, prm, ptr(ptype))

        if ptype.value not in PicamValueTypeLookup:
            print("Ignoring parameter", name)
            print("  Not a valid parameter type:", ptype.value)
            return

        if PicamValueTypeLookup[ptype.value] in ["Integer", "Boolean",
                                                 "Enumeration"]:
            val = piint(value)
            self.status(self.lib.Picam_SetParameterIntegerValue(self.cam, prm,
                                                                val))

        if PicamValueTypeLookup[ptype.value] == "LargeInteger":
            val = pi64s(value)
            self.status(self.lib.Picam_SetParameterLargeIntegerValue(self.cam,
                                                                     prm, val))

        if PicamValueTypeLookup[ptype.value] == "FloatingPoint":
            val = piflt(value)
            self.status(self.lib.Picam_SetParameterFloatingPointValue(self.cam,
                                                                      prm, val))

        if PicamValueTypeLookup[ptype.value] == "Rois":
            self.status(self.lib.Picam_SetParameterRoisValue(self.cam, prm,
                                                             ptr(value)))

        if PicamValueTypeLookup[ptype.value] == "Pulse":
            self.status(self.lib.Picam_SetParameterPulseValue(self.cam, prm,
                                                              ptr(value)))

        if PicamValueTypeLookup[ptype.value] == "Modulations":
            self.status(self.lib.Picam_SetParameterModulationsValue(self.cam,
                                                                    prm,
                                                                    ptr(value)))

        if self.err != PicamError["None"]:
            print("Ignoring parameter", name)
            print("  Could not change parameter. Keeping previous value:",
                  self.getParameter(name))

    # this function has to be called once all configurations
    # are done to apply settings to the camera
    def sendConfiguration(self):
        """This function has to be called once all configurations are done to
           apply settings to the camera.
        """
        failed = ptr(piint())
        failed_count = piint()

        self.status(self.lib.Picam_CommitParameters(self.cam, ptr(failed),
                                                    ptr(failed_count)))

        if failed_count.value > 0:
            for i in range(failed_count.value):
                print("Could not set parameter",
                      PicamParameterLookup[failed[i]])
        self.status(self.lib.Picam_DestroyParameters(failed))

        self.updateROIS()

    # utility function that extracts the number of pixel sizes of all ROIs
    def updateROIS(self):
        """Internally used utility function to extract a list of pixel sizes
            of ROIs.
        """
        self.ROIS = []
        rois = self.getParameter("Rois")
        self.totalFrameSize = 0
        offs = 0
        for i in range(rois.roi_count):
            w = int(np.ceil(float(rois.roi_array[i].width) / float(
                rois.roi_array[i].x_binning)))
            h = int(np.ceil(float(rois.roi_array[i].height) / float(
                rois.roi_array[i].y_binning)))
            self.ROIS.append((w, h, offs))
            offs = offs + w * h
        self.totalFrameSize = offs

    # set a single ROI
    def setROI(self, x0, w, xbin, y0, h, ybin):
        """Create a single region of interest (ROI).

        :param int x0: X-coordinate of upper left corner of ROI.
        :param int w: Width of ROI.
        :param int xbin: X-Binning, i.e. number of columns that are combined
                    into one larger column (1 to w).
        :param int y0: Y-coordinate of upper left corner of ROI.
        :param int h: Height of ROI.
        :param int ybin: Y-Binning, i.e. number of rows that are combined
                    into one larger row (1 to h).
        """
        r = PicamRoi(x0, w, xbin, y0, h, ybin)
        r1 = PicamRois(ptr(r), 1)
        self.setParameter("Rois", r1)
        self.updateROIS()

    # add a ROI
    def addROI(self, x0, w, xbin, y0, h, ybin):
        """Add a region-of-interest to the existing list of ROIs.

        .. important:: The ROIs should not overlap! However, this function does
            not check for overlapping ROIs!

        :param int x0: X-coordinate of upper left corner of ROI.
        :param int w: Width of ROI.
        :param int xbin: X-Binning, i.e. number of columns that are combined
                    into one larger column (1 to w).
        :param int y0: Y-coordinate of upper left corner of ROI.
        :param int h: Height of ROI.
        :param int ybin: Y-Binning, i.e. number of rows that are combined
                    into one larger row (1 to h).
        """
        # read existing rois
        r = self.getParameter("Rois")
        r0 = (PicamRoi * (r.roi_count + 1))()
        for i in range(r.roi_count):
            r0[i] = r.roi_array[i]
        # add new roi
        r0[-1] = PicamRoi(x0, w, xbin, y0, h, ybin)
        # write back to camera
        r1 = PicamRois(ptr(r0[0]), len(r0))
        self.setParameter("Rois", r1)
        self.updateROIS()

    # acquisition functions
    # readNFrames waits till all frames have been collected
    #       (using Picam_Acquire)
    # N = number of frames
    # timeout = max wait time between frames in ms
    def readNFrames(self, N=1, timeout=9000):
        """This function acquires N frames using Picam_Acquire. It waits till
            all frames have been collected before it returns.

        :param int N: Number of frames to collect (>= 1, default=1). This number
                            is essentially limited by the available memory.
        :param int timeout: Maximum wait time between frames in milliseconds
                            (default=100). This parameter is important when
                             using external triggering.
        :returns: List of acquired frames.
        """
        available = PicamAvailableData()
        errors = piint()

        running = pibln()
        N=1
        self.lib.Picam_IsAcquisitionRunning(self.cam, ptr(running))
        if running.value:
            print("ERROR: acquisition still running")
            return "ERROR: acquisition still running"

        # start acquisition
        self.status(self.lib.Picam_Acquire(self.cam, pi64s(N), piint(timeout),
                                           ptr(available), ptr(errors)))

        # return data as numpy array
        if available.readout_count >= N:
            if len(self.ROIS) == 1:
                return self.getBuffer(available.initial_readout,
                                      available.readout_count)[0:N]
            else:
                return self.getBuffer(available.initial_readout,
                                      available.readout_count)[:][0:N]
        return []

    # this is a helper function that converts a readout buffer into
    # a sequence of numpy arrays. it reads all available data at once into a
    # numpy buffer and reformats data to fit to the output mask
    # size is number of readouts to read
    # returns data as floating point
    def getBuffer(self, address, size):
        """This is an internally used function to convert the readout buffer
         into a sequence of numpy arrays. It reads all available data at once
         into a numpy buffer and reformats data to a usable format.

        :param long address: Memory address where the readout buffer is stored.
        :param int size: Number of readouts available in the readout buffer.
        :returns: List of ROIS; for each ROI, array of readouts; each readout is
                    a NxM array.
        """
        # get number of pixels contained in a single readout and a single frame
        # parameters are bytes, a pixel in resulting array is 2 bytes
        readoutstride = int(self.getParameter("ReadoutStride") / 2)
        framestride = int(self.getParameter("FrameStride") / 2)
        frames = self.getParameter("FramesPerReadout")

        # create a pointer to data
        data_array_type = pi16u * readoutstride * size
        data_array_pointer_type = ctypes.POINTER(data_array_type)
        data_pointer = ctypes.cast(address, data_array_pointer_type)

        # create a numpy array from the buffer
        fdata = np.frombuffer(data_pointer.contents, dtype='uint16')

        # cast it into a usable format - [frames][data]
        fdata = (
            (
                fdata.reshape(size, readoutstride)[:, :frames * framestride]
            ).reshape(size, frames, framestride)[:, :, :self.totalFrameSize]
        ).reshape(size * frames, self.totalFrameSize).astype(float)

        # if there is just a single ROI, we are done
        if len(self.ROIS) == 1:
            return [fdata.reshape(size * frames, self.ROIS[0][0],
                                  self.ROIS[0][1])]

        # otherwise, iterate through rois and add to output list (has to be
        # list due to possibly different sizes)
        out = []
        for i, r in enumerate(self.ROIS):
            out.append(fdata[:, r[2]:r[0] * r[1] + r[2]])
        return out


if __name__ == '__main__':
    from astropy.io import fits
    cam = picam()
    cam.loadLibrary()
    cam.getAvailableCameras()
    cam.connect()

    # cool down CCD
    cam.setParameter("SensorTemperatureSetPoint", 15)

    # shortest expoure
    cam.setParameter("ExposureTime", 0)

    # readout mode
    cam.setParameter("ReadoutControlMode", PicamReadoutControlMode["FullFrame"])

    # custom chip settings

    cam.setParameter("ActiveWidth", 2048)
    cam.setParameter("ActiveHeight", 2048)
    cam.setParameter("ActiveLeftMargin", 54+16)
    cam.setParameter("ActiveRightMargin", 50)
    cam.setParameter("ActiveTopMargin", 7)
    cam.setParameter("ActiveBottomMargin", 3)

    # set logic out to not ready
    cam.setParameter("OutputSignal", PicamOutputSignal["Busy"])

    # shutter delays; open before trigger corresponds to
    # shutter opening pre delay
    cam.setParameter("ShutterTimingMode", PicamShutterTimingMode["Normal"])
    cam.setParameter("ShutterClosingDelay", 0)

    # sensor cleaning
    cam.setParameter("CleanSectionFinalHeightCount", 1)
    cam.setParameter("CleanSectionFinalHeight", 100)
    cam.setParameter("CleanSerialRegister", False)
    cam.setParameter("CleanCycleCount", 1)
    cam.setParameter("CleanCycleHeight", 100)
    cam.setParameter("CleanUntilTrigger", True)

    # sensor gain settings
    # according to manual, Pixis supports 100kHz and 2MHz; select fastest
    cam.setParameter("AdcSpeed", 2.0)
    cam.setParameter("AdcAnalogGain", PicamAdcAnalogGain["Low"])
    cam.setParameter("AdcQuality", PicamAdcQuality["HighCapacity"])

    # trigger and timing settings
    cam.setParameter("TriggerDetermination",
                     PicamTriggerDetermination["PositivePolarity"])
    cam.setParameter("TriggerResponse",
                     PicamTriggerResponse["ReadoutPerTrigger"])

    # send configuration
    cam.sendConfiguration()

    # get readout speed
    print("Estimated readout time = %f ms" %
          cam.getParameter("ReadoutTimeCalculation"))
    
    data = cam.readNFrames(N=1, timeout=10000)
    print(data)
    print(data[0])
    print(data)
    hdu = fits.PrimaryHDU(data, uint=False)
    hdu.scale('int16', bzero=32768)
    hdu.header.set("GAIN_SET", 2, "Gain mode")
    hdu.header.set("ADC", 1, "ADC Quality")
    hdu.header.set("MODEL", 22, "Instrument Mode Number")
    hdu.header.set("INTERFC", "USB", "Instrument Interface")
    hdu.header.set("SNSR_NM", "E2V 2048 x 2048 (CCD 42-40)(B)", "Sensor Name")

    hdu.writeto('test3.fits', output_verify="fix", )
    cam.disconnect()
    cam.unloadLibrary()
