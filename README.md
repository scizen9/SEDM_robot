# SEDM_robot
Spectral Energy Distribution Machine robotic observation software

In the current configuration the robotic observation software is run on nemea.palomar.caltech.edu
and the camera servers are run on pylos.

Several servers are run on nemea from the root directory in the following order:
1. sanity/server/sanity_server.py
2. observatory/server/ocs_server.py
3. sky/server/sky_server.py
4. sedm_observe.py
5. sanity/webwatcher/watcher.py

The two camera servers are run on pylos in the following order:
1. cameras/server/rc_cam_server.py
2. cameras/server/ifu_cam_server.py

This repository also has the code to run the Andor camera as the IFU science camera.  We are
still developing the procedures for running this, but it will eventually replace the IFU
camera command above.
