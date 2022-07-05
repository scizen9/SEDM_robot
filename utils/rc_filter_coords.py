from astropy.coordinates import SkyCoord, Angle
import astropy.units as u
import numpy as np
import os
import time
import json
import SEDM_robot_version as Version

with open(os.path.join(Version.CONFIG_DIR, 'sedm_robot.json')) as data_file:
    sedm_robot_cfg = json.load(data_file)


def offsets(ra, dec, offset_dict=None):
    """
    Get a filter offset dictionary
    :param ra:
    :param dec:
    :param offset_dict:
    :return:
    """
    start = time.time()
    offset_pos = {}
    try:
        if not offset_dict:
            offset_dict = sedm_robot_cfg['rc']['offsets']

        obj = SkyCoord(ra=ra, dec=dec, unit=(u.deg, u.deg))
        for flt, v in offset_dict['offset_filter'].items():

            offra = (Angle(v['ra'], unit=u.arcsec) /
                     np.cos(obj.dec.to('radian')))
            offdec = Angle(v['dec'], unit=u.arcsec)

            new_pos = SkyCoord(obj.ra + offra, obj.dec + offdec, frame='icrs')
            offset_pos[flt] = {'ra': round(new_pos.ra.value, 6),
                               'dec': round(new_pos.dec.value, 6)}

        return {'elaptime': time.time()-start, 'data': offset_pos}
    except Exception as e:
        return {'elaptime': time.time()-start, 'error': str(e)}


if __name__ == "__main__":
    print(offsets(185.729, 15.824))
