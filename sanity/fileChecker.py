import glob
import os
import time
from astropy.io import fits


class Checker:
    def __init__(self, data_dir='/home/sedm/images/'):
        self.data_dir = data_dir

    def check_for_images(self, camera, keywords, time_cut=None, data_dir=None):
        """

        :param camera:
        :param keywords:
        :param time_cut:
        :param data_dir:
        :return:
        """
        if time_cut:
            pass
        start = time.time()
        img_list = []
        files = []
        if not data_dir:
            data_dir = self.data_dir

        # 1. Get the images
        if isinstance(data_dir, list):
            for i in data_dir:
                path = os.path.join(i, camera+"*.fits")
                print("Checking %s" % path)
                files += glob.glob(path)
        else:
            path = os.path.join(data_dir, camera + "*.fits")
            print("Checking %s" % path)
            files += glob.glob(path)

        if not isinstance(keywords, dict):
            return {'elaptime': time.time()-start,
                    'error': "keywords are not in dict form"}

        nfiles = 0
        for f in files:
            key_there = 0
            n_keys = 0
            header = fits.getheader(f)
            for k, v in keywords.items():
                n_keys += 1
                if k.upper() in header:
                    if isinstance(v, str):
                        if v.lower() in header[k.upper()]:
                            key_there += 1
                    elif isinstance(v, float) or isinstance(v, int):
                        if v == header[k.upper()]:
                            key_there += 1
            if key_there == n_keys:
                img_list.append(f)
                nfiles += 1

        return {'elaptime': time.time()-start, 'data': nfiles}


if __name__ == "__main__":
    x = Checker()
    ret = x.check_for_images('ifu', keywords={'object': 'bias',
                                              'ADCSPEED': 2.0},
                             data_dir='/home/sedm/images/20191125')
    print(len(ret['data']))
