import numpy as np
import os

SITE_ROOT = os.path.abspath(os.path.dirname(__file__)+'/..')


def temp_to_focus(current_temp):
    focustable = np.genfromtxt(
        os.path.join(SITE_ROOT, 'utils', 'focus_vs_temp.csv'),
        names=['temp', 'focus'], dtype=[float, float], delimiter=',',
        skip_header=True)
    index = np.where(focustable['temp'] == round(current_temp, 1))
    return focustable['focus'][index][0]
