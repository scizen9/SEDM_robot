import socket
import os
import sys
from observatory.telescope import tcs
import paramiko
import time
import json
import SEDM_robot_version as Version

with open(os.path.join(Version.CONFIG_DIR, 'watcher.json')) as data_file:
    params = json.load(data_file)

telescope = tcs.Telescope(gxnaddress=(params['gxn_ip'], params['gxn_port']),
                          logfname='watcher_tcs.log')

with open(os.path.join(Version.CONFIG_DIR, 'sedm_robot.json')) as data_file:
    sedm_cfg = json.load(data_file)


def sftp_connection(remote_computer=params['remote_computer'],
                    user=params['remote_user'],
                    pwd=params['remote_pwd'],
                    remote_port=params['remote_port']):
    """
    Create a sftp connection to send files.  If a user is not
    listed then we assume the connection to be known and look
    in the ~/.pwd directory for files.

    :param remote_computer: string ip
    :param user: string user name
    :param pwd: string password
    :param remote_port: string remote port
    :return: sftp connection
    """

    sftp = None
    transport = None
    try:
        transport = paramiko.Transport((remote_computer, remote_port))
        transport.connect(username=user, password=pwd.rstrip())
        sftp = paramiko.SFTPClient.from_transport(transport)
    except paramiko.ssh_exception.SSHException:
        print("Error negotiating SSH2 protocol")
        pass
    return sftp, transport


def put_remote_file(remote_path=None, local_path=params['local_path'],
                    remote_computer=params['remote_computer'],
                    replace_path_str="s:"):
    """
    Using the paramiko script transfer a local file to a remote destination

    :param local_path: str path of local file
    :param remote_computer: string ip of remote computer
    :param remote_path: str path of remote directory.  If just a directory
                        the file keeps the local_path file
    :param replace_path_str: str for converting from windows path
    :return: bool, status message
    """

    try:
        sftp, transport = sftp_connection(remote_computer)
    except socket.gaierror:
        print("Socket error, not tranferred!")
        return

    # Remove windows path string in order to use sftp function
    remote_path = remote_path.replace(replace_path_str, "")
    print(local_path)
    print(remote_path)
    if sftp is not None and transport is not None:
        sftp.put(local_path, remote_path)

        sftp.close()
        transport.close()
    else:
        print("Not transferred!")


def connect(connect_ifu=True, connect_rc=True):
    if connect_ifu:
        cifu = socket.socket()
        cifu.connect((sedm_cfg['ifu_ip'], sedm_cfg['ifu_port']))
    else:
        return None
    if connect_rc:
        crc = socket.socket()
        crc.connect((sedm_cfg['rc_ip'], sedm_cfg['rc_port']))
    else:
        return None

    return cifu, crc


def get_camera_info2(conn):  # , cam_string='ifu'):
    """
    """
    conn.send('STATUS')
    time.sleep(.1)
    data = conn.recv(1024)
    print(data)

    # conn.send(b'GETLASTSTART')
    # data2 = time.sleep(.05)
    # print(data2)


def get_camera_info(conn, cam_string='ifu'):
    """

    :param conn:
    :param cam_string:
    :return:
    """
    info_dict = {}
    send_dict = json.dumps({'command': 'STATUS'})

    conn.send(b"%s" % send_dict.encode('utf-8'))
    time.sleep(.05)
    ret = conn.recv(1024)

    try:
        cam_dict = json.loads(ret.decode('utf-8'))
        if 'ifu' in cam_string:
            expt = "%.1f" % cam_dict['camexptime']
        else:
            expt = "%.1f" % (cam_dict['camexptime'] / 1000)
        info_dict['%s_ExposureTime' % cam_string] = expt
        info_dict['%s_Temperature' % cam_string] = str(cam_dict['camtemp'])
        info_dict['%s_SetPoint' % cam_string] = str(cam_dict['state'])
    except Exception as ex:
        print(str(ex))

    send_dict = json.dumps({'command': 'LASTEXPOSED'})
    conn.send(b"%s" % send_dict.encode('utf-8'))
    time.sleep(.05)
    ret = conn.recv(1024)
    try:

        exp_dict = json.loads(ret.decode('utf-8'))
        if 'data' in exp_dict:
            print(exp_dict)
            ob_time = exp_dict['data']
        else:
            ob_time = 'unkown'
        info_dict['%s_LastStartTime' % cam_string] = ob_time
    except Exception as ex:
        print(str(ex))
        print("Time error")

    if not cam_string == 'ifu':
        return info_dict

    """send_dict = json.dumps({'command': 'GETPRESSURE'})
    conn.send(b"%s" % send_dict.encode('utf-8'))
    time.sleep(.05)
    ret = conn.recv(1024)
    try:

        exp_dict = json.loads(ret.decode('utf-8'))
        if 'data' in exp_dict:
            print(exp_dict)
            ob_time = exp_dict['data']
        else:
            ob_time = 'unkown'
        info_dict['chiller_rate'] = ob_time
    except Exception as e:
        print(str(e))
        print("Time error")
    """
    return info_dict


if len(sys.argv) > 1:
    use_winter = True
    from observatory.telescope import winter
    win = winter.Winter()
else:
    use_winter = False
    win = None

try:
    ifu, rc = connect()
except Exception as e:
    print(str(e))
    ifu = False,
    rc = False
    pass

while True:
    # 1. Start by getting information
    status_dict = {}
    try:
        pos = telescope.get_pos()
        status = telescope.get_status()
        weather = telescope.get_weather()
        faults = telescope.get_faults()
        if use_winter:
            wret = win.get_weather()
        else:
            wret = None

        print(faults)
        print(type(pos))
        print(weather)

        if 'data' in pos:
            status_dict.update(pos['data'])
        if 'data' in status:
            status_dict.update(status['data'])
        if 'data' in weather:
            status_dict.update(weather['data'])
        if 'data' in faults:
            f = faults['data']
            f = f.split(':')
            if len(f) == 2:
                flist = f[1].rstrip().lstrip()
                fdict = {'faults': flist.replace('\n', '<br>')}
            else:
                fdict = {'faults': 'None'}
            status_dict.update(fdict)
        if use_winter and 'data' in wret:
            win_dict = wret['data']
            status_dict[
                'windspeed_average'] = str(win_dict['Average_Wind_Speed'])
            status_dict['wind_dir_current'] = str(win_dict['Wind_Direction'])
            status_dict['outside_air_temp'] = str(win_dict['Outside_Temp'])
            status_dict['outside_rel_hum'] = str(win_dict['Outside_RH'])
            status_dict['outside_dewpt'] = str(win_dict['Outside_Dewpoint'])

        print(type(status_dict), 'status_dict')

        if 'utc' in status_dict:
            status_dict['utc2'] = status_dict['utc'][9:]
        if 'telescope_ra' in status_dict:
            s = list(status_dict['telescope_ra'])
            s[3:] = '??'

            status_dict['telescope_ra'] = ''.join(s)
        if 'telescope_dec' in status_dict:
            s = list(status_dict['telescope_dec'])
            s[4:] = '??'
            status_dict['telescope_dec'] = ''.join(s)
        if 'dec_axis_hard_limit_status' in status_dict and \
                'dec_axis_soft_limit_status' in status_dict:
            status_dict['dec_limit_status'] = \
                status_dict['dec_axis_hard_limit_status'] + '<br>' + \
                status_dict['dec_axis_soft_limit_status']

        if 'ha_axis_hard_limit_status' in status_dict and \
                'ha_axis_soft_limit_status' in status_dict:
            status_dict['ha_limit_status'] = \
                status_dict['ha_axis_hard_limit_status'] + '<br>' + \
                status_dict['ha_axis_soft_limit_status']
    except Exception as e:
        print(str(e), 'error in getting a value')
        time.sleep(1)

    try:
        if not ifu or not rc:
            ifu, rc = connect()

        status_dict['ifu_cameraTime'] = time.strftime('%H:%M:%S', time.gmtime())
        status_dict.update(get_camera_info(ifu, 'ifu'))
        status_dict['rc_cameraTime'] = time.strftime('%H:%M:%S', time.gmtime())
        status_dict.update(get_camera_info(rc, 'rc'))

        jsonstr = json.dumps(status_dict)
        print(jsonstr)
        f = open(params['local_path'], "w")
        f.write(jsonstr)
        f.close()

        put_remote_file(local_path=params['local_path'],
                        remote_path=params['remote_path'],
                        remote_computer=params['remote_computer'])
        time.sleep(5)
    except Exception as e:
        print(str(e))
        if '32' in str(e):
            ifu, rc = connect()

        jsonstr = json.dumps(status_dict)
        f = open(params['local_path'], "w")
        f.write(jsonstr)
        f.close()

        try:
            put_remote_file(local_path=params['local_path'],
                            remote_path=params['remote_path'],
                            remote_computer=params['remote_computer'])
        except ConnectionResetError as err:
            print(str(err))
            pass
        pass
        time.sleep(5)

# telescope.close_connection()
