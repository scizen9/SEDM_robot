from sedm_robot import SEDm
from utils import obstimes, rc_focus
import datetime
import time
from astropy.time import Time
import os
import glob
import json
import traceback
import argparse
import SEDM_robot_version as Version

with open(os.path.join(Version.CONFIG_DIR, 'sedm_observe.json')) as cfg_file:
    sedm_observe_cfg = json.load(cfg_file)

status_file_dir = sedm_observe_cfg['status_dir']
manual_dir = sedm_observe_cfg['manual_dir']

calib_done_file = os.path.join(os.path.join(status_file_dir, "calib_done.txt"))
focus_done_file = os.path.join(os.path.join(status_file_dir, "focus_done.json"))
twilights_done_file = os.path.join(os.path.join(status_file_dir,
                                                "twilights_done.txt"))
standard_done_file = os.path.join(os.path.join(status_file_dir,
                                               "standard_done.txt"))

status_files = [calib_done_file, focus_done_file, standard_done_file,
                twilights_done_file]


def uttime(offset=0):
    if not offset:
        return Time(datetime.datetime.utcnow())
    else:
        return Time(datetime.datetime.utcnow() +
                    datetime.timedelta(seconds=offset))


def clean_up():
    print("Cleaning up")
    for ff in status_files:
        if os.path.exists(ff):
            os.remove(ff)


def run_observing_loop(do_focus=True, do_standard=True,
                       do_calib=True, do_twilights=True,
                       lamps_off=False, clean_manual=True,
                       temperature=None, use_winter=False):

    print("\nReSTARTING OBSERVING LOOP at ", datetime.datetime.utcnow())
    print("SEDM_robot version:", Version.__version__, "\n")

    if do_focus:
        pass
    if do_standard:
        pass

    if os.path.exists(focus_done_file):
        focus_done = True
        # open file and read temperature and position
        with open(focus_done_file) as data_file:
            focus_data = json.load(data_file)
    else:
        focus_done = False
        focus_data = None

    if temperature:
        print("Using estimated temperature of %.2f C for initial focus"
              % temperature)
        focus_temp = temperature
        focus_guess = True
    else:
        print("Using weather station temperature for initial focus")
        focus_temp = None
        focus_guess = False

    if os.path.exists(standard_done_file):
        standard_done = True
    else:
        standard_done = False

    if os.path.exists(calib_done_file):
        calib_done = True
    else:
        calib_done = False

    if os.path.exists(twilights_done_file):
        twilights_done = True
    else:
        twilights_done = False

    for i in status_files:
        print(i, os.path.exists(i))

    if clean_manual and os.path.exists(manual_dir):
        manual_files = glob.glob(os.path.join(manual_dir, '*.json'))
        for mf in manual_files:
            print("Deleting residual manual file:", mf)
            os.remove(mf)

    done_list = []

    loop_count = 1
    sci_count = 0
    std_count = 0
    foc_count = 0

    robot = SEDm(focus_temp=focus_temp, focus_guess=focus_guess,
                 use_winter=use_winter)
    robot.initialize(lamps_off=lamps_off)
    ntimes = obstimes.ScheduleNight()
    night_obs_times = ntimes.get_observing_times_by_date()

    for k, v in night_obs_times.items():
        print(k, v.iso)

    if datetime.datetime.utcnow().hour >= 14:
        print("Waiting for afternoon calibrations")
        while datetime.datetime.utcnow().hour != 0:
            time.sleep(60)

    if do_calib:
        if calib_done:
            print("\nAFTERNOON CALS ALREADY COMPLETED\n")
        else:
            print("\nSTARTING AFTERNOON CALS:", datetime.datetime.utcnow(),
                  "\n")
    else:
        print("\nSKIPPING CALS\n")

    if not calib_done and do_calib:
        if not os.path.exists(calib_done_file):
            # ret = robot.take_datacube_eff()
            print("Doing IFU cals")
            ret0 = robot.take_datacube(robot.ifu, cube='ifu', move=True)
            print("take_datacube - IFU status:\n", ret0)
            print("Doing RC cals")
            ret1 = robot.take_datacube(robot.rc, cube='rc', move=True)
            print("take_datacube - RC status:\n", ret1)
            with open(calib_done_file, 'w') as the_file:
                the_file.write('Datacube completed:%s' % uttime())
    night_obs_times = ntimes.get_observing_times_by_date()

    for k, v in night_obs_times.items():
        print(k, v.iso)

    print("Checking for evening civil twilight")

    while uttime() < night_obs_times['evening_civil']:
        time.sleep(60)

    print("\nPAST EVENING CIVIL TWILIGHT:", datetime.datetime.utcnow(), "\n")

    # Twilight flats loop (from civil to nautical twilight)
    while uttime() < night_obs_times['evening_nautical'] and not twilights_done:
        # Check weather/Dome closed before taking twilight flats  - JP
        while not robot.conditions_cleared():
            print("Faults cleared?", robot.conditions_cleared())
            # If we are past nautical twilight, break out of fault loop
            if uttime() > night_obs_times['evening_nautical']:
                break
            time.sleep(60)

        # Conditions cleared, should be ready for twilights
        print("Faults cleared?", robot.conditions_cleared())
        # Be sure we are not past nautical twilight
        if uttime() > night_obs_times['evening_nautical']:
            break
        else:
            # Time to open dome and get twilight flats
            robot.check_dome_status()
            # How much time do we have for them?
            max_time = (night_obs_times['evening_nautical'] -
                        Time(datetime.datetime.utcnow())).sec
            print("Time to do twilights: %.2f s" % max_time)
            robot.take_twilight(robot.rc, max_time=max_time,
                                end_time=night_obs_times['evening_nautical'])
            time.sleep(60)
            # Write file indicating completion of twilight flats - JP
            with open(twilights_done_file, 'w') as the_file:
                the_file.write('Twilights completed:%s' % uttime())
            twilights_done = True
            print("Evening twilights taken")
    if not twilights_done:
        print("Evening twilights NOT taken")

    for k, v in night_obs_times.items():
        print(k, v.iso)

    print("Checking for evening nautical twilight")
    while uttime() < night_obs_times['evening_nautical']:
        time.sleep(5)

    print("\nPAST EVENING NAUTICAL TWILIGHT:", datetime.datetime.utcnow(),
          "\nSTARTING SCIENCE OBSERVATIONS\n")

    print(datetime.datetime.utcnow(), 'current_time')
    print(night_obs_times['morning_nautical'].iso, 'close_time')

    # Main observing loop (until morning nautical twilight)
    while uttime() < night_obs_times['morning_nautical']:
        print("\n", datetime.datetime.utcnow(),
              "START TARGET LOOP #%d" % loop_count)
        print("number of science observations taken: %d" % sci_count)
        print("number of focus sequences taken: %d" % foc_count)
        print("number of standard observations taken: %d\n" % std_count)

        # are we cleared to observe?
        while not robot.conditions_cleared():
            print("Faults cleared?", robot.conditions_cleared())
            if uttime() > night_obs_times['morning_nautical']:
                break
            time.sleep(60)

        # faults cleared, ready to observe
        print('Faults cleared?', robot.conditions_cleared())

        # did we wait too long?
        if uttime() > night_obs_times['morning_nautical']:
            break
        else:
            # open dome if not already open
            print(robot.check_dome_status())

        # first need to focus
        if not focus_done:
            print("Doing focus")
            ret = robot.run_focus_seq(robot.rc, 'rc_focus', name="Focus",
                                      exptime=30)
            print("run_focus_seq status:\n", ret)
            if 'data' in ret:
                focus_done = True
                foc_count += 1
                focus_data = ret['data']
                robot.focus_temp = focus_data['focus_temp']
                robot.focus_pos = focus_data['focus_pos']
                robot.focus_time = focus_data['focus_time']
                with open(focus_done_file, 'w') as the_file:
                    the_file.write(json.dumps(focus_data))
            else:
                focus_done = False
                print("Unable to calculate focus")
        else:
            if focus_data:
                robot.focus_temp = focus_data['focus_temp']
                robot.focus_pos = focus_data['focus_pos']
                robot.focus_time = focus_data['focus_time']
                print("Focus pos of %.2f at temperature of %.2f achieved at %s"
                      % (robot.focus_pos, robot.focus_temp, robot.focus_time))
            else:
                print("No focus data!  Recommend re-focusing!")

        # grab a standard
        if not standard_done:
            print("Doing standard")
            ret = robot.run_standard_seq(robot.ifu)
            std_count += 1
            print("run_standard_seq status:\n", ret)
            with open(standard_done_file, 'w') as the_file:
                the_file.write('Standard completed:%s' % uttime())
            standard_done = True

        # any manual commands?
        if os.path.exists(manual_dir):
            manual_files = sorted(glob.glob(os.path.join(manual_dir, '*.json')))
            if len(manual_files) > 0:
                print("\nFound manual files:", manual_files)
                for mf in manual_files:
                    print("\nDoing manual command in", mf)
                    with open(mf) as man_file:
                        obsdict = json.load(man_file)
                    ret = robot.run_manual_command(obsdict)
                    print('run_manual_command status:\n', ret)
                    if 'success' in ret:
                        if 'command' in obsdict:
                            if obsdict['command'] == 'standard':
                                std_count += 1
                            elif obsdict['command'] != 'focus':
                                sci_count += 1
                        else:
                            print("no command in manual file?")
                        # increment loop count
                        loop_count += 1
                    print("Removing manual file", mf)
                    os.remove(mf)

                    time.sleep(10)
                print("Manual commands completed")
                continue
            else:
                print("\nNo manual files found.")

        # Proceed with normal queued observations
        try:
            ret = robot.sky.get_next_observable_target(return_type='json')
            print('sky.get_next_observable_target status:\n', ret)
        except Exception as ex:
            print(str(ex), "ERROR getting target")
            ret = robot.sky.reinit()
            print(ret, "sky.reinit (1)")
            time.sleep(10)
            ret = robot.sky.reinit()
            print(ret, "sky.reinit (2)")
            ret = None
            pass

        # Try again to get next target
        if not ret:
            ret = robot.sky.get_next_observable_target(return_type='json')
            print('sky.get_next_observable_target status (2):\n', ret)

        # did we get a valid target?
        if 'data' in ret and isinstance(ret['data'], dict):
            obsdict = ret['data']
            # Has this request already been observed?
            if obsdict['req_id'] in done_list:
                robot.sky.update_target_request(obsdict['req_id'],
                                                status='COMPLETED',
                                                check_growth=True)
                continue    # skip it then
            # When will this observation end?
            end_time = datetime.datetime.utcnow() + datetime.timedelta(
                seconds=obsdict['obs_dict']['total'])
            # If it ends after morning twilight, do a standard instead
            if Time(end_time) > night_obs_times['morning_nautical']:
                print("Waiting to close dome")
                print("Doing morning standard")
                ret = robot.run_standard_seq(robot.ifu)
                print("run_standard_seq status:\n", ret)
                with open(standard_done_file, 'w') as the_file:
                    the_file.write('Standard completed:%s' % uttime())
                standard_done = True
                std_count += 1
                time.sleep(600)
                continue
            # If it ends before morning twilight, do observations
            ret = robot.observe_by_dict(obsdict)
            # Add to done list
            done_list.append(obsdict['req_id'])
            print('observe_by_dict status:\n', ret)
            sci_count += 1

            # Update focus done file status (in case a new focus run needed)
            if not os.path.exists(focus_done_file):
                focus_done = False
            # Check focus status based on temperature
            else:
                # get nominal rc focus based on current temperature
                current_temp = float(
                    robot.ocs.check_weather()['data']['inside_air_temp'])
                if abs(robot.focus_temp - current_temp) > 1.0:
                    nominal_rc_focus = rc_focus.temp_to_focus(current_temp)
                    print("Focus %.2f at Temp of %.2f may have changed."
                          % (robot.focus_pos, robot.focus_temp))
                    print("Model focus of %.2f recommended based on current"
                          " Temp of %.2f" % (nominal_rc_focus, current_temp))
        # No good next target at this time, so just do a standard
        else:
            print("No observable target in queue, doing standard")
            ret = robot.run_standard_seq(robot.ifu)
            print("run_standard_seq status:\n", ret)
            with open(standard_done_file, 'w') as the_file:
                the_file.write('Standard completed:%s' % uttime())
            standard_done = True
            std_count += 1

        # check standard status
        if not os.path.exists(standard_done_file):
            standard_done = False

        loop_count += 1
    # end of main observing loop

    print("\nSCIENCE OBSERVATIONS COMPLETE\nMORNING NAUTICAL TWILIGHT: ",
          datetime.datetime.utcnow(), "\n")
    print("number of science observations taken: %d" % sci_count)
    print("number of focus sequences taken: %d" % foc_count)
    print("number of standard observations taken: %d" % std_count)

    # Morning twilight flats loop from nautical to civil twilight
    while uttime() < night_obs_times['morning_civil'] and not twilights_done:
        # Check if Twilights were done at the start of the night - JP
        if not twilights_done and do_twilights:
            # Check weather/Dome closed before taking twilight flats  - JP
            while not robot.conditions_cleared():
                print("Faults cleared?", robot.conditions_cleared())
                # If we are past morning civil twilight, break out of fault loop
                if uttime() > night_obs_times['morning_civil']:
                    break
                time.sleep(60)

            # conditions cleared, so ready for twilights
            print("Faults cleared?", robot.conditions_cleared())
            # but be sure we are not past morning civil twilight
            if uttime() > night_obs_times['morning_civil']:
                break
            else:
                # make sure dome is open
                robot.check_dome_status()
                # how much time do we have for twilights?
                max_time = (night_obs_times['morning_civil'] -
                            Time(datetime.datetime.utcnow())).sec
                print("Seconds for twilight flats:", max_time)
                robot.take_twilight(robot.rc, max_time=max_time,
                                    end_time=night_obs_times['morning_civil'])
                time.sleep(60)
                with open(twilights_done_file, 'w') as the_file:
                    the_file.write('Twilights completed:%s' % uttime())
                twilights_done = True
                print("Morning twilights taken")
        else:
            break
    # end of morning twilight flats loop

    if not twilights_done:
        print("Morning twilights NOT taken")

    print("\nEND OF NIGHT: ", datetime.datetime.utcnow(),
          "\n%d observation sets taken\n" % (sci_count+std_count))

    # close dome
    ret = robot.ocs.dome('close')
    print('ocs.dome status:', ret)
    time.sleep(120)
    # stow telescope
    ret = robot.ocs.stow(ha=0, dec=109, domeaz=220)
    print('ocs.stow status:', ret)

    # clean up done files
    print("Cleaning up")
    clean_up()
    # confirm telescope stow
    ret = robot.ocs.stow(ha=0, dec=109, domeaz=220)
    print('ocs.stow (2) status:', ret)
    # sleep for a few hours
    print("Going to sleep")
    time.sleep(7200)
    print("Gzipping images")
    robot.gzip_images(robot.obs_dir)
    print("Second sleep")
    time.sleep(7200)
    print("Third")
    time.sleep(7200)


if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        description="""Start SEDM observations""",
        formatter_class=argparse.RawTextHelpFormatter
    )

    parser.add_argument('-r', '--reset', action="store_true", default=False,
                        help='Reset calibration lamps')
    parser.add_argument('-c', '--close', action="store_true", default=False,
                        help='Close dome')
    parser.add_argument('-o', '--open', action="store_true", default=False,
                        help='Open dome')
    parser.add_argument('-t', '--temperature', type=float, default=None,
                        help='Temperature estimate (for focus)')
    parser.add_argument('-w', '--winter', action="store_true", default=False,
                        help='Use WINTER for weather data')
    parser.add_argument('-n', '--noclean', action="store_true", default=False,
                        help='Do not clean residual manual files')
    args = parser.parse_args()

    if args.reset:      # reset dome, arc lamps
        trobot = SEDm(run_ifu=False, run_rc=False, run_sky=False,
                      run_sanity=False)
        trobot.initialize(lamps_off=True)

    elif args.close:    # close dome
        trobot = SEDm(run_ifu=False, run_rc=False, run_sky=False,
                      run_sanity=False)
        trobot.initialize()
        # close dome
        tret = trobot.ocs.dome('close')
        print('ocs.dome status:', tret)

    elif args.open:     # open dome
        trobot = SEDm(run_ifu=False, run_rc=False, run_sky=False,
                      run_sanity=False)
        trobot.initialize()
        # open dome
        tret = trobot.ocs.dome('open')
        print('ocs.dome status:', tret)

    else:               # start observations
        lampsoff = False
        del_manual = not args.noclean
        try:
            while True:
                try:
                    if lampsoff:
                        print("Turning all lamps off")
                    else:
                        print("Keeping lamps in current state")
                    run_observing_loop(lamps_off=lampsoff,
                                       temperature=args.temperature,
                                       use_winter=args.winter,
                                       clean_manual=del_manual)
                    lampsoff = False
                    del_manual = True
                except Exception as e:
                    tb_str = traceback.format_exception(etype=type(e), value=e,
                                                        tb=e.__traceback__)
                    print(datetime.datetime.utcnow(),
                          "FATAL (restart):\n", "".join(tb_str))
                    print("\nSleep for 60s and start loop again")
                    # Something went wrong:
                    #   let's restart with all cal lamps off,
                    #   and clean manual files
                    lampsoff = True
                    del_manual = True
                    time.sleep(60)

        except Exception as e:
            tb_str = traceback.format_exception(etype=type(e), value=e,
                                                tb=e.__traceback__)
            print("".join(tb_str))
