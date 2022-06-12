import json
from string import Template
import datetime
import pandas as pd
import astroplan

from astropy.coordinates import SkyCoord, EarthLocation, AltAz, get_moon
import astropy.units as u
import os
import sys
import psycopg2.extras
import psycopg2
import time
from utils import obstimes
from utils import sedmpy_import
import sqlite3
from sky.growth.marshal import Interface
import version

from astropy.time import Time, TimeDelta
from astropy.utils.iers import conf
conf.auto_max_age = None


# noinspection SqlNoDataSourceInspection
class Scheduler:
    def __init__(self, config='schedulerconfig.json',
                 site_name='Palomar', obsdatetime=None,
                 save_as="targets.json"):

        self.scheduler_config_file = config
        with open(os.path.join(version.CONFIG_DIR, config)) as data_file:
            self.params = json.load(data_file)
        self.path = self.params["standard_db"]
        self.target_dir = self.params["target_dir"]
        self.standard_dict = {}
        self.standard_star_list = []

        self.site_name = site_name
        self.times = obstimes.ScheduleNight()
        self.obs_times = self.times.get_observing_times_by_date()
        self.site = EarthLocation.of_site(self.site_name)
        self.obs_site_plan = astroplan.Observer.at_site(
            site_name=self.site_name)
        self.obsdatetime = obsdatetime
        self.running_obs_time = None
        self.save_as = save_as
        self.dbconn = psycopg2.connect(**self.params["dbconn"])
        self.ph_db = sedmpy_import.dbconnect()
        self.growth = Interface()
        self.query = Template(
            "SELECT r.id AS req_id, r.object_id AS obj_id, \n"
            "r.user_id, r.marshal_id, r.exptime, r.maxairmass,\n"
            "r.max_fwhm, r.min_moon_dist, r.max_moon_illum, \n"
            "r.max_cloud_cover, r.status, \n"
            "r.priority AS reqpriority, r.inidate, r.enddate,\n"
            "r.cadence, r.phasesamples, r.sampletolerance, \n"
            "r.filters, r.nexposures, r.obs_seq, r.seq_repeats,\n"
            "r.seq_completed, r.last_obs_jd, r.creationdate,\n"
            "r.lastmodified, r.allocation_id, r.marshal_id, \n"
            "o.id AS obj_id, o.name AS objname, o.iauname, o.ra, o.\"dec\",\n"
            "o.typedesig, o.epoch, o.magnitude, o.creationdate, \n"
            "u.id, u.email, a.id AS allocation_id, \n"
            "a.inidate, a.enddate, a.time_spent, a.designator as p60prid, \n"
            "a.time_allocated, a.program_id, a.active, \n"
            "p.designator, p.name, p.group_id, p.pi,\n"
            "p.time_allocated, r.priority, p.inidate,\n"
            "p.enddate, pe.mjd0, pe.phasedays, pe.phi,\n"
            "r.phase, r.sampletolerance\n"
            "FROM \"public\".request r\n"
            "INNER JOIN \"public\".\"object\" o ON (r.object_id = o.id)\n"
            "INNER JOIN \"public\".users u ON (r.user_id = u.id)\n"
            "INNER JOIN \"public\".allocation a ON (r.allocation_id = a.id)\n"
            "INNER JOIN \"public\".program p ON (a.program_id = p.id)\n"
            "LEFT JOIN \"public\".periodic pe on (pe.object_id=o.id)\n"
            "${where_statement}\n"
            "${and_statement}\n"
            "${group_statement}\n"
            "${order_statement}"
        )

        self.tr_row = Template(
            """<tr id="${allocation}">
            <td>${obstime}</td>
            <td>${objname}</td>
            <td>${priority}</td>
            <td>${project}</td>
            <td>${ra}</td>
            <td>${dec}</td>
            <td>${start_airmass}</td>
            <td>${end_airmass}</td>
            <td>${moon_dist}</d>
            <td>${ifu_exptime}</td>
            <td>Filters:${rc_seq}<br>Exptime:${rc_exptime}</td>
            <td>${total}</td>
            <td><a href='request?request_id=${request_id}'>+</a></td>
            <td>${rejects}</td>
            </tr>"""
        )

    def __load_targets_from_db(self):
        """
        Open the sqlite database of targets
        pacfic

        :return:
        """
        print(self.path)
        conn = sqlite3.connect(self.path)
        cur = conn.cursor()

        results = cur.execute("SELECT * FROM standards")
        standards = results.fetchall()
        for ss in standards:
            name, ra, dec, exptime = ss[0].rstrip().encode(
                'utf8'), ss[3], ss[4], ss[5]

            if name.upper() == 'LB227':
                continue
            coords = SkyCoord(ra=ra, dec=dec, unit='deg')
            obj = astroplan.FixedTarget(name=name, coord=coords)
            self.standard_star_list.append(obj)
            self.standard_dict[name] = {
                'name': name,
                'ra': ra,
                'dec': dec,
                'exptime': exptime
            }

    def get_standard(self, name=None, obsdate=None):
        """
        If the name is not given find the closest standard star to zenith
        :param name:
        :param obsdate:
        :return:
        """
        print("Choosing a standard")
        start = time.time()
        self.__load_targets_from_db()
        if not obsdate:
            obsdate = datetime.datetime.utcnow()

        if not name:
            name = 'zenith'
        print("Finding standard: %s" % name)
        if name.lower() == 'zenith':
            sairmass = 100
            for standard in self.standard_star_list:
                print(standard)
                airmass = self.obs_site_plan.altaz(obsdate, standard).secz

                if sairmass > airmass > 0:
                    target = standard
                    sairmass = airmass
                    name = target.name
        if type(name) == str:
            name = name.encode('UTF-8')
        std = self.standard_dict[name]
        std['name'] = std['name'].decode('utf-8')
        return {'elaptime': time.time() - start,
                'data': std}

    def _set_obs_seq(self, row):
        """
        Parse database target scheme

        :param row:
        :return:
        """

        obs_seq_list = row['obs_seq']
        exp_time_list = row['exptime']
        repeat = row['seq_repeats']

        # Prep the variables
        ifu = False
        rc = False
        rc_total = 0
        ifu_total = 0

        rc_filter_list = ['r', 'g', 'i', 'u']
        ifu_exptime = 0

        # 1. First we extract the filter sequence

        seq = list(obs_seq_list)
        exptime = list(exp_time_list)

        # 2. Remove ifu observations first if they exist
        index = [i for i, sq in enumerate(seq) if 'ifu' in sq]

        if index:
            for j in index:
                ifu = seq.pop(j)
                ifu_exptime = int(exptime.pop(j))

                if ifu_exptime == 0:
                    ifu = False
                elif ifu_exptime == 60:
                    ifu_exptime = 1800

        # 3. If the seq list is empty then there is no photmetry follow-up
        # and we should exit

        if not seq:
            ifu_total = ifu_exptime
            obs_seq_dict = {
                'ifu': ifu,
                'ifu_exptime': ifu_exptime,
                'ifu_total': ifu_total + 47,
                'rc': rc,
                'rc_obs_dict': None,
                'rc_total': 0,
                'total': abs(ifu_total + 47 + (rc_total * repeat))
            }
            return obs_seq_dict

        if ifu:
            ifu_total = ifu_exptime

        # 4. If we are still here then we need to get the photometry sequence
        obs_order_list = []
        obs_exptime_list = []
        obs_repeat_list = []

        for i in range(len(seq)):

            flt = seq[i][-1]
            flt_exptime = int(exptime[i])
            flt_repeat = int(seq[i][:-1])
            # 4a. After parsing the individual elements we need to check that
            # they are
            # valid values
            if flt in rc_filter_list:

                if 0 <= flt_exptime <= 1000:
                    if 1 <= flt_repeat <= 100:
                        obs_order_list.append(flt)
                        obs_exptime_list.append(str(flt_exptime))
                        obs_repeat_list.append(str(flt_repeat))
                        rc_total += ((flt_exptime + 47) * flt_repeat)
            else:
                continue

        # 5. If everything went well then we should have three non empty list.

        if len(obs_order_list) >= 1:
            rc = True
            obs_dict = {
                'obs_order': ','.join(obs_order_list),
                'obs_exptime': ','.join(obs_exptime_list),
                'obs_repeat_filter': ','.join(obs_repeat_list),
                'obs_repeat_seq': repeat}
        else:
            rc = False
            obs_dict = None

        obs_seq_dict = {
            'ifu': ifu,
            'ifu_exptime': ifu_exptime,
            'ifu_total': ifu_total,
            'rc': rc,
            'rc_obs_dict': obs_dict,
            'rc_total': rc_total * repeat,
            'total': abs(ifu_total + (rc_total * repeat))
        }

        return obs_seq_dict

    def _set_fixed_targets(self, row):
        """
        Add a column of SkyCoords to pandas dataframe
        :return:
        """

        return astroplan.FixedTarget(name=row['objname'],
                                     coord=row['SkyCoords'])

    def _set_end_time(self, row):
        return row['start_obs'] + TimeDelta(row['obs_seq']['total'],
                                            format='sec')

    def _set_start_altaz(self, row):
        return row['SkyCoords'].transform_to(AltAz(obstime=row['start_obs'],
                                                   location=self.site)).alt

    def _set_end_altaz(self, row):
        return row['SkyCoords'].transform_to(AltAz(obstime=row['end_obs'],
                                                   location=self.site)).alt

    def _set_start_airmass(self, row):
        return row['SkyCoords'].transform_to(AltAz(obstime=row['start_obs'],
                                                   location=self.site)).secz

    def _set_end_airmass(self, row):
        return row['SkyCoords'].transform_to(AltAz(obstime=row['end_obs'],
                                                   location=self.site)).secz

    def _set_start_ha(self, row):
        return self.obs_site_plan.target_hour_angle(row['start_obs'],
                                                    row['fixed_object'])

    def _set_end_ha(self, row):
        return self.obs_site_plan.target_hour_angle(row['end_obs'],
                                                    row['fixed_object'])

    def _set_rise_time(self, row):
        return self.obs_site_plan.target_rise_time(row['start_obs'],
                                                   row['fixed_object'],
                                                   horizon=15 * u.degree,
                                                   which="next")

    def _set_set_time(self, row):
        return self.obs_site_plan.target_set_time(row['start_obs'],
                                                  row['fixed_object'],
                                                  horizon=15 * u.degree,
                                                  which="next")

    def _convert_row_to_json(self, row, fields=('name', 'ra', 'dec',
                                                'obj_id', 'req_id',
                                                'email', 'objname', 'pi')):
        """

        :param row:
        :param fields:
        :return:
        """
        if fields is not None:
            pass

        return dict(name=row.objname, p60prnm=row.name,
                    p60prid=row.p60prid, p60prpi=row.pi,
                    ra=row.ra, dec=row.dec, equinox=row.epoch,
                    req_id=row.req_id, obj_id=row.obj_id,
                    obs_dict=row.obs_seq, marshal_id=row.marshal_id)

    def simulate_night(self, start_time='', end_time='', do_focus=True,
                       do_standard=True, target_list=None,
                       return_type='html', sort_columns=('priority',
                                                         'start_alt'),
                       sort_order=(False, False), ):
        """

        :param start_time:
        :param end_time:
        :param do_focus:
        :param do_standard:
        :param target_list:
        :param return_type:
        :param sort_columns:
        :param sort_order:
        :return:
        """

        start = time.time()

        # 1. Get start and end times
        if not start_time:
            start_time = self.obs_times['evening_nautical']
            if datetime.datetime.utcnow() > start_time:
                start_time = Time(datetime.datetime.utcnow())
        if not end_time:
            end_time = self.obs_times['morning_astronomical']

        self.running_obs_time = start_time

        if not isinstance(target_list, pd.DataFrame) and not target_list:
            print("sn: Making a new target list for night simulation")
            ret = self.get_active_targets()
            if 'data' in ret:
                targets = ret['data']
            else:
                return ret

            target_list = self.initialize_targets(targets)['data']

        if len(target_list) == 0:
            return {'data': False, 'elaptime': time.time() - start}

        # obsdatetime = self.running_obs_time

        if return_type == 'html':
            html_str = """<table class='table'><tr><th>Expected Obs Time</th>
                              <th>Object Name</th>
                              <th>Priority</th>
                              <th>Project ID</th>
                              <th>RA</th>
                              <th>DEC</th>
                              <th>Start Air</th>
                              <th>End Air</th>
                              <th>Moon Dist</th>
                              <th>IFU Exptime</th>
                              <th>RC Exptime</th>
                              <th>Total Exptime</th>
                              <th>Update Request</th>
                              <th>Priority 4+ reject<br>reasons</th>
                              </tr>"""
        else:
            html_str = ""

        # 2. Get all targets
        targets = target_list

        # 3. Go through all the targets until we fill up the night
        current_time = start_time

        while current_time <= end_time:
            targets = self.update_targets_coords(targets, current_time)['data']
            targets = targets.sort_values(list(sort_columns),
                                          ascending=list(sort_order))
            print("sn: Using input datetime of", current_time.iso)
            # Include focus time?
            if do_focus:
                current_time += TimeDelta(300, format='sec')
                do_focus = False
            if do_standard:
                current_time += TimeDelta(300, format='sec')
                do_standard = False

            time_remaining = end_time - current_time

            if time_remaining.sec <= 0:
                break

            self.running_obs_time = current_time

            z = self.get_next_observable_target(targets,
                                                obsdatetime=current_time,
                                                update_coords=False,
                                                return_type=return_type,
                                                do_sort=False)

            # targets = self.remove_setting_targets(
            #       targets, start_time=current_time, end_time=end_time)

            idx, t = z

            if not idx:
                print("sn:", len(targets))
                if return_type == 'html':
                    html_str += self.tr_row.substitute(
                        {
                            'allocation': "",
                            'obstime': current_time.iso,
                            'objname': "Standard",
                            'priority': "",
                            'project': "Calib",
                            'ra': "",
                            'dec': "",
                            'start_airmass': "",
                            'end_airmass': "",
                            'moon_dist': "",
                            'ifu_exptime': 300,
                            'rc_seq': "",
                            'rc_exptime': "",
                            'total': 300,
                            'request_id': "NA",
                            'rejects': ""
                        }
                    )
                current_time += TimeDelta(300, format='sec')
            else:
                if return_type == 'html':
                    html_str += t[1]
                    t = t[0]

                targets = targets[targets.req_id != idx]
                # Adding overhead
                # current_time += TimeDelta(t['total']*1.2 + 120, format='sec')
                current_time += TimeDelta(t['total'] + 60, format='sec')

        if return_type == 'html':
            html_str += "</table><br>Last Updated:%s UT" % \
                        datetime.datetime.utcnow()
            return html_str

    def get_active_targets(self, startdate=None, enddate=None,
                           where_statement="", and_statement="",
                           group_statement="", order_statement="",
                           save_copy=True):

        start = time.time()

        if not startdate:
            if datetime.datetime.utcnow().hour >= 14:
                startdate = (datetime.datetime.utcnow() + datetime.timedelta(
                    days=1))
            else:
                startdate = datetime.datetime.utcnow()

            startdate = startdate.replace(hour=23, minute=59, second=59)

        if not enddate:
            enddate = (datetime.datetime.utcnow() +
                       datetime.timedelta(days=1)).strftime("%Y-%m-%d")

        if not where_statement:
            where_statement = ("WHERE r.enddate >= '%s' AND r.object_id > 100 "
                               "AND r.inidate <= '%s'" % (enddate, startdate))

        if not and_statement:
            and_statement = "AND r.status = 'PENDING'"

        q = self.query.substitute(where_statement=where_statement,
                                  and_statement=and_statement,
                                  group_statement=group_statement,
                                  order_statement=order_statement)

        self.dbconn = psycopg2.connect(**self.params["dbconn"])

        df = pd.read_sql_query(q, self.dbconn)

        if save_copy:
            df.to_csv(os.path.join(self.target_dir, self.save_as))

        return {"data": df, "elaptime": time.time() - start}

    def initialize_targets(self, target_df, obstime=''):
        start = time.time()
        mask = (target_df['typedesig'] == 'f')
        target_df_valid = target_df[mask]

        target_df['SkyCoords'] = False
        target_df.loc[mask, 'SkyCoords'] = SkyCoord(ra=target_df_valid['ra'],
                                                    dec=target_df_valid['dec'],
                                                    unit="deg")
        target_df['start_obs'] = False
        if not obstime:
            obstime = datetime.datetime.utcnow()

        target_df.loc[mask, 'start_obs'] = Time(obstime)

        target_df['obs_seq'] = target_df.apply(self._set_obs_seq, axis=1)
        target_df['end_obs'] = target_df.apply(self._set_end_time, axis=1)
        target_df['start_alt'] = target_df.apply(self._set_start_altaz,
                                                 axis=1)
        target_df['end_alt'] = target_df.apply(self._set_end_altaz, axis=1)
        target_df['fixed_object'] = target_df.apply(self._set_fixed_targets,
                                                    axis=1)
        target_df['start_ha'] = target_df.apply(self._set_start_ha, axis=1)
        target_df['end_ha'] = target_df.apply(self._set_end_ha, axis=1)
        target_df['start_airmass'] = target_df.apply(self._set_start_airmass,
                                                     axis=1)
        target_df['end_airmass'] = target_df.apply(self._set_end_airmass,
                                                   axis=1)
        target_df['rise_time'] = target_df.apply(self._set_rise_time, axis=1)
        target_df['set_time'] = target_df.apply(self._set_set_time, axis=1)

        return {'data': target_df, 'elaptime': time.time() - start}

    def update_targets_coords(self, df, obstime=None):
        start = time.time()
        df['start_obs'] = False

        if not obstime:
            obstime = datetime.datetime.utcnow()

        df['start_obs'] = Time(obstime)
        df['end_obs'] = df.apply(self._set_end_time, axis=1)
        df['start_alt'] = df.apply(self._set_start_altaz, axis=1)
        df['end_alt'] = df.apply(self._set_end_altaz, axis=1)
        df['start_ha'] = df.apply(self._set_start_ha, axis=1)
        df['end_ha'] = df.apply(self._set_end_ha, axis=1)
        df['start_airmass'] = df.apply(self._set_start_airmass, axis=1)
        df['end_airmass'] = df.apply(self._set_end_airmass, axis=1)
        return {'data': df, 'elaptime': time.time() - start}

    def look_for_new_targets(self, df, startdate=None, enddate=None,
                             where_statement="", and_statement="",
                             group_statement="", order_statement="",
                             field='req_id'):

        start = time.time()

        ret = self.get_active_targets(startdate=startdate,
                                      enddate=enddate,
                                      where_statement=where_statement,
                                      and_statement=and_statement,
                                      group_statement=group_statement,
                                      order_statement=order_statement)

        if 'data' in ret:
            new_df = ret['data']
        else:
            return ret

        new_targets = (list(set(new_df[field]) - set(df[field])))

        dropped_targets = (list(set(df[field]) - set(new_df[field])))

        if len(new_targets) >= 1:
            new_df = new_df[new_df['req_id'].isin(new_targets)]
            ret = self.initialize_targets(new_df)

            if 'data' in ret:
                df = df.append(ret['data'])

        if len(dropped_targets) >= 1:
            df = df[-df["req_id"].isin(dropped_targets)]

        return {'data': df, 'elaptime': time.time() - start}

    def get_next_observable_target(self, target_list=None, obsdatetime=None,
                                   airmass=(1, 2.8), moon_sep=(5, 180),
                                   altitude_min=15, ha=(18.75, 5.75),
                                   return_type='', do_airmass=True,
                                   do_sort=True, do_moon_sep=True,
                                   sort_columns=('priority', 'set_time'),
                                   sort_order=(False, False), save=False,
                                   save_as='',
                                   check_end_of_night=True, update_coords=True):
        """

        :return:
        """
        if check_end_of_night:
            pass
        if moon_sep is not None:
            pass
        if len(ha) > 2:
            print("too many ha angles!")
        cons = ['', 'Alt', 'Air', 'Moon']
        st = time.time()
        # If the target_list is empty then all we can do is return back no
        # target and do a standard for the time being

        if not isinstance(target_list, pd.DataFrame) and not target_list:
            print("gnot: Making a new target list")
            ret = self.get_active_targets()
            if 'data' in ret:
                targets = ret['data']
            else:
                return ret

            target_list = self.initialize_targets(targets)['data']

        # no targets, bail
        if len(target_list) == 0:
            return {'data': False, 'elaptime': time.time() - st}

        # unless obs time input, use now
        if not obsdatetime:
            obsdatetime = Time(datetime.datetime.utcnow())
        else:
            obsdatetime = Time(obsdatetime)

        print("gnot: Using time of", obsdatetime.iso)

        if update_coords:
            target_list = self.update_targets_coords(target_list,
                                                     obsdatetime)['data']

        # Remove targets outside of HA range
        if do_sort:
            target_list = target_list.sort_values(list(sort_columns),
                                                  ascending=list(sort_order))
        rej_html = ""
        target_reorder = False
        # print(target_list['typedesig'])

        # loop over target list
        for row in target_list.itertuples():

            # starting time of observation
            start = obsdatetime
            # add all requested exposures and overheads
            finish = start + TimeDelta(row.obs_seq['total'],
                                       format='sec')
            # We are now into the low priority objects, so just sort on HA
            if row.priority <= 2 and not target_reorder:
                # print(target_list.keys())
                target_list = target_list.sort_values('start_ha',
                                                      ascending=False)
                target_reorder = True
                continue

            # Must have altitude constraint
            constraint = [astroplan.AltitudeConstraint(
                min=altitude_min * u.deg)]
            # Requested airmass constraint
            if do_airmass:
                constraint.append(astroplan.AirmassConstraint(min=airmass[0],
                                                              max=airmass[1]))
            # Requested moon distance constraint
            if do_moon_sep:
                moon_illum = float(
                    self.obs_site_plan.moon_illumination(start)) * 100.
                if moon_illum > 75.:
                    min_moon_sep = 5.0 + (moon_illum - 75.)
                else:
                    min_moon_sep = 5.0
                print("gnot: min moon sep = %.2f" % min_moon_sep)
                # TODO: adjust minimum based on phase of moon
                constraint.append(astroplan.MoonSeparationConstraint(
                    min=min_moon_sep * u.degree))

            # Are we a 'fixed' target?
            if row.typedesig == 'f':
                print("gnot:", row.objname)
                # Are we observable within given constraints?
                if astroplan.is_observable(constraint, self.obs_site_plan,
                                           row.fixed_object,
                                           times=[start, finish],
                                           time_grid_resolution=0.1 * u.hour):

                    s_ha = float(row.start_ha.to_string(unit=u.hour,
                                                        decimal=True))
                    e_ha = float(row.end_ha.to_string(unit=u.hour,
                                                      decimal=True))
                    s_air = float(row.start_airmass)
                    e_air = float(row.end_airmass)
                    if s_ha > 12.:
                        s_ha_ew = -(24. - s_ha)
                    else:
                        s_ha_ew = s_ha

                    if e_ha > 12.:
                        e_ha_ew = -(24. - e_ha)
                    else:
                        e_ha_ew = e_ha

                    # Skip targets that start or end outside HA range
                    if 18.75 > s_ha > 5.25:
                        print("gnot: HA start outside range: %.4f" % s_ha_ew)
                        continue
                    if 18.75 > e_ha > 5.25:
                        print("gnot: HA end outside range: %.4f" % e_ha_ew)
                        continue
                    # Skip extreme airmass at either end
                    if s_air > 3.5:
                        print("gnot: Airmass start outside range: %.3f" % s_air)
                        continue
                    if e_air > 3.5:
                        print("gnot: Airmass end outside range: %.3f" % e_air)
                        continue

                    # Here is our target!
                    moon_coords = get_moon(obsdatetime,
                                           location=self.obs_site_plan.location)
                    tc = row.SkyCoords
                    moon_dist = moon_coords.separation(tc).deg
                    print("gnot: %s %.1f %.6f %.6f %.3f %.3f %.2f %s %s " %
                          (row.objname, row.priority, row.ra, row.dec,
                           row.start_airmass, row.end_airmass, moon_dist,
                           row.start_ha, row.end_ha), row.start_obs)
                    if return_type == 'html':
                        if row.obs_seq['rc']:
                            rc_seq = row.obs_seq['rc_obs_dict']['obs_order'],
                            rc_exptime = row.obs_seq['rc_obs_dict'][
                                             'obs_exptime'],
                        else:
                            rc_seq = 'NA'
                            rc_exptime = 'NA'

                        html = self.tr_row.substitute(
                            {'allocation': row.allocation_id,
                             'obstime': start.iso,
                             'objname': row.objname,
                             'priority': row.priority,
                             'project': row.designator,
                             'ra': "%.7f" % row.ra,
                             'dec': "%+.7f" % row.dec,
                             'start_airmass': "%.4f" % row.start_airmass,
                             'end_airmass': "%.4f" % row.end_airmass,
                             'moon_dist': "%.2f" % moon_dist,
                             'ifu_exptime': row.obs_seq['ifu_exptime'],
                             'rc_seq': rc_seq,
                             'rc_exptime': rc_exptime,
                             'total': row.obs_seq['total'],
                             'request_id': row.req_id,
                             'rejects': rej_html}
                        )
                        return row.req_id, (row.obs_seq, html)
                    elif return_type == 'json':
                        targ = self._convert_row_to_json(row)

                        if save:
                            if not save_as:
                                save_as = os.path.join(
                                    self.target_dir,
                                    "next_target_%s.json" %
                                    datetime.datetime.utcnow().strftime(
                                        "%Y%m%d_%H_%M_%S"))

                            with open(save_as, 'w') as outfile:
                                outfile.write(json.dumps(targ))

                        return {"elaptime": time.time() - st, "data": targ}
                    else:
                        return row.req_id, row.obs_seq
                else:
                    print("gnot: Not observable, priority = %d" % row.priority)
                    if row.priority >= 4:
                        count = 1
                        num = []
                        reas = []
                        for con in constraint:
                            ret = astroplan.is_observable(
                                [con], self.obs_site_plan,
                                row.fixed_object, times=[start, finish],
                                time_grid_resolution=0.1 * u.hour)
                            # print(ret, con)
                            if not ret:
                                num.append(str(count))
                                reas.append(cons[count])
                                print("gnot:", ret, cons[count])
                            count += 1
                        if return_type == 'html' and len(num) >= 1:
                            rej_html += """%s: %s<br>""" % (row.objname,
                                                            ','.join(reas))
                        elif return_type == 'json' and len(num) >= 1:
                            rej_html += ','.join(num)
            sys.stdout.flush()

        if return_type == 'json':
            return {"elaptime": time.time() - st, "error": "No targets found"}
        return False, False

    def get_lst(self, obsdatetime=None):
        start = time.time()
        if obsdatetime:
            self.obsdatetime = obsdatetime
        else:
            self.obsdatetime = datetime.datetime.utcnow()

        obstime = Time(self.obsdatetime)
        lst = self.obs_site_plan.local_sidereal_time(obstime)
        return {"elaptime": time.time() - start,
                "data": lst}

    def get_sun(self, obsdatetime=None):
        start = time.time()
        if obsdatetime:
            self.obsdatetime = obsdatetime
        else:
            self.obsdatetime = datetime.datetime.utcnow()

        obstime = Time(self.obsdatetime)

        sun = self.obs_site_plan.sun_altaz(obstime)

        return {"elaptime": time.time() - start,
                "data": sun}

    def get_twilight_coords(self, obsdatetime=None, dec=33.33):
        """

        :param obsdatetime:
        :param dec:
        :return:
        """
        start = time.time()
        if obsdatetime:
            self.obsdatetime = obsdatetime
        else:
            self.obsdatetime = datetime.datetime.utcnow()

        # Get sidereal time
        lst = self.get_lst(self.obsdatetime)

        ra = lst['data'].degree

        return {'elaptime': time.time() - start,
                'data': {'ra': round(ra, 4),
                         'dec': dec}
                }

    def get_twilight_exptime(self, obsdatetime=None, camera='rc'):
        """

        :param obsdatetime:
        :param camera:
        :return:
        """
        start = time.time()

        if obsdatetime:
            self.obsdatetime = obsdatetime
        else:
            self.obsdatetime = datetime.datetime.utcnow()

        # Get sun angle
        sun_pos = self.get_sun(self.obsdatetime)
        sun_angle = sun_pos['data'].alt.degree
        print(sun_angle, type(sun_angle))
        if -10 >= sun_angle >= -12:
            exptime = 180
        elif -8 >= sun_angle >= -10:
            exptime = 120
        elif -6 >= sun_angle >= -8:
            exptime = 60
        elif -4 >= sun_angle >= -6:
            exptime = 10
        else:
            exptime = 1

        if camera == 'ifu':
            exptime *= 1.5

        return {'elaptime': time.time() - start, 'data': {'exptime': exptime}}

    def get_focus_coords(self, obsdatetime=None, dec=23.33):
        """

        :param obsdatetime:
        :param dec:
        :return:
        """
        start = time.time()

        if obsdatetime:
            self.obsdatetime = obsdatetime
        else:
            self.obsdatetime = datetime.datetime.utcnow() + \
                               datetime.timedelta(hours=1)

        # Get sidereal time
        lst = self.get_lst(self.obsdatetime)

        ra = lst['data'].degree

        return {'elaptime': time.time() - start, 'data': {'ra': round(ra, 4),
                                                          'dec': dec}}

    def add_object(self, name=None, typedesig="f", ra=0.0, dec=0.0,
                   epoch=2000., magnitude=None, iauname=""):
        """Add object to database on pharos
        :param name: (str) object name (required)
        :param typedesig: (str) type of object (required, default=f)
                    'f' (fixed), 'v' (periodic fixed),
                    'P' (built-in planet or satellite name),
                    'e' (heliocentric elliptical),
                    'h' (heliocentric hyperbolic), 'p' (heliocentric parabolic),
                    'E' (geocentric elliptical)
        :param ra:
        :param dec: (float) coordinates in decimal degrees (required for
                typedesig='f' objects)
        :param epoch: (float) coordinate epoch (default=2000.)
        :param magnitude: (float) r-band magnitude
        :param iauname: (str) IAU designation for transient (optional)
        """
        # do we already exist in the db?
        object_id = self.ph_db.get_object_id_from_name(name)
        if object_id:
            return object_id[0][0]
        else:
            # Required
            pardict = {
                'name': name,
                'typedesig': typedesig
            }
            # Optional
            if typedesig == 'f':
                pardict['ra'] = ra
                pardict['dec'] = dec
                pardict['epoch'] = epoch
            if magnitude:
                pardict['magnitude'] = magnitude
            if iauname:
                pardict['iauname'] = iauname
            # Add object
            object_id, message = self.ph_db.add_object(pardict)
            if object_id == -1:
                print("Could not add object to db")
                return -1
            else:
                print(message)
                return object_id

    def get_manual_request_id(self, name="", typedesig="f", ra=None, dec=None,
                              epoch=2000., magnitude=None, exptime=180,
                              allocation_id="", obs_seq='{1ifu}'):
        """
        :param name:
        :param typedesig:
        :param ra:
        :param dec:
        :param epoch:
        :param magnitude:
        :param exptime:
        :param allocation_id: (str) Allocation table id number as a string
        :param obs_seq: (str) Observation sequence: '{1ifu}' or '{1rc}'
        :return: bool, id
        """
        start = time.time()

        object_id = self.ph_db.get_object_id_from_name(name)
        if not object_id:
            object_id = self.add_object(name=name, typedesig=typedesig,
                                        ra=ra, dec=dec, epoch=epoch,
                                        magnitude=magnitude)
            if object_id <= 0:
                print("Could not add %s to SedmDb" % name)
                return {'elaptime': time.time() - start,
                        'error': 'Unable to add object to SedmDb'}
        else:
            for obj in object_id:
                if obj[1].lower() == name.lower():
                    object_id = obj[0]
                    break

        if not allocation_id:
            allocation_id = '20211011220000019'
            p60prid = '2021B-calib'
            p60prnm = "SEDm calibration"
            p60prpi = "SEDm"
        else:
            db_ret = self.ph_db.get_from_allocation(
                ['designator, program_id'],
                where_dict={'id': allocation_id})[0]
            p60prid = db_ret[0]
            prog_id = db_ret[1]
            db_ret = self.ph_db.get_from_program(['name', 'PI'],
                                                 where_dict={'id': prog_id})[0]
            p60prnm = db_ret[0]
            p60prpi = db_ret[1]

        start_date = datetime.datetime.utcnow()
        end_date = start_date + datetime.timedelta(days=1)
        request_dict = {
            'obs_seq': obs_seq,
            'exptime': '{%s}' % int(exptime),
            'object_id': object_id,
            'marshal_id': '-1',
            'user_id': 2,
            'allocation_id': allocation_id,
            'priority': '-1',
            'inidate': start_date.strftime("%Y-%m-%d"),
            'enddate': end_date.strftime("%Y-%m-%d"),
            'maxairmass': '2.5',
            'status': 'PENDING',
            'max_fwhm': '10',
            'min_moon_dist': '30',
            'max_moon_illum': '1',
            'max_cloud_cover': '1',
            'seq_repeats': '1',
            'seq_completed': '0'
        }
        request_id = self.ph_db.add_request(request_dict)[0]
        return {
            'elaptime': time.time() - start,
            'data': {'object_id': object_id, 'request_id': request_id,
                     'p60prid': p60prid, 'p60prnm': p60prnm, 'p60prpi': p60prpi}
        }

    def get_standard_request_id(self, name="", exptime=180):
        """

        :param name:
        :param exptime:
        :return: bool, id
        """
        start = time.time()

        object_id = self.ph_db.get_object_id_from_name(name)
        for obj in object_id:
            if obj[1].lower() == name.lower():
                object_id = obj[0]
                break

        start_date = datetime.datetime.utcnow()
        end_date = start_date + datetime.timedelta(days=1)
        request_dict = {'obs_seq': '{1ifu}',
                        'exptime': '{%s}' % int(exptime),
                        'object_id': object_id,
                        'marshal_id': '-1',
                        'user_id': 2,
                        'allocation_id': '20211011220000019',
                        'priority': '-1',
                        'inidate': start_date.strftime("%Y-%m-%d"),
                        'enddate': end_date.strftime("%Y-%m-%d"),
                        'maxairmass': '2.5',
                        'status': 'PENDING',
                        'max_fwhm': '10',
                        'min_moon_dist': '30',
                        'max_moon_illum': '1',
                        'max_cloud_cover': '1',
                        'seq_repeats': '1',
                        'seq_completed': '0'}
        request_id = self.ph_db.add_request(request_dict)[0]
        return {'elaptime': time.time() - start,
                'data': {'object_id': object_id, 'request_id': request_id}}

    def get_calib_request_id(self, camera='ifu', N=1, object_id="", exptime=0):
        """

        :param camera:
        :param N:
        :param object_id:
        :param exptime:
        :return:
        """
        start = time.time()

        if camera == 'ifu':
            pass
        elif camera == 'rc':
            camera = 'r'
        else:
            return {'request_id': ''}

        start_date = datetime.datetime.utcnow()
        end_date = start_date + datetime.timedelta(days=1)
        request_dict = {'obs_seq': '{%s%s}' % (N, camera),
                        'exptime': '{%s}' % int(exptime),
                        'object_id': object_id,
                        'marshal_id': '-1',
                        'user_id': 2,
                        'allocation_id': '20211011220000019',
                        'priority': '-1',
                        'inidate': start_date.strftime("%Y-%m-%d"),
                        'enddate': end_date.strftime("%Y-%m-%d"),
                        'maxairmass': '2.5',
                        'status': 'PENDING',
                        'max_fwhm': '10',
                        'min_moon_dist': '30',
                        'max_moon_illum': '1',
                        'max_cloud_cover': '1',
                        'seq_repeats': '1',
                        'seq_completed': '0'}

        ret_id = self.ph_db.add_request(request_dict)[0]

        return {'elaptime': time.time() - start, 'data': ret_id}

    def update_request(self, request_id, status="PENDING",
                       check_growth=False):
        """

        :param request_id:
        :param status:
        :param check_growth:
        :return:
        """
        start = time.time()
        ret = self.ph_db.update_request({'id': request_id,
                                         'status': status})

        print(ret)
        if check_growth:
            ret = self.growth.get_marshal_id_from_pharos(request_id)
            print(ret)
            if 'data' in ret:
                ret = self.growth.update_growth_status(growth_id=ret['data'],
                                                       message=status)
                print(ret)
            else:
                return {'elaptime': time.time()-start,
                        'data': "No growth presence"}
        return {'elaptime': time.time()-start, 'data': ret['data']}


if __name__ == "__main__":
    # scheduler_path = '/scr2/sedm/sedmpy/web/static/scheduler/scheduler.html'
    s = time.time()
    sched = Scheduler()
    scheduler_path = sched.params['scheduler_path']
    scheduler_logdir = sched.params['scheduler_logdir']
    # print(x.get_next_observable_target(return_type='json', do_moon_sep=False))
    # time.sleep(111)
    # print(x.simulate_night())
    r = sched.simulate_night(do_focus=True, do_standard=True)
    data = open(scheduler_path, 'w')
    data.write(r)
    data.close()
    now = datetime.datetime.utcnow()
    # are we observing now?
    if sched.obs_times['evening_nautical'] < now < \
            sched.obs_times['morning_astronomical']:
        data = open(os.path.join(scheduler_logdir, "scheduler.%s.html" %
                                 now.strftime("%Y%m%d_%H_%M_%S")), 'w')
        data.write(r)
        data.close()

    # x.update_request(-51465165, "PENDING")
    # print(x.get_standard())
    """z = Time(datetime.datetime.utcnow() + datetime.timedelta(hours=6,
                                                                minutes=15))
    
    #print(x.get_calib_request_id())
    ret = x.get_next_observable_target(do_sort=True, obsdatetime=z, save=True,
                                       update_coords=True, return_type="json")
    print(ret)
    print(x.ph_db)
    r = x.simulate_night()
    data = open(scheduler_path, 'w')
    data.write(r)
    data.close()"""
