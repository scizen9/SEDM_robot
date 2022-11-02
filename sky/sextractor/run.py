# import datetime
import os
import glob
import time
from astropy.io import ascii, fits
import json
import numpy as np
import subprocess
import shutil
from matplotlib import pylab as plt
from utils import rc_focus

SITE_ROOT = os.path.abspath(os.path.dirname(__file__)+'/../..')


class sextractor:
    def __init__(self, config=None):
        """

        :param config:
        """
        if not config:
            self.config = os.path.join(SITE_ROOT, 'config',
                                       'sedm_sextractor_config',
                                       'sextractor_config.json')
        else:
            if os.path.exists(config):
                self.config = config
            else:
                self.config = os.path.join(SITE_ROOT, 'config',
                                           'sedm_sextractor_config',
                                           'sextractor_config.json')

        with open(self.config) as data_file:
            params = json.load(data_file)

        self.sex_exec = params["exec_path"]
        self.default_config = params["config_file"]
        self.arc_config = params["arc_config_file"]
        self.default_cat_path = params["default_path"]
        self.run_sex_cmd = "%s -c %s " % (self.sex_exec, self.default_config)
        self.run_arc_sex_cmd = "%s -c %s " % (self.sex_exec, self.arc_config)

        self.y_max = 2000
        self.y_min = 50

    def run(self, input_image, output_file=None, save_in_seperate_dir=True,
            output_type=None, create_region_file=True, overwrite=False,
            arc=False):

        """

        :param input_image:
        :param output_file:
        :param save_in_seperate_dir:
        :param output_type:
        :param create_region_file:
        :param overwrite:
        :param arc:
        :return:
        """
        if output_type:
            pass
        start = time.time()

        # 1. Start by making sure the input file exists
        if not os.path.exists(input_image):
            return {"elaptime": time.time()-start,
                    "error": "%s does not exists"}

        # 2. If no output file is given then we append to the original file
        # name
        if not output_file:
            if save_in_seperate_dir:
                base_path = os.path.dirname(input_image)
                base_name = os.path.basename(input_image)
                save_path = os.path.join(base_path, "sextractor_catalogs")
                if not os.path.exists(save_path):
                    os.mkdir(save_path)
                output_file = os.path.join(save_path, base_name+'.cat')
            else:
                output_file = input_image + '.cat'

        if not overwrite:
            if os.path.exists(output_file):
                return {"elaptime": time.time()-start,
                        "data": output_file}

        # 3. If we made it here then it's time to run the command.

        # Let's just make sure there are no old files in place
        if os.path.exists(self.default_cat_path):
            os.remove(self.default_cat_path)

        if arc:
            print("sex.run - running sextractor on arc image")
            run_sex_cmd = self.run_arc_sex_cmd
        else:
            print("sex.run - running sextractor on star image")
            run_sex_cmd = self.run_sex_cmd
        # Run the sextractor command
        try:
            subprocess.call("%s %s" % (run_sex_cmd, input_image),
                            stdout=subprocess.DEVNULL, shell=True)
        except:
            time.sleep(10)
            subprocess.call("%s %s" % (run_sex_cmd, input_image),
                            stdout=subprocess.DEVNULL, shell=True)
            pass

        # 4. If everything ran successfully
        # we should have a new file called image.cat
        if not os.path.exists(self.default_cat_path):
            return {'elaptime': time.time()-start,
                    'error': "Unable to run the sextractor command"}
        print("sex.run - putting catalog in", output_file)
        shutil.move(self.default_cat_path, output_file)

        if create_region_file:
            reg_file = output_file + '.reg'
            cat_data = ascii.read(output_file)
            df = cat_data.to_pandas()
            rf = open(reg_file, 'w')
            for ind in df.index:
                rf.write("circle(%s, %s, %s\n" % (df["X_IMAGE"][ind],
                                                  df["Y_IMAGE"][ind], 10))
            rf.close()
            print("sex.run - created region file", reg_file)
        return {"elaptime": time.time()-start,
                "data": output_file}

    def filter_arc_catalog(self, catalog, create_region_file=True, radius=10):

        start = time.time()

        if not os.path.exists(catalog):
            return {"elaptime": time.time()-start,
                    "error": "%s does not exist" % catalog}
        cdata = ascii.read(catalog)

        df = cdata.to_pandas()
        df = df[(df['Y_IMAGE'] < self.y_max) & (df['Y_IMAGE'] > self.y_min)]
        df = df[(df['FLAGS'] <= 0)]
        # cut on size
        size_cut_up = df['B_IMAGE'].median() + 2.5 * df['B_IMAGE'].std()
        size_cut_lo = df['B_IMAGE'].median() - 2.5 * df['B_IMAGE'].std()
        df = df[(size_cut_lo < df['B_IMAGE'] < size_cut_up)]

        if create_region_file:
            reg_file = catalog + '.reg'
            cdata = open(reg_file, 'w')
            for ind in df.index:
                cdata.write("circle(%s, %s, %s\n" % (df["X_IMAGE"][ind],
                                                     df["Y_IMAGE"][ind],
                                                     radius))
            cdata.close()
            print("sex.filter_arc_catalog - ds9 region file:", reg_file)

        return {"elaptime": time.time()-start, "data": df}

    def get_arc_fwhm(self, obs, overwrite=True, filter_catalog=True,
                     catalog_field='B_IMAGE'):
        # run sextractor
        sret = self.run(obs, overwrite=overwrite, arc=True)
        print("sex.run status:\n", sret)

        # 5. Check that there were no errors
        if 'error' in sret:
            print("sex.get_arc_fwhm - sextractor error for", obs)
            return None, None

        # 6. Filter the data if requested
        if filter_catalog:
            sret = self.filter_arc_catalog(sret['data'])
            print("sex.filter_star_catalog filtered")

        # 7. Again check there were no errors
        if 'error' in sret:
            print("sex.get_arc_fwhm - filter_arc_catalog error:\n", sret)
            return None, None

        # 8. Now we get the mean values for the catalog
        df = sret['data']
        if df.empty:
            print("sex.get_arc_fwhm - no data for", obs)
            return None, None

        # 9. Finally get the stats for the image
        print("sex.get_arc_fwhm - number of sources:", len(df.index))
        fwhm = df[catalog_field].median()
        fwhm_std = df.loc[:, catalog_field].std()

        return fwhm, fwhm_std

    def filter_star_catalog(self, catalog, mag_quantile=.8, ellp_quantile=.25,
                            create_region_file=True, radius=10):

        start = time.time()

        if not os.path.exists(catalog):
            return {"elaptime": time.time()-start,
                    "error": "%s does not exist" % catalog}
        cdata = ascii.read(catalog)

        df = cdata.to_pandas()
        mag = df['MAG_BEST'].quantile(mag_quantile)
        ellip = df['ELLIPTICITY'].quantile(ellp_quantile)
        df = df[(df['MAG_BEST'] < mag) & (df['ELLIPTICITY'] < ellip)]
        df = df[(df['Y_IMAGE'] < self.y_max) & (df['Y_IMAGE'] > self.y_min)]
        df = df[(df['FLAGS'] <= 0)]
        # cut on size
        size_cut = df['FWHM_IMAGE'].median() + 2.5 * df['FWHM_IMAGE'].std()
        df = df[(df['FWHM_IMAGE'] < size_cut)]

        if create_region_file:
            reg_file = catalog + '.reg'
            cdata = open(reg_file, 'w')
            for ind in df.index:
                cdata.write("circle(%s, %s, %s\n" % (df["X_IMAGE"][ind],
                                                     df["Y_IMAGE"][ind],
                                                     radius))
            cdata.close()
            print("sex.filter_star_catalog - ds9 region file:", reg_file)

        return {"elaptime": time.time()-start, "data": df}

    def _reject_outliers(self, indata, m=.5):
        return indata[abs(indata - np.mean(indata)) < m * np.std(indata)]

    def get_star_fwhm(self, catalog, do_filter=True, ellip_constraint=.2,
                      create_region_file=True):

        # Read in the sextractor catalog and convert to dataframe
        if catalog[-4:] == 'fits':
            cret = self.run(catalog)
            # print(ret)
            if 'data' in cret:
                catalog = cret['data']
        # print(catalog)
        cdata = ascii.read(catalog)

        df = cdata.to_pandas()
        avgfwhm = 0
        print("sex.get_fwhm")
        if do_filter:
            df = df[(df['X_IMAGE'] > 250) & (df['X_IMAGE']) < 4000]
            df = df[(df['Y_IMAGE'] > 250) & (df['Y_IMAGE']) < 3400]
            df = df[(df['FLAGS'] == 0) & (df['ELLIPTICITY'] < ellip_constraint)]

            d = self._reject_outliers(df['FWHM_IMAGE'].values)
            avgfwhm = np.median(d) * .49

            print("sex.get_fwhm - avgfwhm:", avgfwhm)

            df = df.sort_values(by=['MAG_BEST'])
            df = df[0:5]

            if create_region_file:
                reg_file = catalog + '.reg'
                rdata = open(reg_file, 'w')
                for ind in df.index:
                    rdata.write("circle(%s, %s, %s\n" % (df["X_IMAGE"][ind],
                                                         df["Y_IMAGE"][ind],
                                                         20))
                rdata.close()
                print("sex.get_fwhm - region file:", reg_file)
        print("sex.get_fwhm - FWHM_IMAGE values:\n", df['FWHM_IMAGE'].values)
        fwhm = np.median(df['FWHM_IMAGE'].values) * .49
        print("sex.get_fwhm - median fwhm (asec):", fwhm)

        return avgfwhm

    def run_loop(self, obs_list, header_field='FOCPOS',
                 header_field_temp='IN_AIR', overwrite=False,
                 catalog_field='FWHM_IMAGE', nominal_focus=None,
                 filter_catalog=True, save_catalogs=True, ):
        """

        :param obs_list: (list) list of image files to analyze
        :param header_field: (str) header kwd to track
        :param header_field_temp: (str) header kwd for temperature
        :param overwrite: (bool) overwrite previous sextractor runs?
        :param catalog_field: (str) field in sextractor catalog
        :param nominal_focus: (float) focus determined from temperature only
        :param filter_catalog: (bool) filter the catalog?
        :param save_catalogs: (bool) save catalogs?
        :return:
        """
        if save_catalogs:
            pass
        start = time.time()
        header_field_list = []
        catalog_field_list = []
        error_list = []
        pltdir = None

        # 1. Start by looping through the image list
        for obs in obs_list:
            # 2. Before preforming any analysis do a sanity check to make
            # sure the file exists
            if not os.path.exists(obs):
                print("sex.run_loop - image not found:", obs)
                header_field_list.append(np.NaN)
                catalog_field_list.append(np.NaN)
                error_list.append(np.NaN)
                continue

            # 2.5 Get plot directory
            if pltdir is None:
                pltdir = os.path.dirname(os.path.abspath(obs))

            # 3. Now open the file and get the header information
            try:
                header_field_list.append(
                    float(fits.getheader(obs)[header_field]))
            except:
                print("sex.run_loop - no hdr kwd:", header_field)
                header_field_list.append(np.NaN)
                catalog_field_list.append(np.NaN)
                error_list.append(np.NaN)
                continue

            # 4. We should now be ready to run sextractor
            sret = self.run(obs, overwrite=overwrite)
            print("sex.run status:\n", sret)

            # 5. Check that there were no errors
            if 'error' in sret:
                print("sex.run_loop - sextractor error for", obs)
                header_field_list.append(np.NaN)
                catalog_field_list.append(np.NaN)
                error_list.append(np.NaN)
                continue

            # 6. Filter the data if requested
            if filter_catalog:
                sret = self.filter_star_catalog(sret['data'])
                print("sex.filter_star_catalog filtered")

            # 7. Again check there were no errors
            if 'error' in sret:
                print("sex.run_loop - filter_star_catalog error:\n", sret)
                header_field_list.append(np.NaN)
                catalog_field_list.append(np.NaN)
                error_list.append(np.NaN)
                continue

            # 8. Now we get the mean values for the catalog
            df = sret['data']
            if df.empty:
                print("sex.run_loop - no data for", obs)
                header_field_list.append(np.NaN)
                catalog_field_list.append(np.NaN)
                error_list.append(np.NaN)
                continue

            # 9. Finally get the stats for the image
            print("sex.run_loop - number of sources:", len(df.index))
            catalog_field_list.append(df[catalog_field].median())
            error_list.append(df.loc[:, catalog_field].std())

        catalog = np.array(catalog_field_list)
        header = np.array(header_field_list)
        std_catalog = np.array(error_list)

        current_temp = float(fits.getheader(obs_list[-1])[header_field_temp])
        if nominal_focus is None:
            mod_foc = rc_focus.temp_to_focus(current_temp)
        else:
            mod_foc = nominal_focus

        n = len(catalog)

        # test catalog for nans
        n_good = np.where(np.isfinite(catalog))[0].shape[0]
        print("sex.run_loop - N pts, N good:", n, n_good)
        print("sex.run_loop - catalog values:\n", catalog)

        if n_good > 5:
            best_seeing_id = np.nanargmin(catalog)
            print("FWHMS: %s\n focpos: %s\n Best seeing id: %d\n "
                  % (catalog, header, int(best_seeing_id)))

            std_catalog = np.maximum(1e-5, np.array(std_catalog))

            coefs = np.polyfit(header, catalog, w=1 / std_catalog, deg=2)

            xfp = np.linspace(np.min(header), np.max(header), 1000)
            p = np.poly1d(coefs)
            best = xfp[np.argmin(p(xfp))]

            print("Best fit focus:%.2f" % best)
            # make plot and save
            tstamp = time.strftime("%Y%m%d_%H_%M_%S", time.gmtime())
            pltfile = os.path.join(pltdir, 'rcfocus%s.png' % tstamp)
            plt.plot(header, catalog, 'b+')
            plt.plot(xfp, p(xfp))
            plt.axvline(x=best)
            plt.axvline(x=mod_foc, c='g')
            plt.xlabel(header_field)
            plt.ylabel(catalog_field)
            plt.title("Best Fit RC Focus: %.2f \n"
                      "Thermal Model Focus: %.2f at %.2f deg"
                      % (best, mod_foc, current_temp))
            plt.savefig(pltfile)
            plt.clf()

            if (mod_foc - 0.1) <= best <= (mod_foc + 0.1):
                pass
            else:
                print("Fit value outside model range, using model value")
                best = mod_foc
        else:
            print("sex.run_loop - not enough good values, using nominal focus")
            best = mod_foc
            coefs = [0, 0]

        return {'elaptime': time.time()-start,
                'data': [[best], coefs[0]]}


if __name__ == "__main__":
    x = sextractor()
    data_list = sorted(glob.glob("/scr2/bigscr_rsw/guider_images/*.fits"))

    data = open('test.csv', 'w')
    data.write("image, fwhm\n")
    for i in data_list:
        ret = x.get_star_fwhm(i)
        data.write("%s,%s\n" % (i, ret))
    data.close()
    # print(data_list[0])
    # print(x.get_catalog_positions(data_list[2]))
    # print(x.run(data_list[0], overwrite=True))
    # ret = x.run_loop(data_list, overwrite=False)
    # print(ret)
