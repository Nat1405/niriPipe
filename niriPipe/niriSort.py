from astropy.table import vstack, Table
from astroquery.cadc import Cadc
import logging
from niriPipe.niriUtils import getFile
import numpy as np
import os


class NIRIData:
    """The starting point for a data reduction: getting the needed raw data."""

    def __init__(self):
        pass

    def findDataForObservation(self, observationNumber):
        """Gets all raw frames needed for a particular NIRI observation number.
        observationNumber (str): something like GN-2018A-DD-109-15
        """
        # Check observationNumber is valid
        if len(observationNumber.split('-')) != 5:
            raise ValueError('Observation number {} is badly formed.'.format(observationNumber))

        try:
            science_table = self.findObjects(observationNumber)
            if not science_table:
                raise FileNotFoundError()
        except Exception as e:
            logging.error("Problem finding object frames for %.".format(observationNumber))
            raise e
        try:
            flats_table = self.findFlats(science_table)
            long_darks_table = self.findLongDarks(science_table)
            short_darks_table = self.findShortDarks(science_table)
            if not flats_table or not long_darks_table or not short_darks_table:
                raise FileNotFoundError()
        except Exception as e:
            logging.error("Problem finding object frames for %.".format(observationNumber))
            raise e

        return vstack([science_table, flats_table, long_darks_table, short_darks_table], join_type='exact')



    def findObjects(self, observationNumber):
        """Objects are simply those frames that belong to
        the observation number."""
        cadc = Cadc()
        query = "SELECT productID, type, time_exposure, energy_bandpassName, time_bounds_lower, publisherID " \
                "FROM caom2.Plane AS Plane " \
                "JOIN caom2.Observation AS Observation ON Plane.obsID = Observation.obsID " \
                "WHERE (Observation.collection = 'GEMINI' " \
                "AND Observation.instrument_name = 'NIRI' " \
                "AND Plane.dataProductType = 'image' " \
                "AND Observation.observationID LIKE '{}%') " \
                "ORDER BY productID".format(observationNumber)
        job = cadc.create_async(query)
        job.run().wait()
        job.raise_if_error()
        result = job.fetch_result().to_table()
        return result

    def findFlats(self, science_table):
        """Flats are within 24 hours of science frames.
            Time is calculated by getting the average mjd obs of a science
            observation, then going +- half a day. 
        """

        FLAT_THRESHOLD = 0.5

        avg_time = np.average(science_table['time_bounds_lower'])
        filters = list(set(science_table['energy_bandpassName']))

        filters = ["Plane.energy_bandpassName = '{}'".format(x) for x in filters]
        filter_snippet = " OR ".join(filters)

        cadc = Cadc()
        query = "SELECT productID, type, time_exposure, energy_bandpassName, time_bounds_lower, publisherID " \
                "FROM caom2.Plane AS Plane " \
                "JOIN caom2.Observation AS Observation ON Plane.obsID = Observation.obsID " \
                "WHERE (Observation.collection = 'GEMINI' " \
                "AND Observation.instrument_name = 'NIRI' " \
                "AND Plane.dataProductType = 'image' " \
                "AND Observation.type = 'FLAT' " \
                "AND Plane.time_bounds_lower BETWEEN {} - {} AND {} + {} " \
                "AND ( {} ) ) " \
                "ORDER BY productID".format(
                                        avg_time,
                                        FLAT_THRESHOLD,
                                        avg_time,
                                        FLAT_THRESHOLD,
                                        filter_snippet
                )
        job = cadc.create_async(query)
        job.run().wait()
        job.raise_if_error()
        result = job.fetch_result().to_table()
        return result

    def findLongDarks(self, science_table):
        DARK_THRESHOLD = 0.5
        EXP_TIME_DELTA = 0.01

        avg_time = np.average(science_table['time_bounds_lower'])

        exp_times = list(set(science_table['time_exposure']))

        exp_times = ["Plane.time_exposure BETWEEN {} AND {}".format(
            exp_time - EXP_TIME_DELTA, exp_time + EXP_TIME_DELTA)
            for exp_time in exp_times]

        exp_times_snippet = " OR ".join(exp_times)

        cadc = Cadc()
        query = "SELECT productID, type, time_exposure, energy_bandpassName, time_bounds_lower, publisherID " \
                "FROM caom2.Plane AS Plane " \
                "JOIN caom2.Observation AS Observation ON Plane.obsID = Observation.obsID " \
                "WHERE (Observation.collection = 'GEMINI' " \
                "AND Observation.instrument_name = 'NIRI' " \
                "AND Plane.dataProductType = 'image' " \
                "AND Observation.type = 'DARK' " \
                "AND Plane.time_bounds_lower BETWEEN {} - {} AND {} + {} " \
                "AND ( {} ) ) " \
                "ORDER BY productID".format(
                                        avg_time,
                                        DARK_THRESHOLD,
                                        avg_time,
                                        DARK_THRESHOLD,
                                        exp_times_snippet
                )
        job = cadc.create_async(query)
        job.run().wait()
        job.raise_if_error()
        result = job.fetch_result().to_table()
        return result

    def findShortDarks(self, science_table):
        DARK_THRESHOLD = 0.5
        UPPER_SHORT_DARK_BOUND = 10
        avg_time = np.average(science_table['time_bounds_lower'])

        cadc = Cadc()
        query = "SELECT productID, type, time_exposure, energy_bandpassName, time_bounds_lower, publisherID " \
                "FROM caom2.Plane AS Plane " \
                "JOIN caom2.Observation AS Observation ON Plane.obsID = Observation.obsID " \
                "WHERE (Observation.collection = 'GEMINI' " \
                "AND Observation.instrument_name = 'NIRI' " \
                "AND Plane.dataProductType = 'image' " \
                "AND Observation.type = 'DARK' " \
                "AND Plane.time_bounds_lower BETWEEN {} - {} AND {} + {} " \
                "AND Plane.time_exposure BETWEEN 0 AND {} ) " \
                "ORDER BY productID".format(
                    avg_time,
                    DARK_THRESHOLD,
                    avg_time,
                    DARK_THRESHOLD,
                    UPPER_SHORT_DARK_BOUND
                )
        job = cadc.create_async(query)
        job.run().wait()
        job.raise_if_error()
        result = job.fetch_result().to_table()
        return result


    def downloadTable(self, table, directory=os.getcwd()):
        """Downloads the results of a CADC query."""
        cadc = Cadc()
        urls = cadc.get_data_urls(table)
        pids = list(table['productID'])
        cwd = os.getcwd()
        os.chdir(directory)
        for url, pid in zip(urls, pids):
            try:
                filename = getFile(url)
                logging.info("Downloaded {}".format(filename))
            except Exception as e:
                logging.error("{} failed to download.".format(pid))
                os.chdir(cwd)
                raise e
        os.chdir(cwd)










