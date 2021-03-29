# -*- coding: utf-8 -*-
# ***********************************************************************
# ******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
# *************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
#
#  (c) 2021.                            (c) 2021.
#  Government of Canada                 Gouvernement du Canada
#  National Research Council            Conseil national de recherches
#  Ottawa, Canada, K1A 0R6              Ottawa, Canada, K1A 0R6
#  All rights reserved                  Tous droits réservés
#
#  NRC disclaims any warranties,        Le CNRC dénie toute garantie
#  expressed, implied, or               énoncée, implicite ou légale,
#  statutory, of any kind with          de quelque nature que ce
#  respect to the software,             soit, concernant le logiciel,
#  including without limitation         y compris sans restriction
#  any warranty of merchantability      toute garantie de valeur
#  or fitness for a particular          marchande ou de pertinence
#  purpose. NRC shall not be            pour un usage particulier.
#  liable in any event for any          Le CNRC ne pourra en aucun cas
#  damages, whether direct or           être tenu responsable de tout
#  indirect, special or general,        dommage, direct ou indirect,
#  consequential or incidental,         particulier ou général,
#  arising from the use of the          accessoire ou fortuit, résultant
#  software.  Neither the name          de l'utilisation du logiciel. Ni
#  of the National Research             le nom du Conseil National de
#  Council of Canada nor the            Recherches du Canada ni les noms
#  names of its contributors may        de ses  participants ne peuvent
#  be used to endorse or promote        être utilisés pour approuver ou
#  products derived from this           promouvoir les produits dérivés
#  software without specific prior      de ce logiciel sans autorisation
#  written permission.                  préalable et particulière
#                                       par écrit.
#
#  This file is part of the             Ce fichier fait partie du projet
#  OpenCADC project.                    OpenCADC.
#
#  OpenCADC is free software:           OpenCADC est un logiciel libre ;
#  you can redistribute it and/or       vous pouvez le redistribuer ou le
#  modify it under the terms of         modifier suivant les termes de
#  the GNU Affero General Public        la “GNU Affero General Public
#  License as published by the          License” telle que publiée
#  Free Software Foundation,            par la Free Software Foundation
#  either version 3 of the              : soit la version 3 de cette
#  License, or (at your option)         licence, soit (à votre gré)
#  any later version.                   toute version ultérieure.
#
#  OpenCADC is distributed in the       OpenCADC est distribué
#  hope that it will be useful,         dans l’espoir qu’il vous
#  but WITHOUT ANY WARRANTY;            sera utile, mais SANS AUCUNE
#  without even the implied             GARANTIE : sans même la garantie
#  warranty of MERCHANTABILITY          implicite de COMMERCIALISABILITÉ
#  or FITNESS FOR A PARTICULAR          ni d’ADÉQUATION À UN OBJECTIF
#  PURPOSE.  See the GNU Affero         PARTICULIER. Consultez la Licence
#  General Public License for           Générale Publique GNU Affero
#  more details.                        pour plus de détails.
#
#  You should have received             Vous devriez avoir reçu une
#  a copy of the GNU Affero             copie de la Licence Générale
#  General Public License along         Publique GNU Affero avec
#  with OpenCADC.  If not, see          OpenCADC ; si ce n’est
#  <http://www.gnu.org/licenses/>.      pas le cas, consultez :
#                                       <http://www.gnu.org/licenses/>.
#
#
# ***********************************************************************
import logging
import astropy.table
from astroquery.cadc import Cadc
import re
import io
from cadcutils import net
from cadcdata import CadcDataClient


class Finder:
    """
    Finds all data for a given NIRI stack.
    """

    def __init__(self, state):
        self.state = state
        self.logger = logging.getLogger('{}.{}'.format(
            self.__module__, self.__class__.__name__))
        try:
            self._log_basic_constraints()
        except KeyError as e:
            logging.critical(
                "Insufficient constraints provided; " +
                "is the config file complete?")
            raise e
        self.query_prefix = \
            "SELECT publisherID, productID, energy_bandpassName, " + \
            "time_bounds_lower, time_exposure, type, intent, " + \
            "observationID " + \
            "FROM caom2.Plane AS Plane " + \
            "JOIN caom2.Observation AS Observation " + \
            "ON Plane.obsID = Observation.obsID " + \
            "WHERE Observation.collection = 'GEMINI' " + \
            "AND Observation.instrument_name = 'NIRI' " + \
            "AND Plane.dataProductType = 'image' "
        self.query_suffix = "ORDER BY observationID"

    def run(self):
        """
        Runs the datafinder.

        self._find_objects() gets object frames based on information in
        self.state, and gets additional metadata from those object frames
        about the current stack (things like integration time, filter, etc.)
        This metadata (encoded in self.state) is then used to find appropriate
        calibrations. NIRI needs flat field frames, long darks (darks with
        the same integration time as science frames), and optional short darks
        (1 second darks used to generate a bad pixel mask).
        """
        return astropy.table.vstack([
            self._find_objects(),
            self._segment(self._find_flats()),
            self._segment(self._find_longdarks()),
            self._segment(self._find_shortdarks())
        ])

    def _log_basic_constraints(self):
        """
        Log constraints that must be provided for the program to work.
        """
        self.logger.debug("Stack name: {}".format(
            self.state['current_stack']['obs_name']))

        self.logger.debug("Min required object frames: {}".format(
            self.state['config']['DATAFINDER']['min_objects']))

        self.logger.debug("Min required flat frames: {}".format(
            self.state['config']['DATAFINDER']['min_flats']))

        self.logger.debug("Min required longdark frames: {}".format(
            self.state['config']['DATAFINDER']['min_longdarks']))

        self.logger.debug("Min required shortdark frames: {}".format(
            self.state['config']['DATAFINDER']['min_shortdarks']))

    def _find_objects(self):
        """
        Find OBJECT frames and set metadata used to find calibrations.

        Should be called before any of the find_cal_* methods.
        """
        object_query = self.query_prefix + \
            "AND Observation.observationID LIKE '{}%' ".format(
                self.state['current_stack']['obs_name']) + \
            self.query_suffix

        object_table = self._find_frames(object_query, 'object')

        # Set state based on the returned table.
        # Assuming all rows have same filter/exptime/etc,
        # get information from the first row.
        # Some metadata isn't available from CADC, so use cadc-data
        # to get header for file and set it that way.
        try:
            self.state['current_stack']['filter'] = \
                object_table['energy_bandpassName'][0]
            self.state['current_stack']['exptime'] = \
                object_table['time_exposure'][0]
            self.state['current_stack']['mjd_date'] = \
                (object_table['time_bounds_lower'][0] +
                    object_table['time_bounds_lower'][0])/2
            self.state['current_stack']['camera'] = \
                self._metadata_from_header(
                    object_table[0]['productID']+'.fits', card='CAMERA')
        except Exception as e:
            self.logger.critical("Failed to set stack metadata from objects.")
            raise e

        return object_table

    def _find_flats(self):
        """
        Find flat frames.

        NIRI has three cameras and (I think) we need to make sure the camera
        used in flats matches the camera used for the object frames.
        """
        flat_query = self.query_prefix + \
            "AND Observation.type = 'FLAT' " + \
            "AND Plane.time_bounds_lower >= '{}' ".format(
                self.state['current_stack']['mjd_date'] - 2) + \
            "AND Plane.time_bounds_lower <= '{}' ".format(
                self.state['current_stack']['mjd_date'] + 2) + \
            "AND Plane.energy_bandpassName = '{}' ".format(
                self.state['current_stack']['filter']) + \
            self.query_suffix

        flat_table = self._find_frames(flat_query, 'flat')

        # Exclude flats that don't use same camera as objects
        self.logger.debug("Adding camera information to flats.")
        try:
            cam_column = [self._metadata_from_header(
                x['productID']+'.fits', 'CAMERA') for x in flat_table]
            flat_table.add_column(cam_column, name='camera')
            self.logger.debug("Done getting camera information.")
            mask = (flat_table['camera'] !=
                    self.state['current_stack']['camera'])
            flat_table.remove_rows(mask)
            self.logger.debug("{} frames remain after matching camera.".format(
                len(flat_table)))
        except Exception:
            self.logger.warning(
                "Failed to make sure flats had same camera as objects.")

        return flat_table

    def _find_longdarks(self):
        """
        Find longdark frames.

        Long darks are darks with the same integration time as object frames.
        """
        longdark_query = self.query_prefix + \
            "AND Observation.type = 'DARK' " + \
            "AND Plane.time_bounds_lower >= '{}' ".format(
                self.state['current_stack']['mjd_date'] - 2) + \
            "AND Plane.time_bounds_lower <= '{}' ".format(
                self.state['current_stack']['mjd_date'] + 2) + \
            "AND Plane.time_exposure = '{}' ".format(
                self.state['current_stack']['exptime']) + \
            self.query_suffix

        return self._find_frames(longdark_query, 'longdark')

    def _find_shortdarks(self):
        """
        Short darks are optional darks with integration time ~= 1.0s.

        Used to create a "fresh" bad pixel mask.
        """
        shortdark_query = self.query_prefix + \
            "AND Observation.type = 'DARK' " + \
            "AND Plane.time_bounds_lower >= '{}' ".format(
                self.state['current_stack']['mjd_date'] - 2) + \
            "AND Plane.time_bounds_lower <= '{}' ".format(
                self.state['current_stack']['mjd_date'] + 2) + \
            "AND Plane.time_exposure >= '0.99' " + \
            "AND Plane.time_exposure <= '1.01' " + \
            self.query_suffix

        return self._find_frames(shortdark_query, 'shortdark')

    def _find_frames(self, query, frame_type):
        """
        Do the heavy lifting of finding frames from CADC.

        Tries to find more than "min_objects", "min_flats", etc. of
        each type of frame. Either returns a table (possibly empty)
        when appropriate, or raises an exception if insufficient files
        found.
        """
        self.logger.debug("Finding {} frames.".format(frame_type))
        key = 'min_{}s'.format(frame_type)
        try:
            table = Finder._do_query(query)
        except Exception as e:
            if self.state['config']['DATAFINDER'][key]:
                logging.critical("{} query failed.".format(frame_type))
                raise e
            else:
                self.logger.debug("Found no frames; returning empty table.")
                return astropy.table.Table(names=('publisherID', 'productID'))

        if len(table) < \
                self.state['config']['DATAFINDER'][key]:
            raise RuntimeError("Required {} {} frames; found {}.".format(
                self.state['config']['DATAFINDER'][key],
                frame_type,
                len(table)))
        self.logger.debug("Found {} {} frames.".format(len(table), frame_type))
        return table

    @staticmethod
    def _do_query(query):  # pragma: no cover
        """
        Does a CADC async query.
        """
        cadc = Cadc()
        job = cadc.create_async(query)
        job.run().wait()
        job.raise_if_error()
        return job.fetch_result().to_table()

    def _metadata_from_header(self, productID, card):  # pragma: no cover
        """
        Use cadc-data to get metadata not findable by tap.

        This method is a hack to get around metadata not available in CADC TAP.
        The better solution is to get all metadata possible from CADC TAP.
        There are probably better ways to do this, but the intention is to
        minimize the use of this method.
        """
        anonSubject = net.Subject()
        client = CadcDataClient(anonSubject)
        with io.BytesIO() as f:
            f.name = None
            client.get_file('GEM', productID, f, fhead=True)
            contents = f.getvalue().decode('utf-8')
            if card == 'CAMERA':
                pattern = re.compile(r'(?<=CAMERA  \= \')[^\s]*')
                return pattern.search(contents).group()
            else:
                raise ValueError("Card {} not implemented.".format(card))

    def _segment(self, in_table):
        """
        Narrow a table down to the single observation closest to the object
        observation in time.

        First, compute an observation name column for the table. Then, for
        each observation name, calculate the time difference between it
        and the object frames (use an average here of the first and last
        frames in the observation). Return the frames of that closest
        observation.

        The failure of segmentation might cause downstream errors, so log
        a warning if it fails.
        """
        self.logger.debug(
            "Segmentation starting with {} frames.".format(len(in_table)))
        # Do segmentation based on state['current_stack']['mjd+date']

        # Make a new column with the observation name
        # Test strings:
        #  ivo://cadc.nrc.ca/GEMINI?GN-CAL20190404-10-013/N20190404S0013
        # ivo://cadc.nrc.ca/GEMINI?GN-2019A-FT-108-12-010/N20190405S0120
        #   ivo://cadc.nrc.ca/GEMINI?GN-CAL20190406-8-004/N20190406S0115
        # Results:
        # GN-CAL20190404-10, GN-2019A-FT-108-12, GN-CAL20190406-8
        pattern = re.compile(
            r'.+?(?=-\d\d\d$)')

        new_column = []
        for item in in_table['observationID']:
            try:
                tmp = pattern.search(item).group()
            except TypeError:  # pragma: no cover
                tmp = pattern.search(item.decode('utf-8')).group()
            except AttributeError:
                self.logger.warning(
                    "Unable to parse observationID {}; ".format(item) +
                    "quiting segmentation.")
                return in_table
            new_column.append(tmp)

        new_table = astropy.table.Table([
            new_column,
            in_table['time_bounds_lower']],
            names=['observation_name', 'time_bounds_lower']
        )

        closest_obs_name = None
        closest_time = float('inf')
        for obs_name in set(new_table['observation_name']):
            mask = (new_table['observation_name'] == obs_name)
            time_obs = (max(new_table[mask]['time_bounds_lower']) +
                        min(new_table[mask]['time_bounds_lower'])) / 2
            delta = abs(time_obs - self.state['current_stack']['mjd_date'])
            if delta < closest_time:
                closest_obs_name = obs_name
                closest_time = delta

        if not closest_obs_name:
            self.logger.warning("Segmentation failed.")
            return in_table

        mask = (new_table['observation_name'] == closest_obs_name)
        out_table = in_table[mask]

        self.logger.debug(
            "Segmentation finished with {} frames.".format(len(out_table)))
        return out_table
