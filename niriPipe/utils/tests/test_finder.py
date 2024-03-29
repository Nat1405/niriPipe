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

import unittest
from unittest.mock import patch
import pytest
import astropy.table
import os
import logging
from niriPipe.utils.finder import Finder
import niriPipe.utils.customLogger

THIS_DIR = os.path.dirname(os.path.realpath(__file__))
TESTDATA_DIR = os.path.join(THIS_DIR, 'data')

# Need to enable propagation for log capturing to work
niriPipe.utils.customLogger.enable_propagation()
niriPipe.utils.customLogger.set_level(logging.DEBUG)


def raise_exception():
    """
    Raises an exception.
    """
    raise Exception()


def get_state(
        min_objects=1,
        min_flats=1,
        min_longdarks=1,
        min_shortdarks=1,
        stack_metadata=None,
        max_tries=30
        ):
    """
    Return appliation state dictionary.
    """

    state = {
            'config': {
                'DATAFINDER': {
                    'min_objects': min_objects,
                    'min_flats': min_flats,
                    'min_longdarks': min_longdarks,
                    'min_shortdarks': min_shortdarks,
                    'max_tries': max_tries
                }
            },
            'current_stack': {
                'obs_name': 'GN-XXXXX-X-X-X',
                'bandpass': 'J'
            }
    }

    if stack_metadata:
        for key, value in stack_metadata.items():
            state['current_stack'][key] = value

    return state


class TestFinder(unittest.TestCase):
    """
    Test the Finder class.
    """
    @pytest.fixture(autouse=True)
    def initdir(self, tmpdir):
        tmpdir.chdir()

    @pytest.fixture(autouse=True)
    def inject_fixtures(self, caplog):
        """
        Magic to get log capturing working.
        """
        self._caplog = caplog

    def test_init(self):
        """
        Make sure insufficient state is caught
        """
        state = {}
        with pytest.raises(KeyError):
            Finder(state)

    @patch.object(Finder, '_do_query', side_effect=[
            astropy.table.Table(
                [
                    ['object'],
                    ['ivo://cadc.nrc.ca/GEMINI?GN-CAL20190404-10-013/' +
                        'N20_object_00'],
                    ['GN-CAL20190404-10-013'],
                    [1.0],
                    ['J'],
                    [58000.01],
                    ['GN-CAL20190404']],
                names=[
                    'productID', 'publisherID', 'observationID',
                    'time_exposure', 'energy_bandpassName',
                    'time_bounds_lower', 'proposal_id']),
            astropy.table.Table(
                [
                    ['flat1', 'flat2'],
                    ['ivo://cadc.nrc.ca/GEMINI?GN-CAL20190404-10-013/' +
                        'N20_flat_00',
                        'ivo://cadc.nrc.ca/GEMINI?GN-CAL20190404-12-013/' +
                        'N20_flat_01'],
                    ['GN-CAL20190404-10-013', 'GN-CAL20190404-12-013'],
                    [58000.02, 58000.01],
                ],
                names=[
                    'productID', 'publisherID', 'observationID',
                    'time_bounds_lower']),
            astropy.table.Table(
                [
                    ['longdark'],
                    ['ivo://cadc.nrc.ca/GEMINI?GN-CAL20190404-10-013/' +
                        'N20_longdark_00'],
                    ['GN-CAL20190404-12-013'],
                    [58000.01],
                ],
                names=[
                    'productID', 'publisherID', 'observationID',
                    'time_bounds_lower']),
            astropy.table.Table(
                [
                    ['shortdark'],
                    ['ivo://cadc.nrc.ca/GEMINI?GN-CAL20190404-10-013/' +
                        'N20_shortdark_00'],
                    ['GN-CAL20190404-12-013'],
                    [58000.01],
                ],
                names=[
                    'productID', 'publisherID', 'observationID',
                    'time_bounds_lower']),
        ])
    @patch.object(Finder, '_metadata_from_header',
                  return_value='f6')
    def test_run_flawless(self, mock1, mock2):
        """
        Perfect run that returns the minimum number of required files.

        _do_query() will be called:
            - First for objects
            - Second for flats
            - Third for longdarks
            - Fourth for shortdarks
        """
        self._caplog.clear()

        state = get_state()

        with self._caplog.at_level(logging.DEBUG):
            finder = Finder(state)
            finder.run()

        assert 'object' in self._caplog.text
        assert 'flat' in self._caplog.text
        assert 'longdark' in self._caplog.text
        assert 'shortdark' in self._caplog.text

    @patch.object(Finder, '_find_frames', return_value=None)
    def test_find_objects(self, mock):
        """
        Catch possible failures in _find_objects.
        """
        self._caplog.clear()

        state = get_state()

        finder = Finder(state)
        with self._caplog.at_level(logging.CRITICAL):
            with pytest.raises(Exception):
                finder._find_objects()
        assert 'Failed to set stack metadata' in self._caplog.text

    @patch.object(Finder, '_find_frames', return_value=astropy.table.Table([
            ['flat1', 'flat2'],
            [
                'ivo://cadc.nrc.ca/GEMINI?GN-2019B-FT-108/N20_flat',
                'ivo://cadc.nrc.ca/GEMINI?GN-2019B-FT-108/N20_flat2'
            ],
            ['GN-2019B-FT-108-001', 'GN-2019B-FT-108-002'],
            [58000, 58000]],
            names=['productID', 'publisherID', 'observationID',
                   'time_bounds_lower']))
    @patch.object(Finder, '_metadata_from_header', side_effect=['f-32', 'f-6'])
    def test_find_flats(self, mock1, mock2):
        """
        Make sure flats are comparing camera information.

        Should filter out the first flat, and return the second flat.
        """
        self._caplog.clear()

        state = get_state(stack_metadata={
            'mjd_date': 10000,
            'bandpass': 'J',
            'proposal_id': 'GN-XXXXX-X-X',
            'camera': 'f-6'})

        finder = Finder(state)
        with self._caplog.at_level(logging.DEBUG):
            flats = finder._find_flats()

        assert '1 flat frames remain after matching cam' in self._caplog.text
        assert 'flat1' not in flats['productID']
        assert 'flat2' in flats['productID']

    @patch.object(Finder, '_find_frames', return_value=astropy.table.Table([
            ['foo'],
            ['ivo://cadc.nrc.ca/GEMINI?GN-2019B-FT-108/N20_flat'],
            ['GN-2019B-FT-108-001'],
            [58000]],
            names=['productID', 'publisherID', 'observationID',
                   'time_bounds_lower']))
    @patch.object(Finder, '_metadata_from_header', return_value='f-6')
    def test_find_flats_after_camera(self, mock1, mock2):
        """
        If sufficient flats are found, but then insufficient flats remain
        after selecting those with the same camera as the object frames,
        raise a RuntimeError for insufficient flats.
        """
        self._caplog.clear()

        state = get_state(stack_metadata={
            'mjd_date': 10000,
            'bandpass': 'J',
            'proposal_id': 'GN-XXXXX-X-X',
            'camera': 'f-32'})

        finder = Finder(state)
        with pytest.raises(RuntimeError) as exc_info:
            finder._find_flats()
        assert 'flat frames; found ' in str(exc_info.value)

    def test_find_frames(self):
        """
        Catch various failure modes in _find_frames.
        """
        self._caplog.clear()

        # If CADC TAP raises exception for required frames, raise error.
        state = get_state()

        with self._caplog.at_level(logging.CRITICAL):
            with patch.object(Finder, '_do_query', raise_exception):
                finder = Finder(state)
                with pytest.raises(Exception):
                    finder._find_frames('fake_query', 'object')
        assert 'object query failed' in self._caplog.text

        # Skip finding optional frames.
        state = get_state(min_objects=0)

        with self._caplog.at_level(logging.DEBUG):
            finder = Finder(state)
            finder._find_frames('fake_query', 'object')
        assert 'No object frames requested; skipping query.' in \
            self._caplog.text

        # If TAP returns less frames than required, raise an exception.
        state = get_state(min_objects=3)

        with patch.object(
                Finder, '_do_query', return_value=['1.fits', '2.fits']):
            finder = Finder(state)
            with pytest.raises(RuntimeError) as exc_info:
                finder._find_frames('fake_query', 'object')
        assert 'Required 3 object' in str(exc_info.value)

    @patch('niriPipe.utils.finder.Finder._do_query', raise_exception)
    def test_do_query_exceed_retries(self):
        """
        ... But shouldn't retry forever.
        """
        self._caplog.clear()

        state = get_state()
        finder = Finder(state)
        with self._caplog.at_level(logging.WARNING):
            with pytest.raises(RuntimeError):
                finder._find_frames('fake_query', 'object')

        assert 'attempt 2' in self._caplog.text
        assert 'object query failed.' in self._caplog.text

    def test_segmentation(self):
        """
        Make sure only frames of the closest observation in time to the
        object frames are returned.
        """

        # Empty input table should just return input table
        state = get_state()

        empty_table = astropy.table.Table(
            [[], [], [], []],
            names=['productID', 'publisherID', 'observationID',
                   'time_bounds_lower'])

        self._caplog.clear()
        with self._caplog.at_level(logging.DEBUG):
            finder = Finder(state)
        assert len(finder._segment(empty_table)) == 0
        assert 'skipping segmentation.' in self._caplog.text

        # A one-row table should just be returned
        state = get_state(stack_metadata={'mjd_date': 57000})

        one_row_table = astropy.table.Table([
            ['foo'],
            ['ivo://cadc.nrc.ca/GEMINI?GN-CAL20190404-10-013/N20_shortdark'],
            ['GN-CAL20190404-10-013'],
            [58000]],
            names=['productID', 'publisherID', 'observationID',
                   'time_bounds_lower'])

        finder = Finder(state)
        assert len(finder._segment(one_row_table)) == 1

        # We need to compute an observation name column to do segmentation.
        # If that fails, log a warning because this might break downstream
        # processing.
        state = get_state(stack_metadata={'mjd_date': 57000})

        bad_one_row_table = astropy.table.Table([
            ['foo'],
            ['ivo://bad_publisher_id'],
            ['GN-FOO'],
            [58000]],
            names=['productID', 'publisherID', 'observationID',
                   'time_bounds_lower'])

        finder = Finder(state)
        self._caplog.clear()
        with self._caplog.at_level(logging.WARNING):
            finder = Finder(state)
        assert len(finder._segment(bad_one_row_table)) == 1
        assert 'Unable to parse observationID' in self._caplog.text

    def test_mark_as(self):
        """
        _mark_as adds an extra column to tables to identify frames
        as object, flat, longdark, or shortdark.

        The possible flows are:
            - empty table provided; add column but no rows and return table.
            - n-row table; add column and rows with type.
        """
        state = get_state()
        finder = Finder(state)
        # Empty table test
        empty_table = astropy.table.Table(
            [[], [], [], []],
            names=['productID', 'publisherID', 'observationID',
                   'time_bounds_lower'])
        out_table = finder._mark_as('flat', empty_table)
        assert 'niriPipe_type' in out_table.columns

        two_row_table = astropy.table.Table([
            ['foo', 'bar'],
            ['ivo://bad_publisher_id', 'bad_id_2'],
            ['GN-FOO', 'GN-BAR'],
            [58000, 10000]],
            names=['productID', 'publisherID', 'observationID',
                   'time_bounds_lower'])

        two_row_table = finder._mark_as('object', two_row_table)
        assert two_row_table[1]['niriPipe_type'] == 'object'


