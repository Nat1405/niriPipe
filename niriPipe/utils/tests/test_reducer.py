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
from pathlib import Path
import logging
from niriPipe.utils.reducer import Reducer
from niriPipe.utils.state import get_initial_state
import niriPipe.utils.customLogger
import shutil

THIS_DIR = os.path.dirname(os.path.realpath(__file__))
TESTDATA_DIR = os.path.join(THIS_DIR, 'data')

# Need to enable propagation for log capturing to work
# Note that DRAGONS messes with the root logger, so can't
# check DEBUG log messages in pytests.
niriPipe.utils.customLogger.enable_propagation()
niriPipe.utils.customLogger.set_level(logging.DEBUG)


def get_state_table(
        intent=['science'],
        min_objects=None,
        min_flats=None,
        min_longdarks=None,
        min_shortdarks=None,
        populated_table=True):
    """
    The Reducer takes state and an input table of files as input.

    State should be loaded from the default config file, with
    some state set specifically for each test.

    The table should have appropriate columns and be either empty
    or populated with the minumum number of rows.
    """
    state = get_initial_state(
            obs_name=['GN-FOO-BAR'],
            intent=intent,
            configfile=None)

    # Override default configuration here
    if min_objects:
        state['config']['DATAFINDER']['min_objects'] = min_objects
    if min_flats:
        state['config']['DATAFINDER']['min_flats'] = min_flats
    if min_longdarks:
        state['config']['DATAFINDER']['min_longdarks'] = min_longdarks
    if min_shortdarks:
        state['config']['DATAFINDER']['min_shortdarks'] = min_shortdarks

    if populated_table:
        table = astropy.table.Table([[
            'ivo://cadc.nrc.ca/GEMINI?GN-2019A-FT-108-12-001/N20190405S0111',
            'ivo://cadc.nrc.ca/GEMINI?GN-2019A-FT-108-16-001/N20190406S0007',
            'ivo://cadc.nrc.ca/GEMINI?GN-2019A-FT-108-16-036/N20190406S0042',
            'ivo://cadc.nrc.ca/GEMINI?GN-CAL20190406-8-001/N20190406S0112', ],
            [
                'N20190405S0111',
                'N20190406S0007',
                'N20190406S0042',
                'N20190406S0112',
            ],
            [
                'object',
                'flat',
                'longdark',
                'shortdark'
            ]],
            names=['publisherID', 'productID', 'niriPipe_type']
        )
    else:
        table = astropy.table.Table([[]], names=['niriPipe_type'])

    return state, table


def raise_exception():
    raise RuntimeError("Fake exception...")


class MockReduce:
    """
    Mocks the main dragons REDUCE class
    (recipe_system.reduction.coreReduce.Reduce)
    """
    def __init__(self):
        self.files = []
        self.uparms = []
        self.recipename = ""

    def runr(self):
        self.output_filenames = ['fake_file.fits']


class TestReducer(unittest.TestCase):
    """
    Test the Reducer class.
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

    def test_init_dragons_good(self):
        """
        Make sure dragons init works.

        Assume raw data path has been created.

        Should:
            - Output success message
            - Have written to config file
            - Have created caldb
        """
        state, _ = get_state_table()

        if os.path.exists(os.path.join(str(Path.home()), '.geminidr')):
            shutil.rmtree(os.path.join(str(Path.home()), '.geminidr'))

        os.mkdir(os.path.join(
                os.getcwd(),
                state['config']['DATARETRIEVAL']['raw_data_path']))

        reducer = Reducer(state=state, table=None)
        with self._caplog.at_level(logging.DEBUG):
            reducer._init_dragons()
        assert os.stat(os.path.join(
                str(Path.home()),
                '.geminidr',
                'rsys.cfg'
            )).st_size > 0
        assert os.stat(os.path.join(
                os.getcwd(),
                state['config']['DATARETRIEVAL']['raw_data_path'],
                'cal_manager.db'
            )).st_size > 0

    def test_init_with_no_reduction(self):
        """
        Should be able to not make any products.
        """
        state, table = get_state_table(
            min_objects='0',
            min_flats='0',
            min_longdarks='0',
            min_shortdarks='0',
            populated_table=False
        )

        reducer = Reducer(state=state, table=table)
        products = reducer.run()

        assert not any([
            products['processed_dark'],
            products['processed_bpm'],
            products['processed_flat'],
            products['processed_stack']
        ])

    @patch('recipe_system.reduction.coreReduce.Reduce', MockReduce)
    def test_perfect_reduction(self):
        """
        Mock DRAGONS to do a perfect reduction.
        """
        state, table = get_state_table(
            intent=['calibration'],
            min_longdarks='1',
            min_shortdarks='1'
        )

        reducer = Reducer(state=state, table=table)
        with patch('recipe_system.cal_service.CalibrationService.add_cal'):
            products = reducer.run()

        assert all([
            products['processed_dark'] == 'fake_file.fits',
            products['processed_bpm'] == 'fake_file.fits',
            products['processed_flat'] == 'fake_file.fits',
            products['processed_stack'] == 'fake_file.fits'
        ])

    @patch('recipe_system.reduction.coreReduce.Reduce', raise_exception)
    @patch('gempy.utils.logutils')
    def test_reduction_exception(self, mock):
        """
        If the dragons reducer throws an exception, report it and exit.
        """
        self._caplog.clear()

        state, table = get_state_table(
            intent=['science']
        )

        reducer = Reducer(state=state, table=table)
        with pytest.raises(RuntimeError):
            with self._caplog.at_level(logging.DEBUG):
                reducer.run()

        assert 'Failed to make processed dark.' in self._caplog.text

    """
    Make product has four use cases along with tweakable parameters.

        - Make the processed long dark.
        - Make the processed bad pixel mask.
        - Make the processed flat frame.
        - Make the object stack.

    The bad pixel mask might not exist, so needs to work both with and
    without a bad pixel mask.

    The dark correction needs turned off for standard star stacks.

    Only the processed flat and dark should be added to the DRAGONS caldb.
    """

    def test_make_product_skip(self):
        self._caplog.clear()

        state, table = get_state_table(
            min_objects='0'
        )

        reducer = Reducer(state=state, table=table)
        reducer.products = {'processed_bpm': 'pretend_bpm.fits'}
        with self._caplog.at_level(logging.DEBUG):
            reducer._make_product(
                frame_type='object',
                mask=(table['niriPipe_type'] == 'object'),
                product_name='processed_stack'
            )

        assert 'Skipping creation of processed_stack' in self._caplog.text

    @patch('recipe_system.reduction.coreReduce.Reduce', MockReduce)
    def test_make_product_recipe_name(self):
        """
        Should be able to pass a custom recipe name.
        """
        self._caplog.clear()

        state, table = get_state_table()

        reducer = Reducer(state=state, table=table)
        reducer.products = {'processed_bpm': 'pretend_bpm.fits'}
        with self._caplog.at_level(logging.DEBUG):
            reducer._make_product(
                frame_type='object',
                mask=(table['niriPipe_type'] == 'object'),
                product_name='processed_stack',
                recipename='fake_recipe.py',
                add_to_cal_db=False
            )

        assert 'Using provided recipe: fake_recipe.py' in self._caplog.text

    @patch('recipe_system.reduction.coreReduce.Reduce', MockReduce)
    def test_make_product_bad_pixel_mask(self):
        """
        Custom bad pixel mask should be provided if it exists.
        """
        self._caplog.clear()

        state, table = get_state_table()

        reducer = Reducer(state=state, table=table)
        reducer.products = {'processed_bpm': 'pretend_bpm.fits'}
        with self._caplog.at_level(logging.DEBUG):
            reducer._make_product(
                frame_type='flat',
                mask=(table['niriPipe_type'] == 'flat'),
                product_name='processed_flat',
                add_to_cal_db=False
            )

        assert 'Using provided bad pixel mask: pretend_bpm.fits' in \
            self._caplog.text

    @patch('recipe_system.reduction.coreReduce.Reduce', MockReduce)
    def test_make_product_turn_off_dark(self):
        """
        Dark correction should be turned off for standard star stack.
        """
        self._caplog.clear()

        state, table = get_state_table(intent=['calibration'])

        print(state['config']['DATAFINDER']['min_longdarks'])

        reducer = Reducer(state=state, table=table)
        reducer.products = {'processed_bpm': None}
        with self._caplog.at_level(logging.DEBUG):
            reducer._make_product(
                frame_type='object',
                mask=(table['niriPipe_type'] == 'object'),
                product_name='processed_stack',
                add_to_cal_db=False
            )

        assert 'Turning off dark correction' in self._caplog.text
