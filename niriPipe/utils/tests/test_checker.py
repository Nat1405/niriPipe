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
import os
import logging
from niriPipe.utils.checker import Checker
import niriPipe.utils.customLogger

THIS_DIR = os.path.dirname(os.path.realpath(__file__))
TESTDATA_DIR = os.path.join(THIS_DIR, 'data')

# Need to enable propagation for log capturing to work
# Note that DRAGONS messes with the root logger, so can't
# check DEBUG log messages in pytests.
niriPipe.utils.customLogger.enable_propagation()
niriPipe.utils.customLogger.set_level(logging.DEBUG)


def raise_exception(*args, **kwargs):
    """
    Raises an exception.
    """
    raise IOError('not found')


def get_state(
            min_objects='1',
            min_flats='1',
            min_longdarks='1',
            min_shortdarks='1'):
    state = {
        'config': {
            'DATAFINDER': {
                'min_objects': min_objects,
                'min_flats': min_flats,
                'min_longdarks': min_longdarks,
                'min_shortdarks': min_shortdarks
            }
        }
    }
    return state


def get_products(
        processed_flat='/fake/path/N2019_flat.fits',
        processed_dark='/fake/path/N2019_dark.fits',
        processed_bpm='/fake/path/N2019_bpm.fits',
        processed_stack='/fake/path/N2019_stack.fits'):
    """
    The Checker takes a dictionary of reduction products as input,
    and raises an error if something goes wrong.
    """

    products = {
        'processed_flat': processed_flat,
        'processed_dark': processed_dark,
        'processed_bpm': processed_bpm,
        'processed_stack': processed_stack
    }

    return products


class TestChecker(unittest.TestCase):
    """
    Test the Checker class.
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

    def test_checker_skip_all_pass(self):
        """
        If no products required and nothing on disk, check should pass.
        """
        self._caplog.clear()

        state = get_state(
                min_objects='0',
                min_flats='0',
                min_longdarks='0',
                min_shortdarks='0'
        )

        products = get_products(
            processed_stack=None,
            processed_flat=None,
            processed_dark=None,
            processed_bpm=None
        )

        checker = Checker(state=state, products=products)
        with self._caplog.at_level(logging.DEBUG):
            checker.run()

        assert 'Skipping checking of processed_stack' in self._caplog.text
        assert 'Skipping checking of processed_flat' in self._caplog.text
        assert 'Skipping checking of processed_dark' in self._caplog.text
        assert 'Skipping checking of processed_bpm' in self._caplog.text

    def test_checker_skip_all_fail(self):
        """
        If no products required, and products are still present, something
        weird might be going on. Checker should raise an exception.
        """
        self._caplog.clear()

        state = get_state(
                min_objects='0',
                min_flats='0',
                min_longdarks='0',
                min_shortdarks='0'
        )

        products = get_products()

        checker = Checker(state=state, products=products)
        with self._caplog.at_level(logging.DEBUG):
            with patch('os.path.exists', return_value=True):
                with pytest.raises(RuntimeError) as exc_info:
                    checker.run()

        assert 'processed_stack not required' in str(exc_info.value)

    def test_checker_skip_some_pass(self):
        """
        If only some products required, check those and skip the others.
        """
        self._caplog.clear()

        state = get_state(
                min_objects='1',
                min_flats='1',
                min_longdarks='0',
                min_shortdarks='0'
        )

        products = get_products(
            processed_dark=None,
            processed_bpm=None
        )

        checker = Checker(state=state, products=products)
        with self._caplog.at_level(logging.DEBUG):
            with patch('os.path.exists', return_value=True):
                with patch('astropy.io.fits.getval', return_value='niriPipe'):
                    checker.run()

        assert 'Output stack found:' in self._caplog.text
        assert 'Output flat found:' in self._caplog.text
        assert 'Skipping checking of processed_dark' in self._caplog.text
        assert 'Skipping checking of processed_bpm' in self._caplog.text

    def test_checker_all_pass(self):
        """
        Checker should pass if all products present with proper metadata.
        """
        self._caplog.clear()

        # All products required by default
        state = get_state()
        products = get_products()

        checker = Checker(state=state, products=products)
        with self._caplog.at_level(logging.DEBUG):
            with patch('os.path.exists', return_value=True):
                with patch('astropy.io.fits.getval', return_value='niriPipe'):
                    checker.run()

        assert 'Output stack found:' in self._caplog.text
        assert 'Output flat found:' in self._caplog.text
        assert 'Output dark found:' in self._caplog.text
        assert 'Output bpm found:' in self._caplog.text

    def test_checker_one_bad_missing_file(self):
        """
        If one required product is missing, checker should raise an error.
        """
        self._caplog.clear()

        state = get_state()
        products = get_products(processed_stack=None)

        checker = Checker(state=state, products=products)

        with patch('astropy.io.fits.getval', raise_exception):
            with pytest.raises(IOError) as exc_info:
                checker.run()

        assert 'not found' in str(exc_info.value)

    def test_checker_one_bad_metadata(self):
        """
        If one required product has bad metadata, checker should raise an
        error.
        """
        self._caplog.clear()

        state = get_state()
        products = get_products()

        checker = Checker(state=state, products=products)

        with patch('astropy.io.fits.getval', return_value='bad_value'):
            with pytest.raises(RuntimeError) as exc_info:
                checker.run()

        assert 'Malformed metadata' in str(exc_info.value)
