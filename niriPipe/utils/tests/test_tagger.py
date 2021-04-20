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
from niriPipe.utils.tagger import Tagger
import niriPipe.utils.customLogger

THIS_DIR = os.path.dirname(os.path.realpath(__file__))
TESTDATA_DIR = os.path.join(THIS_DIR, 'data')

# Need to enable propagation for log capturing to work
# Note that DRAGONS messes with the root logger, so can't
# check DEBUG log messages in pytests.
niriPipe.utils.customLogger.enable_propagation()
niriPipe.utils.customLogger.set_level(logging.DEBUG)


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
    The Tagger takes a dictionary of reduction products as input,
    and raises an error if something goes wrong. It is stateless
    and configuration-less.
    """

    products = {
        'processed_flat': processed_flat,
        'processed_dark': processed_dark,
        'processed_bpm': processed_bpm,
        'processed_stack': processed_stack
    }

    return products


class TestTagger(unittest.TestCase):
    """
    Test the Tagger class.
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

    def test_tagger_skip_tagging(self):
        """
        Skip tagging of products that aren't required.
        """
        products = get_products(processed_stack=None)

        self._caplog.clear()
        tagger = Tagger(
                products=products,
                state=get_state(
                        min_objects='1',
                        min_flats='0',
                        min_longdarks='1',
                        min_shortdarks='0'
                    )
        )
        with patch('astropy.io.fits.setval'):
            with self._caplog.at_level(logging.DEBUG):
                tagger.run()

        assert "Skipping tagging of processed_flat" in self._caplog.text
        assert "Skipping tagging of processed_bpm" in self._caplog.text

    def test_tagger_good_stack_no_cals(self):
        """
        If 'output_stack' exists but no other products present,
        just add generic metadata keywords (SOFTWARE, SOFT_VER, SOFT_DOI)
        """
        products = get_products(
            processed_flat=None,
            processed_dark=None,
            processed_bpm=None
        )

        self._caplog.clear()
        tagger = Tagger(
                products=products,
                state=get_state(
                    min_flats='0',
                    min_longdarks='0',
                    min_shortdarks='0'
                )
        )
        with self._caplog.at_level(logging.DEBUG):
            with patch('astropy.io.fits.setval'):
                tagger.run()

        assert 'Setting SOFTWARE' in self._caplog.text
        assert 'Setting SOFT_VER' in self._caplog.text
        assert 'Setting SOFT_DOI' in self._caplog.text

    def test_tagger_good_stack_bpm_present(self):
        """
        If 'output_stack' exists and 'processed_bpm' exists,
        add the 'BPMIMG' header keyword.
        """
        products = get_products(
            processed_flat=None,
            processed_dark=None
        )

        self._caplog.clear()
        tagger = Tagger(products=products, state=get_state())
        with self._caplog.at_level(logging.DEBUG):
            with patch('astropy.io.fits.setval'):
                tagger.run()

        assert 'Setting BPMIMG' in self._caplog.text
