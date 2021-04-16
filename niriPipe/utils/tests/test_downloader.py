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
import shutil
import logging
from niriPipe.utils.downloader import Downloader
import niriPipe.utils.customLogger

THIS_DIR = os.path.dirname(os.path.realpath(__file__))
TESTDATA_DIR = os.path.join(THIS_DIR, 'data')

NETWORK_TESTS = True

BAD_DATE_STRING = 'Unable to get date'


# Need to enable propagation for log capturing to work
niriPipe.utils.customLogger.enable_propagation()
niriPipe.utils.customLogger.set_level(logging.DEBUG)


class MockResponse:
    def __init__(self, json_data, status_code, headers=None):
        self.json_data = json_data
        self.status_code = status_code
        self.headers = headers

    def json(self):
        return self.json_data

    def iter_content(self, **args):
        return open(self.json_data['test_file'], mode='rb')


def get_state():
    return {
            'current_working_directory': os.getcwd(),
            'config': {
                'DATARETRIEVAL': {
                    'raw_data_path': 'rawData',
                }
            }
        }


def throw_exception(*args, **kwargs):
    raise IOError


def output_input(foo, input_value):
    return input_value


class TestDownloader(unittest.TestCase):
    """
    Class for testing the Downloader class.

    Adds some extra strings it expects to find in log messages.
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

    @unittest.skipUnless(NETWORK_TESTS, 'Skipping live network tests.')
    def test_end_to_end_download(self):
        """
        Test end-to-end download of a single Gemini NIFS file.
        """
        publisherIDs = \
            ['ivo://cadc.nrc.ca/GEMINI?GN-2014A-Q-85-32-014/N20140505S0341']
        productIDs = ['N20140505S0341']
        table = astropy.table.Table(
            [publisherIDs, productIDs],
            names=('publisherID', 'productID'))

        state = get_state()

        d = Downloader(table=table, state=state)
        d.download_query_cadc()

        assert os.path.exists(os.path.join(
            d.download_path,
            'N20140505S0341.fits'
        ))

    def test_downloader_bad_table(self):
        """
        Tables should have publisherID and productID columns.
        """
        # productID missing
        foo = []
        bar = []
        table = astropy.table.Table(
            [foo, bar],
            names=('foo', 'bar'))

        state = get_state()

        d = Downloader(table=table, state=state)
        with pytest.raises(KeyError):
            d.download_query_cadc()
        shutil.rmtree(d.download_path)

        # publisherID column missing
        garbage = [None]
        productIDs = ['N20140505S0341']
        table = astropy.table.Table(
            [garbage, productIDs],
            names=('garbage', 'productID'))
        d = Downloader(table=table, state=state)
        with pytest.raises(AttributeError):
            d.download_query_cadc()

    @unittest.skipUnless(NETWORK_TESTS, 'Skipping live network tests.')
    @patch('niriPipe.utils.downloader.Downloader._write_with_temp_file')
    def test__get_file(self, mock):
        """
        Make sure filenames are being formatted correctly.
        """
        state = get_state()

        url = 'https://www.cadc-ccda.hia-iha.nrc-cnrc.gc.ca/data/pub/GEM/' + \
              'N20140505S0114.fits?RUNID=mf731ukqsipqpdgk'

        d = Downloader(table=None, state=state)
        d._get_file(url)
        assert d._get_file(url) == \
            'N20140505S0114.fits'

    def test__write_with_temp_file(self):
        """
        Download file with a checksum validation if header present,
        otherwise download without checksum validation.
        """
        # "Download" a file with an MD5
        response = MockResponse(
            {
                "Content-Disposition":
                    'inline; filename="N20140505S0114.fits"',
                "test_file": 'fake_content'
            },
            200,
            headers={
                "Content-MD5": 'c157a79031e1c40f85931829bc5fc552'
            }
        )
        state = get_state()

        with open('fake_content', 'w') as f:
            f.write("bar\n")

        d = Downloader(table=None, state=state)
        d._write_with_temp_file(
            response,
            filename='fake_file.txt'
        )

        with open(os.path.join(d.download_path, 'fake_file.txt')) as f:
            assert f.read() == 'bar\n'
        if os.path.exists(d.download_path):
            shutil.rmtree(d.download_path)

        # Try the same, but missing Content-MD5
        response = MockResponse(
            {
                "Content-Disposition":
                    'inline; filename="N20140505S0114.fits"',
                "test_file": 'fake_content'
            },
            200,
            headers={
                "Checksum": ''
            }
        )
        with open('fake_content', 'w') as f:
            f.write("bar\n")
        if os.path.exists(d.download_path):
            shutil.rmtree(d.download_path)

        d = Downloader(table=None, state=state)
        with self._caplog.at_level(logging.WARNING):
            d._write_with_temp_file(
                response,
                filename='fake_file.txt'
            )
        assert 'Content-MD5 header not found for file' in self._caplog.text

        with open(os.path.join(d.download_path, 'fake_file.txt')) as f:
            assert f.read() == 'bar\n'
        if os.path.exists(d.download_path):
            shutil.rmtree(d.download_path)

        # Test catch of an MD5 mismatch
        response = MockResponse(
            {
                "Content-Disposition":
                    'inline; filename="N20140505S0114.fits"',
                "test_file": 'fake_content'
            },
            200,
            headers={
                "Content-MD5": '00000000000000000000000000000000'
            }
        )
        with open('fake_content', 'w') as f:
            f.write("bar\n")

        d = Downloader(table=None, state=state)
        with pytest.raises(RuntimeError):
            d._write_with_temp_file(
                response,
                filename='fake_file.txt'
            )
