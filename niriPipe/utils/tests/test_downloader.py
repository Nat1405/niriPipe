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
from unittest.mock import patch, Mock
import pytest
import astropy.table
import os
import logging
import datetime
from niriPipe.utils.downloader import Downloader

THIS_DIR = os.path.dirname(os.path.realpath(__file__))
TESTDATA_DIR = os.path.join(THIS_DIR, 'data')

NETWORK_TESTS = True

SKIP_STRING = 'Skipping Gemini metadata check'
BAD_DATE_STRING = 'Unable to get date'


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
                    'temp_downloads_path': '.temp_downloads'
                }
            }
        }


def throw_exception():
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
            filename=os.path.join(
                d.download_path,
                'fake_file.txt'
            )
        )

        with open(os.path.join(d.download_path, 'fake_file.txt')) as f:
            assert f.read() == 'bar\n'

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

        d = Downloader(table=None, state=state)
        with self._caplog.at_level(logging.WARNING):
            d._write_with_temp_file(
                response,
                filename=os.path.join(
                    d.download_path,
                    'fake_file.txt'
                )
            )
        assert 'Content-MD5 header not found for file' in self._caplog.text

        with open(os.path.join(d.download_path, 'fake_file.txt')) as f:
            assert f.read() == 'bar\n'

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
        with pytest.raises(IOError):
            d._write_with_temp_file(
                response,
                filename=os.path.join(
                    d.download_path,
                    'fake_file.txt'
                )
            )

    # Test querying Gemini observatory archive for
    # bad metadata.

    # Several cases possible:
    # - No files provided. Should skip metadata check.
    # - Files provided, but can't find date for any.
    #   Should skip metadata check.
    # - One file has a valid date: should do metatdata
    #   check for that file.
    # - N files have valid date: should do metadata check
    #   for all of them.

    @patch.object(Downloader, '_find_fits', return_value=[])
    def test__check_for_MD_files_1(self, mock):
        """
        No files found. Skip metadata check.
        """
        state = get_state()
        self._caplog.clear()

        d = Downloader(table=None, state=state)
        with self._caplog.at_level(logging.WARNING):
            d._check_for_MD_files()
            assert SKIP_STRING in self._caplog.text

    @patch.multiple('niriPipe.utils.downloader.Downloader',
                    _find_fits=Mock(return_value=['fake_fits.fits']),
                    _remove_file=Mock(),
                    _get_date=throw_exception)
    def test__check_for_MD_files_2(self, **mocks):
        """
        Files found, but unable to get dates. Skip metadata check.
        """
        state = get_state()
        self._caplog.clear()

        d = Downloader(table=None, state=state)
        with self._caplog.at_level(logging.WARNING):
            d._check_for_MD_files()
        assert BAD_DATE_STRING in self._caplog.text
        assert SKIP_STRING in self._caplog.text

    @patch('requests.get', throw_exception)
    @patch.multiple('niriPipe.utils.downloader.Downloader',
                    _find_fits=Mock(return_value=['fake_file.fits']),
                    _get_date=Mock(
                        return_value=datetime.date(
                            2017, 5, 14)))
    def test__check_for_MD_files_3(self, **mocks):
        """
        Files found, one has valid date, but Gemini query failed.
        Skip metadata check.
        """
        state = get_state()
        self._caplog.clear()

        d = Downloader(table=None, state=state)
        d._check_for_MD_files()
        assert SKIP_STRING in self._caplog.text

    @patch.multiple('niriPipe.utils.downloader.Downloader',
                    _find_fits=Mock(return_value=['fake_file.fits']),
                    _remove_file=Mock(),
                    _file_exists=Mock(return_value=True),
                    _get_date=Mock(
                        return_value=datetime.date(
                            2017, 5, 14)))
    def test__check_for_MD_files_4(self, **mocks):
        """
        Single file and date found, bad metadata. Remove that file.
        """
        state = get_state()
        self._caplog.clear()

        response = MockResponse(
            [
                {
                    "name": "fake_file.fits",
                    "mdready": False
                }
            ],
            200
        )

        d = Downloader(table=None, state=state)
        with patch('requests.get', return_value=response):
            with self._caplog.at_level(logging.INFO):
                d._check_for_MD_files()
        assert 'Removing frame fake_file.fits' in self._caplog.text

    @patch.multiple('niriPipe.utils.downloader.Downloader',
                    _find_fits=Mock(return_value=[
                        'fake_1.fits',
                        'fake_2.fits',
                        'fake_3.fits',
                        'fake_4.fits'
                    ]),
                    _remove_file=Mock(),
                    _file_exists=Mock(return_value=True),
                    _get_date=Mock(
                        side_effect=[
                            datetime.date(2017, 5, 18),
                            datetime.date(2017, 5, 14),
                            datetime.date(2017, 5, 15),
                            datetime.date(2017, 5, 15),
                        ]))
    def test__check_for_MD_files_5(self, **mocks):
        """
        Several files found, and N have valid date; remove the
        1 with broken metadata.
        """
        state = get_state()
        self._caplog.clear()

        response = MockResponse(
            [
                {
                    "name": "fake_1.fits",
                    "mdready": False
                },
                {
                    "name": "fake_2.fits",
                    "mdready": True
                },
                {
                    "name": "fake_3.fits",
                    "mdready": True
                },
                {
                    "malformed_name": "fake_4.fits",
                    "mdready": True
                },
            ],
            200
        )

        d = Downloader(table=None, state=state)
        with patch('requests.get', return_value=response):
            with self._caplog.at_level(logging.DEBUG):
                d._check_for_MD_files()
        assert 'Removing frame fake_1.fits' in self._caplog.text
        assert 'Frame fake_2.fits passed' in self._caplog.text
        assert SKIP_STRING in self._caplog.text
