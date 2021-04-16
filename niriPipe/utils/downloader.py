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
from astroquery.cadc import Cadc
import os
import requests
import re
import hashlib
import shutil
import glob
import datetime
import astrodata
import gemini_instruments  # noqa: F401
import niriPipe.utils.customLogger


class Downloader:
    """
    Downloads fits files from the CADC archive.

    Based on work in:
    https://github.com/Nat1405/Nifty4Gemini/blob/master/nifty/pipeline/nifsUtils.py

    Parameters
    ----------
    table: :obj:`astropy.table.Table`
        Table of data to download. Requires publisherID and productID column.
    state: :obj:`utils.State`
        Required application state.

    Raises
    ------
    KeyError
        If input table is missing required columns.

    """
    def __init__(self, table, state):
        self.table = table
        self.state = state
        self.logger = niriPipe.utils.customLogger.get_logger(
            '{}.{}'.format(
                self.__module__, self.__class__.__name__))
        self.download_path = os.path.join(
            self.state['current_working_directory'],
            self.state['config']['DATARETRIEVAL']['raw_data_path']
        )
        self._prep_directory(self.download_path)

    def _prep_directory(self, directory):
        # Don't catch errors that result from directory already existing.
        os.mkdir(directory)

    def download_query_cadc(self):
        """
        Download a table of publisherIDs from the CADC archive.
        """

        # Store product id's for later
        try:
            pids = list(self.table['productID'])
        except KeyError as e:
            self.logger.error("No productID column found in input table.")
            raise e

        try:
            urls = Cadc.get_data_urls(self.table)
        except Exception as e:
            self.logger.error(
                "Problem getting data urls; did the input table " +
                "have a publisherID column?", exc_info=True
                )
            raise e

        for url, pid in zip(urls, pids):
            try:
                filename = self._get_file(url)
                self.logger.info("Downloaded {}".format(filename))
            except Exception as e:
                self.logger.error(
                    "Frame {} failed to download.".format(pid),
                    exc_info=True
                )
                raise e

    def _get_file(self, url):
        """
        Gets a file from the specified url and returns the filename.
        """
        r = requests.get(url, stream=True)
        # Parse out filename from header
        try:
            filename = re.findall(
                "filename=(.+)",
                r.headers['Content-Disposition'])[0]
            # Some filenames now have extra quotation marks
            filename = filename.replace('"', '')
        except (KeyError, IndexError):
            # 'Content-Disposition' header wasn't found, so parse filename
            # from URL. Typical URL looks like:
            # https://www.cadc-ccda.hia-iha.nrc-cnrc.gc.ca/data/pub/GEM/N20140505S0114.fits?RUNID=mf731ukqsipqpdgk
            filename = (url.split('/')[-1]).split('?')[0]

        # Write the fits file to the current directory, verifying the md5
        # hash as we go. Store partial results in a temporary file.
        self._write_with_temp_file(r, filename)

        return filename

    def _write_with_temp_file(self, response, filename):
        """
        Write the fits file, verifying the md5 hash as we go.
        Store partial results in a temporary file.
        """
        try:
            server_checksum = response.headers['Content-MD5']
        except KeyError:
            # Catch case that header didn't contain a 'content-md5' header
            self.logger.warning(
                "Content-MD5 header not found for file {}.".format(filename) +
                " Skipping checksum validation."
            )
            server_checksum = None

        # Write out content (first to a temp file) optionally doing md5 check.
        download_checksum = hashlib.md5()
        tmp_dest = os.path.join(self.download_path, '.'+filename)
        dest = os.path.join(self.download_path, filename)
        try:
            with open(tmp_dest, mode='wb') as f:
                for chunk in response.iter_content(chunk_size=128):
                    f.write(chunk)
                    download_checksum.update(chunk)
            if server_checksum:
                if server_checksum == download_checksum.hexdigest():
                    self.logger.debug(
                        "Download finished with matching checksum " +
                        "for {}.".format(filename))
                    shutil.move(tmp_dest, dest)
                    self.logger.debug(
                        "Moved {} to {}.".format(
                            os.path.basename(tmp_dest),
                            os.path.basename(dest)))
                else:
                    raise RuntimeError(
                        "Checksum mismatch for {}.".format(filename))
            else:
                self.logger.debug(
                        "Download finished for {}.".format(filename))
                shutil.move(tmp_dest, dest)
                self.logger.debug(
                        "Moved {} to {}.".format(
                            os.path.basename(tmp_dest),
                            os.path.basename(dest)))
        except Exception as e:
            self.logger.error("Problem downloading {}.".format(filename))
            # Remove temporary file
            if os.path.exists(tmp_dest):
                self.logger.debug("Removing temp file {}".format(tmp_dest))
                os.remove(tmp_dest)
            raise e

        return filename

    def _get_date(self, fits_file):
        """
        Returns UT date from fits file.
        """
        return astrodata.open(fits_file).ut_date()

    def _find_fits(self, path):
        """
        Wrapper to make testing easier.
        """
        return glob.glob(path)  # pragma: no cover

    def _file_exists(self, path):
        """
        Wrapper to make testing easier.
        """
        return os.path.exists(path)  # pragma: no cover

    def _remove_file(self, file):
        """
        Wrapper to make testing easier.
        """
        os.remove(file)  # pragma: no cover
