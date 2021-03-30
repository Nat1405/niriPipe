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
import os
import logging
import hashlib
from pathlib import Path
import niriPipe.utils.downloader
from niriPipe.utils.finder import Finder


def create_logger():
    root_logger = logging.getLogger('niriPipe')
    root_logger.setLevel(logging.DEBUG)
    root_logger.propagate = True
    if not root_logger.hasHandlers():
        ch = logging.StreamHandler()
        formatter = logging.Formatter(
            '%(asctime)s %(name)s %(levelname)s %(message)s')
        ch.setFormatter(formatter)
        root_logger.addHandler(ch)

    module_logger = logging.getLogger('niriPipe.inttests')
    return module_logger


module_logger = create_logger()


def md5_of_dir(directory):
    tmp_hash = hashlib.md5()
    for path in sorted(
            Path(directory).iterdir(),
            key=lambda p: str(p).lower()):
        tmp_hash.update(path.name.encode())
        if path.is_file():
            with open(path, "rb") as f:
                for chunk in iter(lambda: f.read(4096), b""):
                    tmp_hash.update(chunk)
    return tmp_hash.hexdigest()


def downloader_inttest():
    """
    Test by downloading all science data from GN-2019A-FT-108.

    Uses a hard-coded hash; liable to break if CADC files change.


    """
    query = \
        "SELECT observationID, publisherID, productID " +\
        "FROM caom2.Observation AS o JOIN caom2.Plane AS p " +\
        "ON o.obsID=p.obsID " +\
        "WHERE collection = 'GEMINI' " +\
        "AND instrument_name = 'NIRI' " +\
        "AND type = 'OBJECT' " +\
        "AND intent = 'science' " +\
        "AND proposal_id = 'GN-2019A-FT-108'"

    try:
        table = Finder._do_query(query)
    except Exception:
        raise RuntimeError("Problem getting table from CADC.")

    state = {
            'current_working_directory': os.getcwd(),
            'config': {
                'DATARETRIEVAL': {
                    'raw_data_path': 'rawData'
                }
            }
        }

    module_logger.info(
        "Starting download test; downloading science " +
        "frames from GN-2019A-FT-108.")
    d = niriPipe.utils.downloader.Downloader(state=state, table=table)
    d.download_query_cadc()

    # Verify hash of downloaded files is correct.
    expected_hash = '4b00b29786d6d4546675f3e108682c69'
    out_hash = md5_of_dir(os.path.join(
                state['current_working_directory'],
                state['config']['DATARETRIEVAL']['raw_data_path']))
    if out_hash == expected_hash:
        module_logger.info("Download test results hashed to expected value.")
    else:
        raise RuntimeError(
            "Hash of downloads '{}' is different ".format(out_hash) +
            "than the expected value of '0d6ae285dcdb60904561aca79618afef'.")


def finder_inttest():
    """
    Make sure the Finder class can find NIRI data.
    """

    """
    First test: find data for stack GN-2019A-FT-108-12.

    Stack name:  GN-2019A-FT-108-12
    Object:      N20190405S01[11-37]
    Flats:       N20190406S00[07-26]
    Shortdark:   N20190406S01[12-21]
    Longdarks:   N20190406S00[42-56]
    """

    state = {
        'config': {
            'DATAFINDER': {
                'min_objects': '1',
                'min_flats': '1',
                'min_longdarks': '1',
                'min_shortdarks': '0',
                'max_tries': 30,
            }
        },
        'current_stack': {
            'obs_name': 'GN-2019A-FT-108-12',
            'proposal_id': 'GN-2019A-FT-108'
        }
    }

    module_logger.info("Starting finder inttest with stack {}".format(
        state['current_stack']['obs_name']))

    CADC_data_finder = Finder(state=state)
    table = CADC_data_finder.run()
    table.sort(['productID'])

    module_logger.debug("Resulting table:")
    module_logger.debug('\n'+'\n'.join(table.pformat_all()))

    # Object frames
    desired_frames = \
        ['N20190405S01'+str(x) for x in range(11, 38)]
    # Flats
    desired_frames.extend(
        ['N20190406S00'+'{:02d}'.format(x) for x in range(7, 27)])
    # Short darks
    desired_frames.extend(
        ['N20190406S01'+'{:02d}'.format(x) for x in range(12, 22)])
    # Long darks
    desired_frames.extend(
        ['N20190406S00'+'{:02d}'.format(x) for x in range(42, 57)])

    desired_frames.sort()

    for frame in desired_frames:
        assert frame in table['productID'], "Frame {} not found.".format(frame)

    module_logger.info(
        "Test for stack GN-2019A-FT-108-12 succeeded. " +
        "Desired {} frames, found {}.".format(len(desired_frames), len(table)))

    """
    Another test: get a stack and associated calibrations from
    Tim Davidge's program.

    Stack name:     GN-2007B-Q-85-24
    Object:         N20080120S01[02-31]
    Flats:          N20080120S03[24-32], N20080120S03[34-42]
    Longdarks:      N20080121S03[83-94]
    Shortdarks:     N20080119S00[16-25]
    """

    desired_frames = ['N20080120S01{:02d}'.format(x) for x in range(2, 32)]
    desired_frames.extend(
        ['N20080121S0{:03d}'.format(x) for x in range(77, 138)])
    desired_frames.extend(
        ['N20080120S03{:02d}'.format(x) for x in range(24, 33)])
    desired_frames.extend(
        ['N20080120S03{:02d}'.format(x) for x in range(34, 43)])
    desired_frames.extend(
        ['N20080121S03{:02d}'.format(x) for x in range(83, 95)])
    desired_frames.extend(
        ['N20080119S00{:02d}'.format(x) for x in range(16, 26)])

    desired_frames.sort()

    state = {
        'config': {
            'DATAFINDER': {
                'min_objects': '1',
                'min_flats': '1',
                'min_longdarks': '1',
                'min_shortdarks': '0',
                'max_tries': 30
            }
        },
        'current_stack': {
            'obs_name': 'GN-2007B-Q-85-24',
            'proposal_id': 'GN-2007B-Q-85'
        }
    }

    module_logger.info("Starting finder inttest with stack {}".format(
        state['current_stack']['obs_name']))

    CADC_data_finder = Finder(state=state)
    table = CADC_data_finder.run()

    module_logger.debug("Resulting table:")
    module_logger.debug('\n'+'\n'.join(table.pformat_all()))

    for frame in desired_frames:
        assert frame in table['productID'], "Frame {} not found.".format(frame)

    module_logger.info(
        "Test for stack GN-2007B-Q-85-24 succeeded. " +
        "Desired {} frames, found {}.".format(len(desired_frames), len(table)))


def run_inttest():
    """
    Make sure reductions are working and making sense.
    """

    class Args:
        pass

    args = Args()
    args.obsID = ['GN-2019A-FT-108-12']
    args.proposal_id = []
    args.verbose = True
    args.config = None

    niriPipe.niriReduce.run_main(args)
