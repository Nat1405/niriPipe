import os
import shutil
import pytest

from niriPipe.niriUtils import downloadQueryCadc
from niriPipe.niriSort import NIRIData

try:
    from mock import Mock, patch, PropertyMock
except ImportError:
    pytest.skip("Install mock for the cadc tests.", allow_module_level=True)


def data_path(filename):
    data_dir = os.path.join(os.path.dirname(__file__), 'data')
    return os.path.join(data_dir, filename)


"""
def download_test_data():
    if os.path.exists(data_path("GN-2014A-Q-85-all")):
        shutil.rmtree(data_path("GN-2014A-Q-85-all"))
    os.mkdir(data_path("GN-2014A-Q-85-all"))

    query = "SELECT observationID, type, publisherID, productID \
             FROM caom2.Plane AS Plane JOIN caom2.Observation AS Observation ON Plane.obsID = Observation.obsID \
             WHERE  ( Observation.instrument_name = 'NIFS' \
             AND Observation.collection = 'GEMINI' \
             AND Observation.proposal_id = 'GN-2014A-Q-85' )"

    downloadQueryCadc(query, directory=data_path("GN-2014A-Q-85-all"))

    one_day_files = glob.glob(os.path.join(
        data_path("GN-2014A-Q-85-all"), "*20140428*"))

    if os.path.exists(data_path("GN-2014A-Q-85_one_day")):
        shutil.rmtree(data_path("GN-2014A-Q-85_one_day"))
    os.mkdir(data_path("GN-2014A-Q-85_one_day"))

    for file in one_day_files:
        shutil.copy(file, data_path("GN-2014A-Q-85_one_day"))
"""


def test_getFilesGN2018ADD10915(tmpdir):
    """Test should find:
        - 30 Object files, with exposure time/filter combinations of (15.001, J),
            (30.002, H), (30.002, K(short)).
        - 40 J band flats, 40 H band flats, 60 K(short) flats
        - 55 Long Darks with exptime = 15.001, 35 long darks with exptime = 30.002
        - 55 possible Short Darks to use.
    """

    tmpdir = str(tmpdir)

    nData = NIRIData()

    result = nData.findDataForObservation('GN-2018A-DD-109-15')

    objects_list = ['N20180711S0' + str(x) for x in range(184, 214)]

    flats_list = ['N20180711S0' + str(x) for x in range(348, 388)]
    flats_list.extend(['N20180711S0' + str(x) for x in range(448, 488)])
    flats_list.extend(['N20180711S0' + str(x) for x in range(528, 598)])
    flats_list.extend(['N20180711S0' + str(x) for x in range(728, 748)])

    long_darks_list = ['N20180711S0' + str(x) for x in range(418, 433)]
    long_darks_list.extend(['N20180711S0' + str(x) for x in range(558, 568)])
    long_darks_list.extend(['N20180711S0' + str(x) for x in range(608, 618)])
    long_darks_list.extend(['N20180711S0' + str(x) for x in range(518, 528)])

    short_darks_list = ['N20180711S0' + str(x) for x in range(388, 403)]
    short_darks_list.extend(['N20180711S0' + str(x) for x in range(548, 578)])
    short_darks_list.extend(['N20180711S0' + str(x) for x in range(598, 628)])

    assert sorted(list(result['productID'])) == \
        sorted(list(set(sum([objects_list, flats_list, long_darks_list, short_darks_list], []))))
