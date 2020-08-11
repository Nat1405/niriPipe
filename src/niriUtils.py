import glob
import hashlib
import json
import logging
import os
import re
import requests
import shutil
import tempfile
from astroquery.cadc import Cadc


def downloadQueryCadc(query, directory='./rawData'):
    """
    Finds and downloads all CADC files for a particular CADC query to
    the current working directory.
    """

    cadc = Cadc()
    job = cadc.create_async(query)
    job.run().wait()
    job.raise_if_error()
    result = job.fetch_result().to_table()

    # Store product id's for later
    pids = list(result['productID'])

    urls = cadc.get_data_urls(result)
    cwd = os.getcwd()
    os.chdir(directory)
    for url, pid in zip(urls, pids):
        try:
            filename = getFile(url)
            logging.debug("Downloaded {}".format(filename))
        except Exception as e:
            logging.error("A frame failed to download.")
            os.chdir(cwd)
            raise e
    os.chdir(cwd)


def getFile(url):
    """
    Gets a file from the specified url and returns the filename.
    """
    r = requests.get(url, stream=True)
    # Parse out filename from header
    try:
        filename = re.findall(
            "filename=(.+)", r.headers['Content-Disposition'])[0]
    except KeyError:
        # 'Content-Disposition' header wasn't found, so parse filename from URL
        # Typical URL looks like:
        # https://www.cadc-ccda.hia-iha.nrc-cnrc.gc.ca/data/pub/GEM/N20140505S0114.fits?RUNID=mf731ukqsipqpdgk
        filename = (url.split('/')[-1]).split('?')[0]

    # Write the fits file to the current directory, verifying the md5 hash as 
    # we go. Store partial results in a temporary file.
    writeWithTempFile(r, filename)

    return filename


def writeWithTempFile(request, filename):
    """ Write the fits file, verifying the md5 hash as we go. Store partial 
    results in a temporary file. """
    temp_downloads_path = '.temp-downloads'
    if not os.path.exists(temp_downloads_path):
        os.mkdir(temp_downloads_path)
    try:
        server_checksum = request.headers['Content-MD5']
    except KeyError:
        # Catch case that header didn't contain a 'content-md5' header
        logging.warning(
            "Content-MD5 header not found for file {}. "\
            "Skipping checksum validation.".format(filename))
        server_checksum = None

    # Write out content (first to a temp file) optionally doing an md5 verification.
    download_checksum = hashlib.md5()
    with tempfile.TemporaryFile(mode='w+b', prefix=filename, dir=temp_downloads_path) as f:
        for chunk in request.iter_content(chunk_size=128):
            f.write(chunk)
            download_checksum.update(chunk)
        if server_checksum and (server_checksum != download_checksum.hexdigest()):
            logging.error("Problem downloading {}.".format(filename))
            raise IOError
        f.seek(0)
        with open(filename, 'w') as out_fp:
            out_fp.write(f.read())

    return filename


def checkForMDFiles(path, query):
    """
    Gemini sometimes attaches an "-md!" prefix to their files to indicate there's
    some sort of metadata problem with them. The CADC archive doesn't currently
    contain that information, but it can be queried from the Gemini Science Archive.
    """
    try:
        # This abuses getFile because (currently) if the content-disposition header isn't found it
        # uses a hard-coded part of the url as the filename (which makes no sense here). However,
        # it doesn't seem as if that should break things.
        json_filename = getFile(query)
    except Exception:
        logging.warning(
            "MD Report check failed. Some fits files with broken metadata may have made it through.")
        return

    with open(json_filename) as f:
        data = json.load(f)
    for item in data:
        try:
            if not item['mdready']:
                # Do some sort of removal of the actual fits file here
                fits_filename = item['filename'].rstrip('.bz2')
                if os.path.exists(os.path.join(path, fits_filename)):
                    os.remove(os.path.join(path, fits_filename))
        except Exception:
            continue
