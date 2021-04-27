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
import logging
import astrodata  # noqa: F401
import recipe_system.reduction.coreReduce
import recipe_system.utils.reduce_utils
import gempy.utils
import os


def raiseIfError(msg):
    """
    Wrapper for pipeline steps that MUST succeed.

    Shorthand for:
    try:
        foo()
    except Exception as e:
        logging.critical(msg)
        raise e
    """
    def decorator(func):
        def _wrapper(*args, **kwargs):
            try:
                func(*args, **kwargs)
            except Exception as e:
                logger = args[0].logger
                logger.critical(msg)
                raise e

        return _wrapper
    return decorator


class Reducer:
    """
    Uses Gemini DRAGONS to reduce NIRI data.

    The reducer recieves as input an astropy table containing downloaded
    calibrations and object frames (it is assumed that the table is
    correct). It first sets up Gemini DRAGONS (initializes the caldb,
    writes to the DRAGONS config file, etc.). It then proceeds with the
    reduction.
    """
    def __init__(self, state, table):
        self.state = state
        self.table = table
        self.logger = logging.getLogger('{}.{}'.format(
            self.__module__, self.__class__.__name__))

        self.products = {
            'processed_dark': None,
            'processed_bpm': None,
            'processed_flat': None,
            'processed_stack': None
        }

    def run(self):
        """
        Main entrypoint to the reducer.
        """
        self._init_dragons()

        # Catch case of empty table here...
        if len(self.table) == 0:
            return self.products

        self._make_dark()
        self._make_bpm()
        self._make_flat()
        self._make_object_stack()

        return self.products

    @raiseIfError('Failed to initialize DRAGONS.')
    def _init_dragons(self):
        """
        Do required setup of Gemini DRAGONS.
        """
        self.logger.debug("Starting DRAGONS initialization.")

        logfile = self.state['config']['REDUCTION']['logfile']
        self.logger.debug("DRAGONS log file is {}".format(logfile))
        gempy.utils.logutils.config(file_name=logfile)

        self.logger.debug("DRAGONS initialization finished.")

    @raiseIfError('Failed to make processed dark.')
    def _make_dark(self):
        """
        Make the processed dark and return path to it.

        If the num_required_longdarks is zero, any errors in
        processing shouldn't matter.
        """
        self._make_product(
            frame_type='longdark',
            mask=(self.table['niriPipe_type'] == 'longdark'),
            product_name='processed_dark'
        )

    @raiseIfError('Failed to make processed bad pixel mask.')
    def _make_bpm(self):
        """
        Make a "fresh" bad pixel mask.
        """
        self._make_product(
            frame_type='shortdark',
            mask=((self.table['niriPipe_type'] == 'flat') |
                  (self.table['niriPipe_type'] == 'shortdark')),
            product_name='processed_bpm',
            recipename='makeProcessedBPM'
        )

    @raiseIfError('Failed to make processed flat.')
    def _make_flat(self):
        """
        Make the processed flat field frame.
        """
        self._make_product(
            frame_type='flat',
            mask=(self.table['niriPipe_type'] == 'flat'),
            product_name='processed_flat'
        )

    @raiseIfError('Failed to make processed output stack.')
    def _make_object_stack(self):
        """
        Make the actual stack.
        """
        self._make_product(
            frame_type='object',
            mask=(self.table['niriPipe_type'] == 'object'),
            product_name='processed_stack'
        )

    def _make_product(
            self, frame_type, mask, product_name, recipename=None
            ):
        """
        Does the work of creating a product.
        """
        if not int(self.state['config']['DATAFINDER']['min_{}s'.format(
                frame_type)]):
            self.logger.debug("Skipping creation of {}.".format(product_name))
            return None
        self.logger.debug("Starting creation of {}.".format(product_name))

        input_frames = self.table[mask]

        self.logger.debug(
            "Found {} input {} frames.".format(len(input_frames), frame_type))

        self.logger.debug(
            "Starting reduction of {} frames with DRAGONS.".format(frame_type))

        # This (probably badly?) assumes that filenames will always equal
        # prefix + productID + .fits. A better way would for the data table to
        # have a column with the actual path to each product, but I think it's
        # too annoying to implement and test for the gain right now. #techdebt
        prefix = self.state['config']['DATARETRIEVAL']['raw_data_path']
        paths = [
                            os.path.join(prefix, x + '.fits')
                            for x in input_frames['productID']
        ]

        dragons_reduce = recipe_system.reduction.coreReduce.Reduce()
        dragons_reduce.files.extend(paths)

        if recipename:
            self.logger.debug("Using provided recipe: {}".format(recipename))
            dragons_reduce.recipename = recipename

        # Use custom bad pixel mask if provided
        if self.products['processed_bpm']:
            self.logger.debug("Using provided bad pixel mask: {}".format(
                    self.products['processed_bpm']))
            dragons_reduce.uparms = \
                [('addDQ:user_bpm', self.products['processed_bpm'])]

        # Dark correction can be optional
        if int(self.state['config']['DATAFINDER']['min_longdarks']) == 0:
            self.logger.debug("Turning off dark correction.")
            dragons_reduce.uparms.append(('darkCorrect:do_dark', False))

        # Provide calibrations manually to DRAGONS for object frames
        if frame_type == 'object':
            dragons_reduce.ucals = \
                recipe_system.utils.reduce_utils.normalize_ucals(
                    dragons_reduce.files, Reducer._pretty_string(
                        self.products))

        # Start up DRAGONS
        dragons_reduce.runr()

        self.logger.debug("Finished creation of {}.".format(product_name))

        self.products[product_name] = dragons_reduce.output_filenames[0]

    @staticmethod
    def _pretty_string(product_dict):
        """
        _pretty_string takes in a dict of cals like:
            {
            'processed_flat': <path_to_flat>,
            'processed_dark': <path_to_dark>,
            'processed_bpm': <path_to_bpm>, ...
            }
        filters the keywords DRAGONS likes out ('processed_flat',
        'processed_dark'), and gets it in normalize_ucals format
                [
                'processed_flat:<path_to_flat>',
                'processed_dark:<path_to_dark>'
                ]
        """
        out_list = []
        for key in product_dict:
            if key == 'processed_flat' and product_dict[key]:
                out_list.append('processed_flat:'+product_dict[key])
            elif key == 'processed_dark' and product_dict[key]:
                out_list.append('processed_dark:'+product_dict[key])
        return out_list
