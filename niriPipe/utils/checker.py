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
import astropy.io.fits as fits
import os
import niriPipe.utils.tagger
import niriPipe.utils.customLogger


class Checker:
    """
    Checks that required products are present and well-formed.
    """
    def __init__(self, products, state):
        self.products = products
        self.state = state
        self.logger = niriPipe.utils.customLogger.get_logger('{}.{}'.format(
            self.__module__, self.__class__.__name__))
        self.keys = niriPipe.utils.tagger.Tagger.keys

    def run(self):
        """
        Check that required products are present, and do a quick check to make
        sure Tagger was run on them.
        """
        for raw_key, product_key in self.keys:
            config_key = 'min_{}s'.format(raw_key)
            product_name = 'processed_{}'.format(product_key)

            if int(self.state['config']['DATAFINDER'][config_key]):
                self.logger.debug("Checking {}".format(product_name))
                self.logger.debug("Product file is {}".format(
                    self.products[product_name]))
                if not Checker._check_metadata(
                        self.products[product_name]):
                    raise RuntimeError(
                        "Malformed metadata in processed_{}".format(
                            product_key))
                self.logger.info(
                    "Output {} found: {}".format(
                        product_key,
                        self.products[product_name]))
            else:
                self.logger.debug("Skipping checking of {}.".format(
                    product_name))
                # If a product isn't required but exists, something weird
                # might be going on. Raise an error if that happens.
                if self.products[product_name] and \
                        os.path.exists(self.products[product_name]):
                    raise RuntimeError(
                        "{} not required but {} exists on disk!".format(
                            product_name, self.products[product_name]))

        return self.products

    @staticmethod
    def _check_metadata(file):
        """
        Do a quick check to make sure Tagger was run on products.
        """
        return (fits.getval(file, 'SOFTWARE', extname='PRIMARY') == 'niriPipe')
