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
import astropy.io.fits as fits
import niriPipe.utils.customLogger


class Tagger:
    """
    Makes products have CADC-appropriate metadata.
    """
    keys = [
            ('object', 'stack'),
            ('flat', 'flat'),
            ('longdark', 'dark'),
            ('shortdark', 'bpm')
    ]

    def __init__(self, products, state):
        self.products = products
        self.state = state
        self.logger = niriPipe.utils.customLogger.get_logger('{}.{}'.format(
            self.__module__, self.__class__.__name__))

    def run(self):
        for raw_key, product_key in self.keys:
            config_key = 'min_{}s'.format(raw_key)
            product_name = 'processed_{}'.format(product_key)

            if int(self.state['config']['DATAFINDER'][config_key]):
                # Add product-specific keywords
                if self.products['processed_bpm']:
                    self.set_header_keyword(
                        filename=self.products[product_name],
                        keyword='BPMIMG',
                        value=os.path.basename(self.products['processed_bpm']),
                        extname='PRIMARY',
                        comment='Bad pixel mask used'
                    )

                # Add generic metadata keywords
                self.set_header_keyword(
                    filename=self.products[product_name],
                    keyword='SOFTWARE',
                    value='niriPipe',
                    extname='PRIMARY',
                    comment='Data reduction software name'
                )
                self.set_header_keyword(
                    filename=self.products[product_name],
                    keyword='SOFT_VER',
                    value='0.1',
                    extname='PRIMARY',
                    comment='Data reduction software version'
                )
                self.set_header_keyword(
                    filename=self.products[product_name],
                    keyword='SOFT_DOI',
                    value='10.5281/zenodo.4729003',
                    extname='PRIMARY',
                    comment='Data reduction software DOI'
                )
            else:
                self.logger.debug("Skipping tagging of {}.".format(
                    product_name))

        return self.products

    def set_header_keyword(self, filename, keyword, value, extname, comment):
        """
        Stub to make testing easier.
        """
        self.logger.info(
            "Setting {} in {} to {}.".format(keyword, filename, value))
        fits.setval(
                filename, keyword, value=value,
                extname=extname, comment=comment)
