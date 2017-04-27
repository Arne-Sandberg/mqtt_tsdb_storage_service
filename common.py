#!/usr/bin/env python


################################################################################
#
#  Copyright (C) 2016, Carnegie Mellon University
#  All rights reserved.
#
#  This program is free software: you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation, version 2.0 of the License.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with this program.  If not, see <http://www.gnu.org/licenses/>.
#
#  Contributing Authors (specific to this file):
#
#  Khushboo Bhatia               khush[at]cmu[dot]edu
#
################################################################################

from optparse import OptionParser
from configparser import ConfigParser

def parse_arguments():
    optp = OptionParser()
    optp.add_option('-f','--config_file', dest='config_file', help='Config file')
    opts, args = optp.parse_args()
    if opts.config_file is None:
        optp.print_help()
        exit()

    config = ConfigParser()
    config.read(opts.config_file)

    opts = dict()

    for key in config['DEFAULT']:
        opts[key] = config['DEFAULT'][key]

    return opts
