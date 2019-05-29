#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Copyright (C) Joseph Areeda (2019)
#
# This file is part of GWpy and LDVW.
#
# GWpy is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# GWpy is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with GWpy.  If not, see <http://www.gnu.org/licenses/>.
#

"""
Generate timing data for reading frame data
"""
__author__ = 'joseph areeda'
__email__ = 'joseph.areeda@ligo.org'
__version__ = '0.0.2'
__process_name__ = 'profile_get_data'

import argparse
import logging
import os
from time import time

from gwdetchar.io.datafind import get_data

verbosity = True


def time_get(channel, start, end, nproc):
    tstrt = time()
    data = get_data(channel, start, end, nproc=nproc, verbose=verbosity,
                    source=args.cache)
    elap = time() - tstrt
    nbytes = get_size(data)
    rep = '{:d}, {:.1f}, {:d}, {:d}, {:.3f}\n'.format(len(channel),
                                    (end-start)/3600, nproc, nbytes, elap)
    out.write(rep)
    out.flush()
    logger.info(rep)

def get_size(data):
    ret = 0
    for k in data.keys():
        d = data[k]
        l = len(d)
        ds = d.itemsize
        dsize = l * ds
        ret += dsize
    return ret

parser = argparse.ArgumentParser(description=__doc__,
                                 prog='gsyTimelinePlot.py')

parser.add_argument('-c', '--cache', help='LAL cache file', required=True,
                    action='store')
parser.add_argument('-C', '--chan-list', help='list of channels in frames',
                    required=True, action='store')
parser.add_argument('-o', '--out', help='output file', required=True,
                    action='store')
parser.add_argument('-s', '--start', help='start GPS', required=True,
                    action='store', type=int)
parser.add_argument('-e', '--end', help='end GPS', required=True,
                    action='store', type=int)

args = parser.parse_args()

logging.basicConfig()
logger = logging.getLogger(__process_name__)
logger.setLevel(logging.DEBUG)

timing_hdr = '# n-chan/read, data len(hr), nproc, nbytes, read time\n'
nprocs = [1, 8]
rd_hrs = [1, 28]


if os.path.isfile(args.out):
    out = open(args.out,'a')
else:
    out = open(args.out, 'w')
    out.write(timing_hdr)

logger.info(timing_hdr)
chans = list()

with open(args.chan_list, 'r') as cl:
    c = cl.readline()
    while c:
        chans.append(c.replace('\n', ''))
        c = cl.readline()

nchans = len(chans)

# once to prime any buffers

time_get(chans[0:1], args.start, args.end, 1)

for nchan in [2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192, 16384, 32768, 65536, 131072]:
    if nchan <= nchans:
        for nproc in range(1, min(nchan,nprocs[1])):
            for hrs in range(rd_hrs[0], rd_hrs[1]):
                time_get(chans[0:nchan], args.start, args.start +  hrs*3600,
                         nproc)