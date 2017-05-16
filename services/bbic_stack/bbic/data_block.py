# BBIC data block
# Author: Jafet Villafranca, 2016
#         Raphael Dumusc,    2015
#
# Copyright (c) BBP/EPFL 2014-2015; All rights reserved.
# Do not distribute without further notice.

import math
from PIL import Image
import numpy as np
from .image_utils import compress_and_serialize
from .block_provider import BlockProvider


class DataBlock:
    """A block of a volume or image stack"""

    def __init__(self, u, v, z, nominal_size):
        assert isinstance(u, int)
        assert isinstance(v, int)
        assert isinstance(z, int)
        assert isinstance(nominal_size, int)

        self.u = u
        self.v = v
        self.z = z
        self.width = 0
        self.height = 0
        self.depth = 0
        self.nominal_size = nominal_size
        self.volume = None

    def allocate(self, width, height, depth):
        """Allocate memory for the block"""
        assert isinstance(width, int)
        assert isinstance(height, int)
        assert isinstance(depth, int)

        self.volume = np.empty([depth, height, width], dtype=np.uint8)
        self.width, self.height, self.depth = width, height, depth

    def allocateAndSet(self, width, height, depth, value):
        """Allocate memory for the block"""
        assert isinstance(width, int)
        assert isinstance(height, int)
        assert isinstance(depth, int)
        assert isinstance(value, int)

        if value is 0:
            self.volume = np.zeros([depth, height, width], dtype=np.uint8)
        else:
            self.volume = np.full([depth, height, width], value,dtype=np.uint8)
        self.width, self.height, self.depth = width, height, depth

    def __str__(self):
        return "Block (%d, %d, %d), dim: [%d, %d, %d]" %\
               (self.u, self.v, self.z, self.width, self.height, self.depth)

    def is_valid(self):
        """Does this block hold volume data"""
        return self.volume is not None

    def to_x_tiles(self, format_, source):
        """Export the block in tiles (compressed using *format_*)
        along the x axis"""
        x_tiles = []
        for x in range(0, self.width):
            from PIL import ImageOps
            if source == 0:  # source is X
                im = Image.fromarray(self.volume[:, :, x],mode='L').rotate(-90)
                im = ImageOps.mirror(im)
            elif source == 1:  # source is Y
                im = Image.fromarray(self.volume[:, :, x], mode='L')
            else:  # source is Z
                im = Image.fromarray(self.volume[:, :, x], mode='L').rotate(-90)

            tile = compress_and_serialize(im, format_)
            x_tiles.append(tile)
        return x_tiles

    def to_y_tiles(self, format_, source):
        """Export the block in tiles (compressed using *format_*)
        along the y axis"""
        y_tiles = []
        for y in range(0, self.height):
            from PIL import ImageOps
            if source == 0:  # source is X
                im = Image.fromarray(self.volume[:, y, :], mode='L').rotate(90)
                im = ImageOps.flip(im)
            elif source == 1:  # source is Y
                im = Image.fromarray(self.volume[:, y, :], mode='L')
            else:  # source is Z
                im = Image.fromarray(self.volume[:, y, :], mode='L')
                im = ImageOps.flip(im)

            tile = compress_and_serialize(im, format_)
            y_tiles.append(tile)
        return y_tiles

    def get_subblock_count(self, subblock_size):
        """Get the number of subblocks of size <subblock_size> that can be
        formed from this block"""
        assert isinstance(subblock_size, int)

        num_subblocks_x = math.ceil(float(self.width) / subblock_size)
        num_subblocks_y = math.ceil(float(self.height) / subblock_size)
        num_subblocks_z = math.ceil(float(self.depth) / subblock_size)
        return num_subblocks_x, num_subblocks_y, num_subblocks_z

    def get_subblock(self, u, v, z, subblock_size):
        """Return a subblock"""
        assert isinstance(u, int)
        assert isinstance(v, int)
        assert isinstance(z, int)
        assert isinstance(subblock_size, int)

        if self.nominal_size & subblock_size != 0:
            raise ValueError("Requested subblock of incompatible size")

        start = [u*subblock_size, v*subblock_size, z*subblock_size]
        if start[0] >= self.width or start[1] >= self.height or start[2] >= self.depth:
            print(self)
            raise ValueError("Requested subblock exceedes block boundaries")

        end = [min(start[0]+subblock_size, self.width),
               min(start[1]+subblock_size, self.height),
               min(start[2]+subblock_size, self.depth)]

        subblock = DataBlock(u, v, z, subblock_size)
        subblock.allocate(end[0]-start[0], end[1]-start[1], end[2]-start[2])
        subblock.volume[:] = self.volume[start[2]:end[2], start[1]:end[1], start[0]:end[0]]
        return subblock

    def copy(self, other):
        """Copy volume data from a (smaller or equal sized) block into this one."""
        assert isinstance(other, DataBlock)
        assert self.is_valid() and other.is_valid()

        shape = other.volume.shape
        self.volume[0:shape[0], 0:shape[1], 0:shape[2]] = other.volume[:]

    def split(self, subblock_size):
        """Split the block into smaller blocks of the desired size"""
        assert isinstance(subblock_size, int)

        subblocks = []
        nbr_subblocks = self.get_subblock_count(subblock_size)
        for z in range(nbr_subblocks[2]):
            for v in range(nbr_subblocks[1]):
                for u in range(nbr_subblocks[0]):
                    subblocks.append(self.get_subblock(u, v, z, subblock_size))
        return subblocks

    def fill(self, source, source_offset=(0, 0, 0)):
        """Fill the block from a source of smaller-sized blocks, reading blocks from the given (u,v,z) offset"""
        assert isinstance(source, BlockProvider)
        assert source.get_block_size() < self.nominal_size
        assert self.nominal_size % source.get_block_size() == 0

        # Iterate over source blocks
        nbr_subblocks = self.get_subblock_count(source.get_block_size())
        for z in range(nbr_subblocks[2]):
            for v in range(nbr_subblocks[1]):
                for u in range(nbr_subblocks[0]):
                    src_block = source.get_block(u+source_offset[0], v+source_offset[1], z+source_offset[2])
                    shape = src_block.volume.shape
                    start = [z*source.get_block_size(),
                             v*source.get_block_size(),
                             u*source.get_block_size()]
                    end = [start[0]+shape[0], start[1]+shape[1], start[2]+shape[2]]
                    self.volume[start[0]:end[0], start[1]:end[1], start[2]:end[2]] = src_block.volume[:]
