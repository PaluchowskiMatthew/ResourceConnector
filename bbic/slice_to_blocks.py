# BBIC slice to blocks
# Author: Raphael Dumusc 2015
#
# Copyright (c) BBP/EPFL 2015; All rights reserved.
# Do not distribute without further notice.

from PIL import Image
import numpy as np
from .block_provider import BlockProvider
from .image_provider import ImageProvider
from .data_block import DataBlock


class SliceToBlocks(BlockProvider):
    """Reads slices from an image stack into blocks"""

    def __init__(self, image_provider, block_size):
        assert isinstance(image_provider, ImageProvider)
        assert isinstance(block_size, int)

        self.image_provider = image_provider
        self.block_size = block_size
        self.slice = None

    def get_block_size(self):
        """Get the size of the block that this source provides"""
        return self.block_size

    def get_block(self, u, v, z):
        """Get a block by its indices"""
        assert isinstance(u, int)
        assert isinstance(v, int)
        assert isinstance(z, int)

        if self.slice is None or self.slice.index is not z:
            del self.slice
            self.slice = Slice(z, self.block_size, self.image_provider)

        return self.slice.get_block(u, v)

    def get_dimensions(self):
        """Get the dimensions of the block source as a tuple (width, height, depth)"""
        return self.image_provider.get_dimensions()


class Slice:
    """A slice of a volume or image stack, of depth block_size"""
    def __init__(self, index, block_size, image_provider):
        assert isinstance(index, int)
        assert isinstance(block_size, int)
        assert isinstance(image_provider, ImageProvider)

        self.index = index
        self.block_size = block_size
        self.data = None
        self._read_data(image_provider)

    def get_block(self, u, v):
        """Get a block by its indices"""
        assert isinstance(u, int)
        assert isinstance(v, int)
        block = DataBlock(u, v, self.index, self.block_size)
        block.allocateAndSet(self.block_size, self.block_size, self.block_size, 0)
        x = u*self.block_size
        y = v*self.block_size
        end_x = min(x+self.block_size, self.data.shape[2])
        end_y = min(y+self.block_size, self.data.shape[1])
        dx = end_x-x
        dy = end_y-y
        block.volume[:, 0:dy, 0:dx] = self.data[:, y:end_y, x:end_x]
        return block

    def _read_data(self, image_provider):
        """Read the slice data from the image provider"""
        assert isinstance(image_provider, ImageProvider)

        dim = image_provider.get_dimensions()

        slice_start = self.index*self.block_size
        slice_end = min(slice_start + self.block_size, dim[2])

        self.data = np.zeros((self.block_size, dim[1], dim[0]), dtype=np.uint8)

        for slice_index in range(slice_start, slice_end):
            im = image_provider.get_image(slice_index)
            arr = np.array(im)
            self.data[slice_index-slice_start, :] = arr[:]
