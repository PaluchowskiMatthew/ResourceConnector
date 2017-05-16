# BBIC block provider
# Author: Raphael Dumusc 2015
#
# Copyright (c) BBP/EPFL 2014-2015; All rights reserved.
# Do not distribute without further notice.


class BlockProvider:
    """Abstract class for block providers"""

    def get_block_size(self):
        """Get the size of the block that this source provides"""
        raise NotImplementedError

    def get_block(self, u, v, z):
        """Get a block by its indices"""
        raise NotImplementedError

    def get_dimensions(self):
        """Get the dimensions of the block source as a tuple (width, height, depth)"""
        raise NotImplementedError
