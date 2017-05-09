# BBIC Image stack reader
# Author: Raphael Dumusc 2015
#
# Copyright (c) BBP/EPFL 2014-2015; All rights reserved.
# Do not distribute without further notice.

import os
from PIL import Image
try:
    from mpi4py import MPI
except ImportError:
    pass
from .image_provider import ImageProvider


class ImageStack(ImageProvider):
    """Image stack reader"""
    def __init__(self, filename_pattern):
        self.filename_pattern = filename_pattern
        self.filenames = self._get_filenames(filename_pattern)
        self.num_slices = len(self.filenames)
        self.width = 0
        self.height = 0

    def _get_filenames(self, pattern):
        """Get the filenames of the input image slices."""
        filenames = []
        if pattern.find('%') == -1:
            with open(pattern, 'rb') as f:
                filenames = list(filter(lambda y: y != '', map(lambda x: x.strip(), f.read().decode('ascii').split('\n'))))
        else:
            start_idx = 0 if os.path.exists(pattern % 0) else 1
            slice_idx = start_idx
            while os.path.exists(pattern % slice_idx):
                filenames.append(pattern % slice_idx)
                slice_idx += 1
        return filenames

    def determine_stack_size(self, mpi_comm=None):
        """Compute overall stack width and height by opening all images."""
        if mpi_comm is None:
            mpi_size = 1
            mpi_rank = 0
        else:
            mpi_size = mpi_comm.Get_size()
            mpi_rank = mpi_comm.Get_rank()

        self.width = self.height = 0
        for filename in self.filenames[mpi_rank::mpi_size]:
            im = Image.open(filename)
            self.width = max(self.width, im.size[0])
            self.height = max(self.height, im.size[1])
        if mpi_comm is not None:
            self.width = mpi_comm.allreduce(self.width, op=MPI.MAX)
            self.height = mpi_comm.allreduce(self.height, op=MPI.MAX)

    def _expand_image(self, im, w, h, color='white'):
        """Return an image expanded to the given size"""
        dx = w - im.size[0]
        dy = h - im.size[1]
        if dx == dy == 0:
            return im
        out = Image.new(im.mode, (w, h), color)
        out.paste(im, (dx >> 1, dy >> 1))
        return out

    def get_dimensions(self):
        """Get the dimensions of the block source"""
        return self.width, self.height, self.num_slices

    def get_image(self, index, padding_value=0):
        """Get an image, padded to the stack dimensions"""
        im = Image.open(self.filenames[index])
        if im.mode != 'L':
            im = im.convert('L')
        im = self._expand_image(im, self.width, self.height, padding_value)
        return im

