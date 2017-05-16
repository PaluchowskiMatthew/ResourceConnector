# BBIC image provider
# Author: Raphael Dumusc 2015
#
# Copyright (c) BBP/EPFL 2014-2015; All rights reserved.
# Do not distribute without further notice.


class ImageProvider:
    """Abstract class for image providers"""

    def get_dimensions(self):
        """Get the dimensions of the image source as a tuple (width, height, num_slices)"""
        raise NotImplementedError

    def get_image(self, slice_index, padding_value=0):
        """Get an image by its slice index, padded with the given value (if needed)"""
        raise NotImplementedError
