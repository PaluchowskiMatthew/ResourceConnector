# BBIC image utilities
# Author: Raphael Dumusc 2015
#
# Copyright (c) BBP/EPFL 2014-2015; All rights reserved.
# Do not distribute without further notice.

from io import BytesIO
from PIL import Image
import numpy as np


def compress_and_serialize(image, format_):
    """Return a byte array containing the serialized image, compressed in the desired format"""
    bytes_buffer = BytesIO()
    image.save(bytes_buffer, format_)
    array = np.fromstring(bytes_buffer.getvalue(), dtype=np.uint8)
    if len(array) == 0:
        raise Exception('zero-length image')
    return array


def get_compressed_tile(im, x, y, tile_size, format_):
    """Get a tile from an image at the given position"""
    (w, h) = im.size
    w1 = min(tile_size, w - x)
    h1 = min(tile_size, h - y)
    tile = im.crop([x, y, x + w1, y + h1])
    return compress_and_serialize(tile, format_)
