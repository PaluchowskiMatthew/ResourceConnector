# BBIC stack
# Author: Raphael Dumusc 2015
#
# Copyright (c) BBP/EPFL 2014-2015; All rights reserved.
# Do not distribute without further notice.

import math
from datetime import datetime
from io import BytesIO
from PIL import Image
import numpy as np
from .data_block import DataBlock
from .block_provider import BlockProvider
from .image_provider import ImageProvider


class Stack:
    """A tiled image Stack consisting of 1 or more resolution Levels"""

    def __init__(self, stack_group, index):
        self.stack_group = stack_group
        self.index = index
        self.width = 0
        self.height = 0
        self.num_slices = 0
        self.tile_size = 0
        self.format = "JPEG"
        self.num_levels = 0
        self.is_video = False
        self.fps = 0
        self.description = ""
        self.original_filenames = ""
        self.local_to_world = self._get_local_to_world('Z')
        self.orientation = ''
        self.slice_positions = ''

    def __str__(self):
        return "Stack%d [%d, %d, %d], tile size: %d, #levels: %d, format: %s" % \
               (self.index, self.width, self.height, self.num_slices, self.tile_size, self.num_levels, self.format)

    def print_structure(self):
        """Print the detailed structure"""
        for l in range(self.num_levels):
            print(self.get_level(l))

    def read_attrs(self):
        """Read attributes from file"""
        self.width = int(self.stack_group.attrs["width"])
        self.height = int(self.stack_group.attrs["height"])
        self.num_slices = int(self.stack_group.attrs["num_slices"])
        self.tile_size = int(self.stack_group.attrs["tile_size"])
        self._read_format()
        self.num_levels = int(self.stack_group.attrs["num_levels"])
        self.is_video = self.stack_group.attrs["is_video"]
        self.fps = self.stack_group.attrs["fps"]
        self.description = self.stack_group.attrs["description"].decode('ascii')
        self.original_filenames = self.stack_group.attrs["original_filenames"].decode('ascii')
        self.local_to_world = self.stack_group.attrs["local_to_world"]
        self.orientation = self.stack_group.attrs["orientation"].decode('ascii')
        if "slice_positions" in self.stack_group:
            self.slice_positions = self.stack_group.attrs["slice_positions"].decode('ascii')

    def write_attrs(self):
        """Write attributes to file"""
        self.stack_group.attrs.create("width", self.width)
        self.stack_group.attrs.create("height", self.height)
        self.stack_group.attrs.create("num_slices", self.num_slices)
        self.stack_group.attrs.create("tile_size", self.tile_size)
        self._write_format()
        self.stack_group.attrs.create("num_levels", self.num_levels)
        self.stack_group.attrs.create('is_video', self.is_video)
        self.stack_group.attrs.create('fps', self.fps)
        self.stack_group.attrs.create('description', self.description.encode('ascii'))
        self.stack_group.attrs.create('original_filenames', self.original_filenames.encode('ascii'))
        self.stack_group.attrs.create('local_to_world', self.local_to_world)
        self.stack_group.attrs.create('orientation', self.orientation.encode('ascii'))
        self.stack_group.attrs.create('slice_positions', self.slice_positions.encode('ascii'))
        self.update_modify_time()

    def set_axis(self, axis='Z'):
        self.local_to_world = self._get_local_to_world(axis)

    def _get_local_to_world(self, axis):
        mat = np.eye(4)
        mat[0, 3] = -(self.width >> 1)
        mat[1, 3] = -(self.height >> 1)
        mat[2, 3] = -(self.num_slices >> 1)
        if axis is not None:
            if axis == 'X':
                rot = np.array([0, 0, 1, 0,
                                1, 0, 0, 0,
                                0, 1, 0, 0,
                                0, 0, 0, 1]).reshape((4, 4)).T
            elif axis == 'Y':
                rot = np.array([0, 0, 1, 0,
                                0, -1, 0, 0,
                                1, 0, 0, 0,
                                0, 0, 0, 1]).reshape((4, 4)).T
                flip = np.eye(4)
                flip[1, 1] = -1
                rot = np.dot(flip, rot)
            else:
                rot = np.array([1, 0, 0, 0,
                                0, 1, 0, 0,
                                0, 0, 1, 0,
                                0, 0, 0, 1]).reshape((4, 4)).T
            mat = np.dot(rot, mat)
        return mat

    def set_slice_positions(self, slice_positions_file):
        if slice_positions_file is not None:
            with open(slice_positions_file, 'rb') as f:
                self.slice_positions = map(lambda x: float(x.strip()), f.read().split('\n'))

    def get_dimensions(self):
        return self.width, self.height, self.num_slices

    def get_level(self, level_index):
        """Get a level of the stack, creating it if it does not exist"""
        assert isinstance(level_index, int)
        nx = math.ceil(float(self.width >> level_index) / self.tile_size)
        ny = math.ceil(float(self.height >> level_index) / self.tile_size)
        index = 'levels/%d' % level_index
        if index not in self.stack_group:
            level_group = self.stack_group.create_group(index)
            level = StackLevel(level_group, level_index, self.tile_size)
            level.num_x_tiles = int(nx)
            level.num_y_tiles = int(ny)
            level.num_slices = self.num_slices
            level.write_attrs()
        else:
            level = StackLevel(self.stack_group[index], level_index, self.tile_size)
            level.read_attrs()
        level.width = self.width >> level_index
        level.height = self.height >> level_index
        return level

    def _read_format(self):
        """Determine format from the type attribute"""
        type = self.stack_group.attrs["type"]
        format = type.decode('ascii').replace('image/', '')
        self.format = format.upper()

    def _write_format(self):
        """Write the type attribute based on the format"""
        self.stack_group.attrs.create('type', ('image/' + self.format.lower()).encode('ascii'))

    def update_modify_time(self):
        self.stack_group.attrs.create('modify_time', str(datetime.utcnow()).encode('ascii'))

    def compute_num_levels(self, limit_to_tile_size=False):
        """Compute the number of levels that this stack should contain"""
        if limit_to_tile_size:
            # New proposed format: stop dividing when the dimensions of the
            # level reach tile_size
            return int(math.ceil(math.log(max(self.width, self.height), 2) -
                                 math.log(self.tile_size, 2))) + 1
        else:
            # Old format: create levels until reaching the size of 1 pixel
            # in width or height
            return int(math.floor(math.log(min(self.width, self.height), 2))) + 1

    def create_levels(self, print_info, generate_lods=True):
        """Create the levels for the stack"""
        assert isinstance(print_info, bool)
        assert isinstance(generate_lods, bool)

        self.num_levels = self.compute_num_levels() if generate_lods else 1
        self.stack_group.attrs.create('num_levels', self.num_levels)
        levels = [None] * self.num_levels
        for level in range(0, self.num_levels):
            levels[level] = self.get_level(level)
            if print_info:
                print('level %d: (%d,%d) tiles, %d slices, tile size: %d' %
                      (level, levels[level].num_x_tiles,
                       levels[level].num_y_tiles, self.num_slices,
                       self.tile_size))
        return levels


class StackLevel(BlockProvider, ImageProvider):
    """A resolution level from a Stack"""

    def __init__(self, level_group, index, tile_size):
        assert isinstance(index, int)
        assert isinstance(tile_size, int)
        self.level_group = level_group
        self.index = index
        self.tile_size = tile_size
        self.num_x_tiles = 0
        self.num_y_tiles = 0
        self.num_slices = 0
        self.width = 0
        self.height = 0

    def __str__(self):
        return "StackLevel%d [%d, %d, %d], tile size: %d, #tiles: (%d, %d)" % \
               (self.index, self.width, self.height, self.num_slices,
                self.tile_size, self.num_x_tiles, self.num_y_tiles)

    def read_attrs(self):
        """Read the attributes from file"""
        self.num_x_tiles = int(self.level_group.attrs['num_x_tiles'])
        self.num_y_tiles = int(self.level_group.attrs['num_y_tiles'])
        self.num_slices = int(self.level_group.attrs['num_slices'])

    def write_attrs(self):
        """Write the attributes from file"""
        self.level_group.attrs.create('num_x_tiles', self.num_x_tiles)
        self.level_group.attrs.create('num_y_tiles', self.num_y_tiles)
        self.level_group.attrs.create('num_slices', self.num_slices)

    def get_tile(self, u, v, slice_index):
        """Get a tile of the given slice as an Image"""
        assert isinstance(u, int)
        assert isinstance(v, int)
        assert isinstance(slice_index, int)

        tile_id = '%d/%d/%d' % (slice_index, u, v)
        tile = self.level_group[tile_id]

        data = tile[:].tostring()
        bytes_buffer = BytesIO(data)
        im = Image.open(bytes_buffer)
        return im

    def get_block_size(self):
        """Get the size of the blocks, equivalent to tile_size"""
        return self.tile_size

    def get_block(self, u, v, z):
        """Get a block of tile_size^3 starting from the given u, v, z
        coordinate"""
        assert isinstance(u, int)
        assert isinstance(v, int)
        assert isinstance(z, int)

        slice_start = z*self.tile_size
        slice_end = min(slice_start + self.tile_size, self.num_slices)
        block_depth = slice_end - slice_start

        block = DataBlock(u, v, z, self.tile_size)
        for slice_index in range(slice_start, slice_end):
            im = self.get_tile(u, v, slice_index)
            arr = np.array(im)
            if block.volume is None:
                block.allocate(im.size[0], im.size[1], block_depth)
            block.volume[slice_index-slice_start, :] = arr[:]
        return block

    def get_dimensions(self):
        """Get the dimensions of the stack"""
        return self.width, self.height, self.num_slices

    def get_block_count(self):
        """Get the number of blocks of size <tile_size> that can be formed
        from this stack"""
        num_blocks_x = self.num_x_tiles
        num_blocks_y = self.num_y_tiles
        num_blocks_z = math.ceil(float(self.num_slices) / self.tile_size)
        return num_blocks_x, num_blocks_y, num_blocks_z

    def get_block_list(self):
        """Get the list of all the block in this level,
        uninitialized (no data)"""
        num_blocks = self.get_block_count()
        block_indices = []
        for z in range(num_blocks[2]):
            for v in range(num_blocks[1]):
                for u in range(num_blocks[0]):
                    block_indices.append(DataBlock(u, v, z, self.tile_size))
        return block_indices

    def get_image(self, slice_index, padding=0):
        """Get a full slice from this Level of the stack"""
        assert isinstance(slice_index, int)
        assert isinstance(padding, int)

        tiles = []
        width = height = 0
        for v in range(self.num_y_tiles):
            tiles.append([])
            for u in range(self.num_x_tiles):
                tile = self.get_tile(u, v, slice_index)
                tiles[v].append(tile)
                if v == 0:
                    width += tile.size[0]
            height += tile.size[1]

        image = Image.new('L', (width, height), padding)
        for v in range(self.num_y_tiles):
            for u in range(self.num_x_tiles):
                pos = (u * self.tile_size, v * self.tile_size)
                image.paste(tiles[v][u], pos)
        return image

    def allocate_tile(self, size, u, v, slice_index):
        """Allocate a dataset for the given tile"""
        assert isinstance(u, int)
        assert isinstance(v, int)
        assert isinstance(slice_index, int)

        tile_id = '%d/%d/%d' % (slice_index, u, v)
        self.level_group.create_dataset(tile_id, (size,), np.uint8)

    def store_tile(self, tile, u, v, slice_index):
        """Store a serialized tile, creating the dataset
        if it does not exists"""
        assert isinstance(u, int)
        assert isinstance(v, int)
        assert isinstance(slice_index, int)

        tile_id = '%d/%d/%d' % (slice_index, u, v)
        if tile_id not in self.level_group:
            self.level_group.create_dataset(tile_id, data=tile)
        else:
            dataset = self.level_group[tile_id]
            dataset[:] = tile

    def extract_slices(self, outputdir, format, mpi_comm=None):
        """Write the stack to disk as a collection of images"""
        mpi_stride = 1 if mpi_comm is None else mpi_comm.Get_size()
        for i in range(0, self.num_slices, mpi_stride):
            im = self.get_image(i)
            im.save('%s/%d.%s' % (outputdir, i, format))
