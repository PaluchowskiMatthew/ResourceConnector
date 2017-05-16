# BBIC volume
# Author: Raphael Dumusc 2015
#
# Copyright (c) BBP/EPFL 2014-2015; All rights reserved.
# Do not distribute without further notice.

import math
from PIL import Image
import numpy as np
import scipy.ndimage.filters as filter
import scipy.ndimage.interpolation as interp
from .data_block import DataBlock
from .block_provider import BlockProvider

VOLUME_VERSION_UNKNOWN=0
VOLUME_VERSION_ORIGINAL=1
VOLUME_VERSION_CURRENT=2

def get_indices(axis):
    """Get the indices for reading the volume along the given axis"""
    if axis == 0:
        return 0, 1, 2
    elif axis == 1:
        return 1, 0, 2
    elif axis == 2:
        return 2, 0, 1
    else:
        raise ValueError("Invalid axis")


class Volume:
    """A volume stored as blocks in a multi-resolution octree"""

    def __init__(self, volume_group):
        self.volume_group = volume_group
        self.width = 0
        self.height = 0
        self.depth = 0
        self.block_size = 0
        self.orientation = ""
        self.version = VOLUME_VERSION_CURRENT

    def read_attrs(self):
        """Read the attributes from file"""
        self.width = int(self.volume_group.attrs["width"])
        self.height = int(self.volume_group.attrs["height"])
        self.depth = int(self.volume_group.attrs["num_slices"])
        self.block_size = int(self.volume_group.attrs["tile_size"])
        self.orientation = self.volume_group.attrs["orientation"]
        self.version = int(self.volume_group.attrs["version"]) if "version" in self.volume_group.attrs else VOLUME_VERSION_UNKNOWN

    def write_attrs(self):
        """Write the attributes to the file"""
        self.volume_group.attrs["width"] = self.width
        self.volume_group.attrs["height"] = self.height
        self.volume_group.attrs["num_slices"] = self.depth
        self.volume_group.attrs["tile_size"] = self.block_size
        self.volume_group.attrs["orientation"] = self.orientation
        self.volume_group.attrs["version"] = self.version

    def __str__(self):
        return "Volume v%d [%d, %d, %d], block size: %d, #blocks %s" % \
               (self.version, self.width, self.height, self.depth, self.block_size, self.get_blocks_count())

    def print_structure(self):
        for l in range(self.get_lod_count()):
            print("Level %d: #blocks %s, size %s" % (l, self.get_blocks_count(l), self.get_dimensions(l)))

    def get_lod_count(self):
        """Get the number of LOD"""
        (nw, nh, nz) = self.get_blocks_count()
        return int(math.floor(math.log(min(nw, nh, nz) * self.block_size, 2))) + 1

    def get_dimensions(self, level=0):
        """Get the volume dimensions"""
        assert isinstance(level, int)
        return self.width >> level, self.height >> level, self.depth >> level

    def get_blocks_count(self, level=0):
        """Get the number of blocks in each dimension for the given level"""
        assert isinstance(level, int)
        num_blocks_x = int(math.ceil(float(self.width >> level) / self.block_size))
        num_blocks_y = int(math.ceil(float(self.height >> level) / self.block_size))
        num_blocks_z = int(math.ceil(float(self.depth >> level) / self.block_size))
        return num_blocks_x, num_blocks_y, num_blocks_z

    def get_lod(self, lod_index):
        """Get a LOD of the volume"""
        assert isinstance(lod_index, int)

        index = "levels/%d" % lod_index
        if index not in self.volume_group:
            return None

        lod = VolumeLOD(self.volume_group[index], lod_index, self.block_size)
        lod.read_attrs()
        if self.version < 1:
            # Overwrite wrong legacy dimensions with correct ones...
            (lod.width, lod.height, lod.depth) = self.get_dimensions(lod_index)
            lod.num_blocks = self.get_blocks_count(lod_index)
        return lod

    def fill(self, source, block_size):
        """Fill the volume using the given source"""
        assert isinstance(source, BlockProvider)
        assert isinstance(block_size, int)

        self.width, self.height, self.depth = source.get_dimensions()
        self.block_size = block_size
        self.write_attrs()

        for lod_index in range(self.get_lod_count()):
            self._create_lod(lod_index, True)

        self._fill_lods(source)

    def _create_lod(self, lod_index, pre_allocate=False):
        """Create a LOD of the volume"""
        assert isinstance(lod_index, int)

        index = "levels/%d" % lod_index
        self.volume_group.create_group(index)
        lod = VolumeLOD(self.volume_group[index], lod_index, self.block_size)
        (lod.width, lod.height, lod.depth) = self.get_dimensions(lod_index)
        lod.num_blocks = self.get_blocks_count(lod_index)
        lod.write_attrs()
        if pre_allocate:
            lod.allocate_all_blocks()
        return lod

    def _fill_lods(self, source):
        """Fill all the LODs of the volume"""
        lod0 = self.get_lod(0)
        print("Filling", lod0, "...")
        lod0.fill(source)

        for level in range(1, self.get_lod_count()):
            lod = self.get_lod(level)
            print("Filling", lod, "...")
            lod.fill(VolumeLODDownsampler(self.get_lod(level-1)))


class VolumeLOD(BlockProvider):
    """A single LOD of a volume's octree"""

    def __init__(self, level_group, level, block_size):
        assert isinstance(level, int)
        assert isinstance(block_size, int)
        self.group = level_group
        self.level = level
        self.block_size = block_size
        self.num_blocks = (0, 0, 0)
        self.width = 0
        self.height = 0
        self.depth = 0

    def read_attrs(self):
        """Read the attributes from file"""
        self.num_blocks = self._read_num_blocks()
        # Compatibility with old volume file which don't store this filed
        attrs = self.group.attrs
        self.width = attrs["width"] if "width" in attrs else self.num_blocks[0] * self.block_size
        self.height = attrs["height"] if "height" in attrs else self.num_blocks[1] * self.block_size
        self.depth = attrs["depth"] if "depth" in attrs else self.num_blocks[2] * self.block_size

    def _read_num_blocks(self):
        attrs = self.group.attrs
        return int(attrs["num_x_tiles"]), int(attrs["num_y_tiles"]), int(attrs["num_z_tiles"])

    def write_attrs(self):
        """Write the attributes to the file"""
        attrs = self.group.attrs
        attrs["num_x_tiles"] = self.num_blocks[0]
        attrs["num_y_tiles"] = self.num_blocks[1]
        attrs["num_z_tiles"] = self.num_blocks[2]
        attrs["width"] = self.width
        attrs["height"] = self.height
        attrs["depth"] = self.depth

    def __str__(self):
        return "VolumeLOD %d [%d, %d, %d], block size: %d, #blocks %s" % \
               (self.level, self.width, self.height, self.depth, self.block_size, self.num_blocks)

    def get_block_size(self):
        """Get the size of the blocks"""
        return self.block_size

    def get_block(self, u, v, z):
        """Get a block of the volume by its index"""
        assert isinstance(u, int)
        assert isinstance(v, int)
        assert isinstance(z, int)

        if not self._block_indices_valid(u, v, z):
            raise ValueError("Invalid block requested")

        index = "%d/%d/%d" % (u, v, z)
        if not index in self.group:
            self._allocate_block(u, v, z)

        block = DataBlock(u, v, z, self.block_size)
        block.width, block.height, block.depth = self._get_block_size(u, v, z)
        block.volume = self.group[index]
        return block

    def get_dimensions(self):
        """Get the dimensions of the LOD"""
        return self.width, self.height, self.depth

    def _get_block_size(self, x, y, z):
        """Get the size of a block"""
        assert isinstance(x, int)
        assert isinstance(y, int)
        assert isinstance(z, int)

        if x < 0 or x > self.num_blocks[0] or y < 0 or y > self.num_blocks[1] or z < 0 or z > self.num_blocks[2]:
            return 0, 0, 0

        padding = (self.width % self.block_size, self.height % self.block_size, self.depth % self.block_size)
        width = padding[0] if x == self.num_blocks[0]-1 and padding[0] > 0 else self.block_size
        height = padding[1] if y == self.num_blocks[1]-1 and padding[1] > 0 else self.block_size
        depth = padding[2] if z == self.num_blocks[2]-1 and padding[2] > 0 else self.block_size
        return width, height, depth

    def _block_indices_valid(self, x, y, z):
        """Check if the given block indices exist in this LOD"""
        assert isinstance(x, int)
        assert isinstance(y, int)
        assert isinstance(z, int)

        if x < 0 or x >= self.num_blocks[0] or y < 0 or y >= self.num_blocks[1] or z < 0 or z >= self.num_blocks[2]:
            return False
        return True

    def allocate_all_blocks(self):
        """Pre allocate all the dataset for the blocks in this lod"""
        for z in range(self.num_blocks[2]):
            for v in range(self.num_blocks[1]):
                for u in range(self.num_blocks[0]):
                    self._allocate_block(u, v, z)

    def _allocate_block(self, u, v, z):
        """Get a block of the volume by its index"""
        assert isinstance(u, int)
        assert isinstance(v, int)
        assert isinstance(z, int)
        assert self._block_indices_valid(u, v, z)

        index = "%d/%d/%d" % (u, v, z)
        self.group.create_dataset(index, shape=(self.block_size, self.block_size, self.block_size),
                                  dtype=np.uint8, compression='lzf')

    def extract_slices(self, outputdir, format, axis=0, mpi_comm=None):
        """Write the volume to disk as a stack of images"""
        tile_size = self.block_size
        ntiles = (self.num_blocks[2], self.num_blocks[1], self.num_blocks[0])
        dim = (self.depth, self.height, self.width)

        mpi_stride = 1 if mpi_comm is None else mpi_comm.Get_size()
        n_images = self.block_size  # thickness of the temporary slice, can be reduced if memory consumption is too high
        (outer_dim, inner_dim1, inner_dim2) = get_indices(axis)
        for outer in range(0, dim[outer_dim], n_images*mpi_stride):
            slice = np.zeros((n_images, ntiles[inner_dim1] * tile_size, ntiles[inner_dim2] * tile_size), dtype=np.uint8)
            for inner1 in range(ntiles[inner_dim1]):
                for inner2 in range(ntiles[inner_dim2]):
                    idx = [0] * 3
                    idx[outer_dim] = int(outer / tile_size)
                    idx[inner_dim1] = inner1
                    idx[inner_dim2] = inner2
                    block = self.get_block(idx[2], idx[1], idx[0])
                    dset = block.volume[:]
                    for n in range(min(n_images, dim[outer_dim] - outer)):
                        ref = [None] * 3
                        ref[outer_dim] = (outer + n) % tile_size
                        ref[inner_dim1] = Ellipsis
                        ref[inner_dim2] = Ellipsis
                        slice[n, inner1*tile_size:(inner1+1)*tile_size, inner2*tile_size:(inner2+1)*tile_size] = dset[ref]
            for n in range(min(n_images, dim[outer_dim] - outer)):
                im = Image.fromarray(np.squeeze(slice[n, :, :]))
                im = im.crop((0, 0, dim[inner_dim2], dim[inner_dim1]))
                im.save('%s/%d.%s' % (outputdir, outer + n, format))

    def fill(self, source):
        """Fill from a source of same dimensions and compatible block size"""
        assert isinstance(source, BlockProvider)

        if(source.get_dimensions() != self.get_dimensions()):
            raise ValueError("Source has different dimensions")

        # Simple case: source blocks have exactly the same size
        if source.get_block_size() == self.block_size:
            for z in range(self.num_blocks[2]):
                for v in range(self.num_blocks[1]):
                    for u in range(self.num_blocks[0]):
                        block = self.get_block(u, v, z)
                        src_block = source.get_block(u, v, z)
                        shape = src_block.volume.shape
                        block.volume[0:shape[0], 0:shape[1], 0:shape[2]] = src_block.volume[:]

        # Second case: source blocks are larger
        elif source.get_block_size() > self.block_size:
            if source.get_block_size() % self.block_size != 0:
                raise ValueError("Incompatible block sizes between source and target!")

            # Iterate over source blocks
            stride = int(source.get_block_size() / self.block_size)
            for z in range(source.get_block_count()[2]):
                for v in range(source.get_block_count()[1]):
                    for u in range(source.get_block_count()[0]):
                        subblocks = source.get_block(u, v, z).split(self.block_size)
                        for subblock in subblocks:
                            self.get_block(subblock.u+u*stride, subblock.v+v*stride, subblock.z+z*stride).copy(subblock)

        # Last case: source blocks are smaller
        else:
            if self.block_size % source.get_block_size() != 0:
                raise ValueError("Incompatible block size between source and target!")

            # Iterate over target blocks
            stride = int(self.block_size / source.get_block_size())
            for z in range(self.num_blocks[2]):
                for v in range(self.num_blocks[1]):
                    for u in range(self.num_blocks[0]):
                        block = self.get_block(u, v, z)
                        block.fill(source, (u*stride, v*stride, z*stride))


class VolumeLODDownsampler(BlockProvider):
    """A source providing downsampled blocks from a VolumeLOD"""

    def __init__(self, lod, filter_size=2):
        assert isinstance(lod, VolumeLOD)
        assert isinstance(filter_size, int)

        self.lod = lod
        self.filter_size = filter_size

    def get_block_size(self):
        """Get the size of the blocks"""
        return self.lod.block_size

    def get_block(self, u, v, z):
        """Get a block"""
        assert isinstance(u, int)
        assert isinstance(v, int)
        assert isinstance(z, int)

        meta_block_size = 2*self.get_block_size()
        meta_block = DataBlock(u, v, z, meta_block_size)
        meta_block.allocateAndSet(meta_block_size, meta_block_size, meta_block_size, 0)
        meta_block.width, meta_block.height, meta_block.depth = self._get_meta_block_size(u, v, z)
        meta_block.fill(self.lod, (2*u, 2*v, 2*z))
        return self._downsample(meta_block)

    def get_dimensions(self):
        """Get the dimensions of the downsampled Volume LOD"""
        dim = self.lod.get_dimensions()
        return dim[0] >> 1, dim[1] >> 1, dim[2] >> 1

    def _get_meta_block_size(self, u, v, z):
        """Get the size of a meta block"""
        assert isinstance(u, int)
        assert isinstance(v, int)
        assert isinstance(z, int)

        meta_block_size = 2*self.get_block_size()
        width = min(meta_block_size, self.lod.width - u*meta_block_size)
        height = min(meta_block_size, self.lod.height - v*meta_block_size)
        depth = min(meta_block_size, self.lod.depth - z*meta_block_size)
        return width, height, depth

    def _downsample(self, meta_block):
        """Get a copy of the meta-block, median-filtered and downsampled by 2"""
        assert isinstance(meta_block, DataBlock)

        block = DataBlock(0, 0, 0, meta_block.nominal_size >> 1)
        block.width, block.height, block.depth = meta_block.width >> 1, meta_block.height >> 1, meta_block.depth >> 1
        tmp_data = filter.median_filter(meta_block.volume, self.filter_size)
        block.volume = interp.zoom(tmp_data, 0.5, order=0, prefilter=False)
        return block
