# BBIC file reader
# Author: Jafet Villafranca, 2016
#         Raphael Dumusc,    2015
#
# Copyright (c) BBP/EPFL 2014-2015; All rights reserved.
# Do not distribute without further notice.

import h5py
import sys

from .volume import *
from .stack import *
from .image_utils import get_compressed_tile

BBIC_UNKNOWN_VERSION = 0
BBIC_CURRENT_VERSION = 1

import tqdm

class File:
    """Read/write BBIC volumes to/from hdf5"""

    def __init__(self, filename, mode='r', mpi_comm=None):
        """Open a volume file"""
        self.version = BBIC_UNKNOWN_VERSION
        self.filename = filename
        self.mpi_comm = mpi_comm
        if mpi_comm is not None:
            self.mpi_size = mpi_comm.Get_size()
            self.mpi_rank = mpi_comm.Get_rank()
        else:
            self.mpi_size = 1
            self.mpi_rank = 0
        self._print_info = (self.mpi_rank == 0)
        self.bbic = None
        self.num_stacks = 0
        self.num_volumes = 0
        self._open(mode)

    def __del__(self):
        """Close the file"""
        self.h5file.close()

    def _open(self, mode):
        """Open the h5 file"""
        if self.mpi_comm is not None and self.mpi_comm.Get_size() > 1:
            if not h5py.get_config().mpi:
                raise RuntimeError("ERROR: h5py is lacking MPI support, aborting!")
            self.h5file = h5py.File(self.filename, mode, driver='mpio', comm=self.mpi_comm)
        else:
            self.h5file = h5py.File(self.filename, mode)
        self.bbic = self.h5file.require_group('bbic')
        self._read_attrs()
        if mode is not 'r':
            self.version = BBIC_CURRENT_VERSION
            self._write_attrs()

    def _read_attrs(self):
        """Write file-level attributes"""
        if 'version' in self.bbic.attrs:
            self.version = self.bbic.attrs['version']
        if 'num_stacks' in self.bbic.attrs:
            self.num_stacks = self.bbic.attrs['num_stacks']
        if 'num_volumes' in self.bbic.attrs:
            self.num_volumes = self.bbic.attrs['num_volumes']

    def _write_attrs(self):
        """Write file-level attributes"""
        self.bbic.attrs.create('version', self.version)
        self.bbic.attrs.create('num_stacks', self.num_stacks)
        self.bbic.attrs.create('num_volumes', self.num_volumes)

    def close_and_reopen(self, mode='r'):
        self.h5file.close()
        self._open(mode)

    def __str__(self):
        return "BBIC file v%d - %d stacks, %d volumes" % (self.version, self.num_stacks, self.num_volumes)

    def get_volume(self, volume_index=0):
        """Get a volume by its index"""
        assert isinstance(volume_index, int)

        index = 'volumes/%d' % volume_index
        if index not in self.bbic:
            return None

        volume = Volume(self.bbic[index])
        volume.read_attrs()
        return volume

    def create_volume(self, volume_index=0):
        """Create an empty volume with the given index"""
        assert isinstance(volume_index, int)

        index = 'volumes/%d' % volume_index
        self.bbic.create_group(index)
        self.num_volumes = max(self.num_volumes, volume_index+1)
        self.bbic.attrs['num_volumes'] = self.num_volumes

        volume = Volume(self.bbic[index])
        volume.write_attrs()
        return volume

    def get_stack(self, stack_index=0):
        """Get a stack by its index"""
        assert isinstance(stack_index, int)

        index = 'stacks/%d' % stack_index
        if index not in self.bbic:
            return None
        stack = Stack(self.bbic[index], stack_index)
        stack.read_attrs()
        return stack

    def create_stack(self, stack_index=0):
        """Create an empty stack with the given index"""
        assert isinstance(stack_index, int)

        stack_path = 'stacks/%d' % stack_index
        stack_group = self.bbic.create_group(stack_path)
        self.num_stacks = max(self.num_stacks, stack_index+1)
        self.bbic.attrs.create('num_stacks', self.num_stacks)

        stack = Stack(stack_group, stack_index)
        stack.write_attrs()
        return stack

    def _all_store_tiles(self, local_tiles, local_tile_sizes, levels, local_slice_index):
        """Store serialized tiles in the level group"""

        if self.mpi_comm is not None:
            # create empty tile datasets across all MPI processes
            all_tile_sizes = self.mpi_comm.allgather(local_tile_sizes)

            for i in range(len(all_tile_sizes)):
                for l in range(len(levels)):
                    slice_index = local_slice_index - self.mpi_comm.Get_rank() + i
                    if slice_index >= levels[l].num_slices:
                        pass
                    for v in range(0, len(all_tile_sizes[i][l])):
                        for u in range(0, len(all_tile_sizes[i][l][v])):
                            if all_tile_sizes[i][l][v][u] > 0:
                                levels[l].allocate_tile(all_tile_sizes[i][l][v][u], u, v, slice_index)

        # write local tiles
        for l in range(len(levels)):
            if local_slice_index >= levels[l].num_slices:
                pass
            for v in range(0, len(local_tile_sizes[l])):
                for u in range(0, len(local_tile_sizes[l][v])):
                    if local_tile_sizes[l][v][u] > 0:
                        levels[l].store_tile(local_tiles[l][v][u], u, v, local_slice_index)

    def _export_image_to_tiles(self, im, levels, slice_index, tile_size, format_, filter_):
        """Split an image into tiles and write them in the level_group"""
        tiles = []
        tile_sizes = []
        for l in range(len(levels)):
            (w, h) = im.size
            tiles.append([])
            tile_sizes.append([])
            for y in range(0, h, tile_size):
                index = int(y/tile_size)
                tiles[l].append([])
                tile_sizes[l].append([])
                for x in range(0, w, tile_size):
                    tile = get_compressed_tile(im, x, y, tile_size, format_)
                    tiles[l][index].append(tile)
                    tile_sizes[l][index].append(len(tile))
            im = im.resize((im.size[0] >> 1, im.size[1] >> 1) if im.size[0] >> 1 > 0 else (1,1), filter_)

        self._all_store_tiles(tiles, tile_sizes, levels, slice_index)

    def _wait_all(self, levels, slice_index):
        """Wait for other processes to finish export_image_to_tiles()"""
        tiles = [[[0 for u in range(level.num_x_tiles)] for v in range(level.num_y_tiles)] for level in levels]
        tile_sizes = [[[0 for u in range(level.num_x_tiles)] for v in range(level.num_y_tiles)] for level in levels]
        self._all_store_tiles(tiles, tile_sizes, levels, slice_index)

    def write(self, image_source, stack, padding_value, interp, start_offset=0,
              level_offset=0, generate_lods=True, reverse=False):
        """Write the BBIC image stack"""
        assert isinstance(image_source, ImageProvider)
        assert isinstance(stack, Stack)
        assert isinstance(start_offset, int)
        assert isinstance(level_offset, int)
        assert isinstance(generate_lods, bool)
        filter_ = {'nearest': Image.NEAREST, 'linear': Image.LINEAR}[interp]

        if self._print_info:
            total_size = int(stack.width * stack.height * stack.num_slices / (1000*1000))
            print('Target stack:(%dx%dx%d) [w/h/slices], %d MB (uncompressed)' %
                  (stack.width, stack.height, stack.num_slices, total_size))

        if self._print_info:
            print('Creating level groups...')
        levels = stack.create_levels(self._print_info, generate_lods)

        if self._print_info:
            print("Processing slices " + str(start_offset) + " to " + str(stack.num_slices-1) + "...")

        for index in range(self.mpi_rank + start_offset, stack.num_slices, self.mpi_size):
            slice_index = index
            if reverse:
                slice_index = stack.num_slices - 1 - index
            im = image_source.get_image(slice_index, padding_value)
            if level_offset > 0:
                im = im.resize((im.size[0] >> level_offset, im.size[1] >> level_offset), filter_)
            if reverse:
                from PIL import ImageOps
                im = ImageOps.mirror(im)
            self._export_image_to_tiles(im, levels[level_offset:], index, stack.tile_size, stack.format, filter_)
            if self._print_info:
                self._print_progress(index, stack.num_slices)

        # Let other mpi processes finish their image
        extra_slices = stack.num_slices % self.mpi_size
        if extra_slices > 0 and self.mpi_rank >= extra_slices:
            slice_index = stack.num_slices - (stack.num_slices % self.mpi_size) + self.mpi_rank
            self._wait_all(levels[level_offset:], slice_index)

        # Wait for all processes to be done filling the stack before returning
        if self.mpi_comm is not None:
            self.mpi_comm.barrier()

        if self._print_info:
            self._print_progress(stack.num_slices-1, stack.num_slices)
            print()
            print('Done.')

    def _print_progress(self, slice_index, num_slices):
        """Print the progression on a single line."""
        if self.mpi_comm is None:
            #sys.stdout.write("\rProgress: %i/%i" % (slice_index+1, num_slices))
            print("\rProgress: %i/%i" % (slice_index+1, num_slices))
        else:
            percent = float(slice_index+1) / num_slices * 100
            #sys.stdout.write("\rProgress: %i%%" % min(percent, 100))
            print("\rProgress: %i%%" % min(percent, 100))
        #sys.stdout.flush()

    def make_all_stacks(self, source_stack, padding_value,
                        interp, generate_lods):
        """Make stacks for the projections in the rest of the dimensions based
        on the layer0 of the source_stack.
        For simplicity, it will always perform the operations needed to
        generate the projections corresponding to the left and upper faces of
        the volume, viewed from the source stack point of view"""
        assert isinstance(source_stack, Stack)
        assert isinstance(padding_value, int)
        assert isinstance(generate_lods, bool)

        #TODO: Progress init
        #pbar = tqdm.tqdm(total=3)

        all_stacks = ['X', 'Y', 'Z']

        # remove the stack that we already have, so we keep only the ones that
        # correspond to the left and upper faces of the volume (in that order)
        stacks_to_generate = list(all_stacks)
        del stacks_to_generate[source_stack.index]

        # if the input stack is the X projection, then the order of the
        # remaining stacks to generate should be inverted [Y, Z] => [Z, Y]
        # so they continue being the left and upper faces of the volume
        if source_stack.index == 0:
            stacks_to_generate.reverse()

        if self._print_info:
            print('Creating stacks for the', stacks_to_generate,
                  'projections...')

        left_stack = self.create_stack(all_stacks.index(stacks_to_generate[0]))
        if source_stack.index == 1:  # volume is viewed from the upper face (Y)
            left_stack.width = source_stack.height
            left_stack.height = source_stack.num_slices
        else:  # volume viewed from one of the side faces (X or Z)
            left_stack.width = source_stack.num_slices
            left_stack.height = source_stack.height
        left_stack.num_slices = source_stack.width
        left_stack.tile_size = source_stack.tile_size
        left_stack.format = source_stack.format
        left_stack.set_axis(stacks_to_generate[0])
        left_stack.write_attrs()

        upper_stack = self.create_stack(all_stacks.index(stacks_to_generate[1]))
        if source_stack.index == 0:  # viewed from the X projection face
            upper_stack.width = source_stack.num_slices
            upper_stack.height = source_stack.width
        else:  # volume is viewed from the Y or Z projection faces
            upper_stack.width = source_stack.width
            upper_stack.height = source_stack.num_slices
        upper_stack.num_slices = source_stack.height
        upper_stack.tile_size = source_stack.tile_size
        upper_stack.format = source_stack.format
        upper_stack.set_axis(stacks_to_generate[1])
        upper_stack.write_attrs()

        if self._print_info:
            print('Filling level0 of the', stacks_to_generate,
                  'projection stacks...')

        # Fill level0 for the rest of stacks,
        # splitting block processing across MPI processes
        level0 = source_stack.get_level(0)
        block_indices = level0.get_block_list()
        for i in range(0, len(block_indices), self.mpi_size):
            current_block_range = block_indices[i:i+self.mpi_size]
            if self.mpi_rank < len(current_block_range):
                local_block = current_block_range[self.mpi_rank]
                block = level0.get_block(local_block.u,
                                         local_block.v,
                                         local_block.z)
            else:
                # Special handling for the remaining blocks
                block = DataBlock(-1, -1, -1, -1)  # invalid

            self._write_block_to_tiles(block, current_block_range,
                                       left_stack, upper_stack, source_stack)
            # TODO: Progress checkpoint 1
            #pbar.update(1)

            if self._print_info:
                self._print_progress(i, len(block_indices))

        # Wait for level0 to be complete before reading from it
        if self.mpi_comm is not None:
            self.mpi_comm.barrier()

        if self._print_info:
            self._print_progress(len(block_indices)-1, len(block_indices))
            print()
            print('Done.')

        if not generate_lods:
            return

        if self._print_info:
            print('Filling levels 1-n of the', stacks_to_generate,
                  'projection stacks...')

        # Create level[1-n] for the rest of stacks (parallel)
        level_offset = 1
        self.write(left_stack.get_level(0), left_stack, padding_value,
                   interp, 0, level_offset)
        # TODO: Progress checkpoint 2
        #pbar.update(1)

        self.write(upper_stack.get_level(0), upper_stack, padding_value,
                   interp, 0, level_offset)
        # TODO: Progress checkpoint 3
        #pbar.update(1)
        #pbar.close()

    def _write_block_to_tiles(self, block, current_block_range,
                              left_stack, upper_stack, source_stack):
        """Store a block of tiles to the target level0 groups of the rest of
        the projections stacks"""

        # Get all x and y tiles for the block
        x_tiles = block.to_x_tiles(left_stack.format, source_stack.index) if block.is_valid() else []
        y_tiles = block.to_y_tiles(upper_stack.format, source_stack.index) if block.is_valid() else []

        left_stack_l0 = left_stack.get_level(0)
        upper_stack_l0 = upper_stack.get_level(0)

        if self.mpi_comm is not None:
            # create empty tile datasets for the current block range
            # across all MPI processes
            x_tile_sizes = [len(tile) for tile in x_tiles]
            y_tile_sizes = [len(tile) for tile in y_tiles]
            all_x_tile_sizes = self.mpi_comm.allgather(x_tile_sizes)
            all_y_tile_sizes = self.mpi_comm.allgather(y_tile_sizes)

            for i in range(len(all_x_tile_sizes)):
                for x in range(len(all_x_tile_sizes[i])):
                    blk = current_block_range[i]
                    if source_stack.index == 0:
                        u = blk.z
                        v = blk.v
                        z = left_stack_l0.num_slices - 1 - (x + blk.u * blk.nominal_size)
                    elif source_stack.index == 1:
                        u = blk.v
                        v = blk.z
                        z = x + blk.u * blk.nominal_size
                    elif source_stack.index == 2:
                        u = left_stack_l0.num_x_tiles - 1 - blk.z
                        v = blk.v
                        z = x + blk.u * blk.nominal_size
                    left_stack_l0.allocate_tile(all_x_tile_sizes[i][x], u, v, z)

            for i in range(len(all_y_tile_sizes)):
                for y in range(len(all_y_tile_sizes[i])):
                    blk = current_block_range[i]
                    if source_stack.index == 0:
                        u = blk.z
                        v = blk.u
                        z = y + blk.v * blk.nominal_size
                    elif source_stack.index == 1:
                        u = blk.u
                        v = blk.z
                        z = upper_stack_l0.num_slices - 1 - (y + blk.v * blk.nominal_size)
                    elif source_stack.index == 2:
                        u = blk.u
                        v = upper_stack_l0.num_y_tiles - 1 - blk.z
                        z = y + blk.v * blk.nominal_size
                    upper_stack_l0.allocate_tile(all_y_tile_sizes[i][y], u, v, z)

        if not block.is_valid():
            return

        # Write the tiles for the local block
        for i in range(0, len(x_tiles)):
            if source_stack.index == 0:
                u = block.z
                v = block.v
                z = left_stack_l0.num_slices - 1 - (i + block.u * block.nominal_size)
            elif source_stack.index == 1:
                u = block.v
                v = block.z
                z = i + block.u * block.nominal_size
            else:
                # BUG: using this logic, part of first column is missing
                u = left_stack_l0.num_x_tiles - 1 - block.z
                v = block.v
                z = i + block.u * block.nominal_size
            left_stack_l0.store_tile(x_tiles[i], u, v, z)

        for i in range(0, len(y_tiles)):
            if source_stack.index == 0:
                u = block.z
                v = block.u
                z = i + block.v * block.nominal_size
            elif source_stack.index == 1:
                u = block.u
                v = block.z
                z = upper_stack_l0.num_slices - 1 - (i + block.v * block.nominal_size)
            else:
                u = block.u
                # BUG: using this logic, part of first row is missing
                v = upper_stack_l0.num_y_tiles - 1 - block.z
                z = i + block.v * block.nominal_size
            upper_stack_l0.store_tile(y_tiles[i], u, v, z)
