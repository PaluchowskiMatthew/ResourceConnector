#!/usr/bin/env python3
#
# BBIC volume tool
# Authors: Raphael Dumusc,       2015
#          Stanislaw Adaszewski, 2014-2015
#
# Copyright (c) BBP/EPFL 2014-2015; All rights reserved.
# Do not distribute without further notice.

import os
import time
from argparse import ArgumentParser
try:
    from mpi4py import MPI
    MPI_ENABLED = MPI.COMM_WORLD.Get_size() > 1
except ImportError:
    MPI_ENABLED = False
import bbic


def create_parser():
    parser = ArgumentParser(description='BBIC volume tool')
    parser.add_argument('volume_filename', help='BBIC volume file')
    parser.add_argument('--volume', help='Volume index, defaults to 0', default=0, type=int)
    parser.add_argument('--level', help='Resolution level to extract, defaults to 0', default=0, type=int)
    parser.add_argument('--to-images', help='Extract the volume as a stack of images to the given folder', default='', dest='to_image_dir')
    parser.add_argument('--create-from', help='Input BBIC image stack to create the volume', default='', dest='source_files')
    parser.add_argument('--format', help='Output format for generated images, defaults to png', default='png')
    parser.add_argument('--axis', help='Axis along which to take slices, defaults to 0', choices=[0, 1, 2], default=0, type=int)
    parser.add_argument('--block-size', help='Block size, defaults to 64', dest='block_size', type=int, default=64)
    return parser


def main():
    start_time = time.time()

    parser = create_parser()
    args = parser.parse_args()

    if MPI_ENABLED:
        comm = MPI.COMM_WORLD
        if comm.Get_rank() == 0:
            print("MPI group size: " + str(comm.Get_size()))
    else:
        print("MPI disabled")
        comm = None

    if args.source_files:
        # Open source image stack
        filename_pattern = args.source_files
        extension = os.path.splitext(filename_pattern)[1]
        source_is_h5 = extension == ".h5"
        if source_is_h5:
            reader = bbic.File(filename_pattern, 'r', comm)
            block_source = reader.get_stack(0).get_level(0)
        else:
            reader = bbic.ImageStack(filename_pattern)
            reader.determine_stack_size(comm)
            block_source = bbic.SliceToBlocks(reader, args.block_size)

        volume_file = bbic.File(args.volume_filename, 'a', comm)
        volume = volume_file.create_volume(args.volume)
        volume.fill(block_source, args.block_size)

    else:
        reader = bbic.File(args.volume_filename, 'r', comm)
        print(reader)
        volume = reader.get_volume(args.volume)
        if volume is None:
            print('Error: the requested volume does not exists in this file!')
            return

        print(volume)
        print("detailed structure:")
        volume.print_structure()

        if args.to_image_dir:
            level = volume.get_lod(args.level)
            if level is None:
                print('Error: the requested level does not exists in this volume!')
                return

            print("Exporting to images: ", level)
            level.extract_slices(args.to_image_dir, args.format, args.axis)

    if not MPI_ENABLED or MPI.COMM_WORLD.Get_rank() == 0:
        print("--- Execution time: %s seconds ---" % round(time.time() - start_time))

if __name__ == '__main__':
    main()
