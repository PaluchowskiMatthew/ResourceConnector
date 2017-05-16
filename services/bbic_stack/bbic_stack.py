#!/usr/bin/env python3
#
# BBIC image stack tool
# Authors: Jafet Villafranca,    2016
#          Raphael Dumusc,       2015
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
    parser = ArgumentParser(description='Create BBIC stack from a given '
                                        'collection of slice images')
    parser.add_argument('stack_filename', help='BBIC stack file')
    parser.add_argument('--create-from', help='Pattern of filenames,'
                                              'e.g. foo_%%03d_bar.png or a '
                                              'text file with a list',
                        dest='source_files')
    parser.add_argument('--to-images', help='Extract the stack as a collection'
                                            ' of images to the given folder',
                        default='', dest='to_image_dir')
    parser.add_argument('--orientation',
                        help='Orientation of the input stack '
                             '(if source_files specified), or the orientation '
                             'of the output image stack (if to_image_dir '
                             'specified). Defaults to sagittal',
                        choices=['coronal', 'axial', 'sagittal',
                                 'coronal-reverse', 'axial-reverse',
                                 'sagittal-reverse'], default='sagittal')
    parser.add_argument('--all-stacks', help='Generate additional stacks '
                                             'along the rest of the axes',
                        action='store_true', dest='all_stacks')
    parser.add_argument('--description', help='Stack description, defaults to '
                                              'Imported image stack',
                        default='Imported image stack')
    parser.add_argument('--tile-size', help='Tile image size, defaults to 256',
                        dest='tile_size', type=int, default=256)
    parser.add_argument('--level', help='Resolution level to extract, '
                                        'defaults to 0', default=0, type=int)
    parser.add_argument('--no-lods', help='Do not generate LODs, only level 0',
                        action='store_true', dest='no_lods')
    parser.add_argument('--format', help='Tile image format, defaults to JPEG',
                        dest='format_', choices=['PNG', 'JPEG', 'TIFF'],
                        default='JPEG')
    parser.add_argument('--mat', help='Matrix to apply to the automatic '
                                      'voxel-based local_to_world matrix',
                        choices=['X', 'Y', 'Z'], default='Z')
    parser.add_argument('--slice-positions', help='Specifies text file name '
                                                  'containing slice positions',
                        dest='slice_positions')
    parser.add_argument('--interp', help='Interpolation type for downsampling,'
                                         ' defaults to linear',
                        choices=['nearest', 'linear'], default='linear')
    parser.add_argument('--from', help='Start from given slice (for resuming),'
                                       ' defaults to 0',
                        dest='from_', type=int, default=0)
    parser.add_argument('--padding-value', help='Padding value for extending '
                                                'tiles',
                        default=255, dest='padding_value')
    return parser


def main():
    start_time = time.time()

    parser = create_parser()
    args = parser.parse_args()

    # Create writer
    if MPI_ENABLED:
        comm = MPI.COMM_WORLD
        if comm.Get_rank() == 0:
            print("MPI group size: " + str(comm.Get_size()))
    else:
        print("MPI disabled")
        comm = None

    reverse = False
    orientation = args.orientation
    if 'reverse' in orientation:
        reverse = True
        orientation = orientation[:orientation.find('-reverse')]

    stack_index = 2  # sagittal by default
    if orientation == 'coronal':
        stack_index = 0
    elif orientation == 'axial':
        stack_index = 1

    if args.source_files is None:
        reader = bbic.File(args.stack_filename, 'r', comm)
        print(reader)
        stack = reader.get_stack(stack_index)
        if stack is None:
            print('Error: the requested stack does not exists in this file!')
            return
        print(stack)

        if args.to_image_dir:
            level = stack.get_level(args.level)
            if level is None:
                print('Error: the requested level does not exists in this stack!')
                return

            print("Exporting to images: ", level)
            level.extract_slices(args.to_image_dir, args.format_)
        else:
            stack.print_structure()

    else:
        # Open source image stack
        filename_pattern = args.source_files
        extension = os.path.splitext(filename_pattern)[1]
        source_is_h5 = extension == ".h5"
        if source_is_h5:
            reader = bbic.File(filename_pattern, 'r', comm)
            image_source = reader.get_stack(0).get_level(0)
        else:
            reader = bbic.ImageStack(filename_pattern)
            reader.determine_stack_size(comm)
            image_source = reader

        # Write the target stack
        writer = bbic.File(args.stack_filename, 'a', comm)
        stack = writer.create_stack(stack_index)
        stack.width, stack.height, stack.num_slices = image_source.get_dimensions()
        stack.tile_size = args.tile_size
        stack.format = args.format_
        stack.description = args.description
        stack.original_filenames = os.path.abspath(filename_pattern)
        stack.set_axis(args.mat)
        stack.orientation = args.orientation
        stack.set_slice_positions(args.slice_positions)
        stack.write_attrs()
        generate_lods = not args.no_lods
        assert isinstance(generate_lods, bool)

        writer.write(image_source, stack, args.padding_value, args.interp,
                     args.from_, 0, generate_lods, reverse)

        # Optional: write additional x and y stacks
        if args.all_stacks:
            if source_is_h5:
                source_stack = reader.get_stack(stack_index)
            else:
                writer.close_and_reopen('a')
                source_stack = writer.get_stack(stack_index)
            writer.make_all_stacks(source_stack, args.padding_value,
                                   args.interp, generate_lods)

    if not MPI_ENABLED or MPI.COMM_WORLD.Get_rank() == 0:
        print("--- Execution time: %s seconds ---" %
              round(time.time() - start_time))

if __name__ == '__main__':
    main()
