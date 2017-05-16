#
# This script is used to filter a set of NRRD files by brain regions.
# usage: python filter_nrrd_by_brain_regions.py -r <input_brain_regions.nrrd> -g <input_gray_levels.nrrd> -n <input_nissl.nrrd> -o <output_folder_path>
#
#
# Examples:
#
# parent_filter: 382
# children filters: 391, 399, 407, 415
# matched 555,065 voxels, no children match
#
# parent_filter: 1080
# children filters: 375, 382, 391, 399, 407, 415, 423, 431, 438, 446, 454, 463, 471, 479, 486, 495, 504, 726, 10703, 10704, 632, 10702, 734, 742, 751, 758, 766, 775, 782, 790, 799, 807, 815, 823, 982, 19
# voxels: 1,414,313 / 76,899,840
# voxels exported by region Counter({'382': 555065, '463': 354791, '10703': 257103, '632': 101404, '423': 88966, '10704': 51187, '982': 4025, '19': 1772})
#
########################################################################################################################

import nrrd
import collections
import numpy as np
from optparse import OptionParser

progress = 0

'''---------- Define brain regions to be extracted ----------'''

# brain region filters - CCFv3
# brain_region_id = 1080
# filtered_children_ids = set([1080, 375, 382, 391, 399, 407, 415, 423, 431, 438, 446, 454, 463, 471, 479, 486, 495, 504, 726, 10703, 10704, 632, 10702, 734, 742, 751, 758, 766, 775, 782, 790, 799, 807, 815, 823, 982, 19])

# brain region filters - CCFv2
brain_region_id = 1080
#filtered_children_ids = set([19, 982, 726, 799, 823, 815, 807, 766, 790, 782, 775, 734, 758, 751, 742, 10702, 632, 10704, 10703, 375, 463, 504, 495, 486, 479, 471, 423, 454, 446, 438, 431, 382, 415, 407, 399, 391])
filtered_children_ids = set([19])

# brain region filters - CCFv2 & CCFv3
# brain_region_id = 382
# filtered_children_ids = set([391, 399, 407, 415])

parser = OptionParser()

def advance5p():
    global progress
    progress +=5
    print "\rProgress: "+str(progress)


def filter_brain_regions(input_brain_regions, input_gray_levels, input_nissl, output_folder_path):
    global progress

    print str(len(filtered_children_ids)) + " brain id filters: " + str(filtered_children_ids)

    # integer value in the voxels are brain region identifier
    print "\nLoading brain regions ("+input_brain_regions+")..."
    region_voxels, region_metadata = nrrd.read(input_brain_regions)
    print region_metadata
    advance5p()

    print "\nLoading gray levels ("+input_gray_levels+")..."
    gray_voxels, gray_metadata = nrrd.read(input_gray_levels)
    gray_voxels = gray_voxels.astype(np.uint8)
    print gray_metadata
    advance5p()

    # integer value in the avg_voxels are count of cells
    print "\nLoading nissl/cell density (" + input_nissl + ")..."
    nissl_voxels, nissl_metadata = nrrd.read(input_nissl)
    print nissl_metadata
    advance5p()


    # 3D Matrix dimensions
    X_MAX = region_metadata['sizes'][0]
    Y_MAX = region_metadata['sizes'][1]
    Z_MAX = region_metadata['sizes'][2]

    # Create output matrices
    filtered_brain_regions = np.zeros((X_MAX, Y_MAX, Z_MAX))
    out_br_filename = output_folder_path + 'brain_region/' + str(brain_region_id) + '.nrrd'

    filtered_nissl = np.zeros((X_MAX, Y_MAX, Z_MAX))
    out_nissl_filename = output_folder_path + 'nissl/' + str(brain_region_id) + '.nrrd'

    filtered_gray_levels = np.zeros((X_MAX, Y_MAX, Z_MAX))
    out_gray_filename = output_folder_path + 'gray_levels/' + str(brain_region_id) + '.nrrd'

    voxel_count=0
    exported_voxel_count=0
    exported_counter = collections.Counter()
    all_counter = collections.Counter()
    progress_counter = 0
    print "\nBuilding NRRD files filtered for brain region '"+ str(brain_region_id) +"' and children..."
    for x in range(0, X_MAX):
        print "X: " + str(x) + "/" + str(X_MAX) + " - #voxels: " + "{:,}".format(exported_voxel_count) + " / " + "{:,}".format(voxel_count)
        print "Voxels exported by region", exported_counter
        print "Brain regions: ", all_counter
        for y in range(0, Y_MAX):
            for z in range(0, Z_MAX):
                region_id = region_voxels[x, y, z]
                all_counter[ str(region_id) ] += 1
                voxel_count += 1
                if( region_id in filtered_children_ids ):
                    exported_counter[ str(region_id) ] += 1
                    exported_voxel_count += 1

                    filtered_brain_regions[x, y, z] = region_voxels[x, y, z]
                    filtered_nissl[x, y, z] = nissl_voxels[x, y, z]
                    filtered_gray_levels[x, y, z] = gray_voxels[x, y, z]
        fraction = int(float(x) / X_MAX * 70)
        if fraction > progress_counter:
            progress_counter = fraction
            progress += 1
            print "\rProgress: "+str(progress)

    # Write nrrd files
    nrrd.write(out_br_filename, filtered_brain_regions)
    advance5p()
    nrrd.write(out_nissl_filename, filtered_nissl)
    advance5p()
    nrrd.write(out_gray_filename, filtered_gray_levels)
    advance5p()


def parse_options():
    """Parser used for script command with all its necessary parameters needed to be run on the cluster

    :return: parsed options and arguments
    """
    global parser
    parser.add_option("-r", "--regions", dest="input_brain_regions",
                      help="Input file for brain region data",
                      action="store", type='string')

    parser.add_option("-g", "--gray", dest="input_gray_levels",
                      help="Input file for gray level data",
                      action="store", type='string')

    parser.add_option("-n", "--nissl", dest="input_nissl",
                      help="Input file for nissl data",
                      action="store", type='string')

    parser.add_option("-o", "--output", dest="output_folder_path",
                      help="Output folder for extracted data files",
                      action="store", type='string')

    parser.print_help()
    return parser.parse_args()


if __name__ == "__main__":
    options, args = parse_options()

    if not options.input_brain_regions or not options.input_gray_levels or not options.input_nissl or not options.output_folder_path:
            print "\n\n ---> Not enough arguments provided! Check usage above."
            exit()

    filter_brain_regions(options.input_brain_regions, options.input_gray_levels, options.input_nissl, options.output_folder_path)


