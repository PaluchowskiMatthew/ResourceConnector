# -*- coding: utf-8 -*-

###############################################################################
#
# Code related to EPFL Master Semester Project:
# "A job management web service for cluster-based processing in Brain Atlasing"
#
# Version 1.0, 02 June 2017
#
# Copyright (c) 2017, Blue Brain Project
#                     Mateusz Paluchowski <mateusz.paluchowski@epfl.ch>
#                     Christian Tresch <christian.tresch@epfl.ch>
#
###############################################################################

import time

def main():
    print('Let\'s wait 30sec...')
    for i in range(30):
        print(i)
        time.sleep(1)
    print('Time is up!')
    return

if __name__ == "__main__":
    main()
