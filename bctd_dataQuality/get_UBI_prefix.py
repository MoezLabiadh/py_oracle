#-------------------------------------------------------------------------------
# Name:     UBI lookup

# Purpose: This module contains UBI prefexices
#          of each BCTS Business Area (BA)
#
#
# Author:  Moez Labiadh, BCTS-TKO
#
# Created:       23-02-2021
#-------------------------------------------------------------------------------

def get_UBI_prefix (BA):
    """Returns the UBI prefix based on BA value entred by the user"""
    prfx_lookup =  {
                    'TBA': 'BA',
                    'TCH': 'BB',
                    'TSG': 'BC',
                    'TPL': 'BD',
                    'TKA': 'BE',
                    'TKO': 'BF',
                    'TPG': 'BG',
                    'TST': 'BH',
                    'TSK': 'BI',
                    'TSN': 'BJ',
                    'TOC': 'BK',
                    'TCC': 'BL',
                    'ALL': 'B'
                  }

    return prfx_lookup.get(BA)
