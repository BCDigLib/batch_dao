# This script generates the Handle batch file that can be used to mint handles. 

# USAGE: 
#    aspace_batch_dao.py tab_file.txt 
# where tab_file.txt is the output of aspace_ead_to_tab.xsl.

import sys
import os
from datetime import datetime
from dotenv import load_dotenv

curr_date = datetime.now().strftime("%Y%m%d-%H%M%S")

# create local LOGS output directory if it doesn't exist
if not os.path.exists('HANDLES'):
    os.makedirs('HANDLES')

# make output log file
output_file = 'HANDLES/handle_batch_text-%s.txt' % curr_date
file_out = open(output_file, 'w+')

# flag to determine if write_out() also prints to STDOUT
IGNORE_STDOUT = False

# output messages to log and/or STDOUT
def write_out(str, write_to_stdout=False):
    file_out.write(str + "\n")
    if write_to_stdout:
        print(str)


def main():
    start_now = datetime.now()
    current_time = start_now.strftime("%Y-%m-%d, %H:%M:%S")
    print("script start time: %s\n" % current_time)

    #
    # Check arguments
    #

    # check to see if we have the correct number of arguments
    if len(sys.argv) != 2:
        print("Please use the correct number of arguments.\nUsage: handle_batch_text.py tab_file.tsv")
        sys.exit()

    # next, check if first argument exists as a file
    tab_file = sys.argv[1]
    if not os.path.isfile(tab_file):
        print("The file '%s' does not exist. Exiting." % tab_file)
        sys.exit()

    #
    # Collect Handle config data
    #

    # read in secrets from .env file
    load_dotenv()

    HANDLE_PREFIX   = os.getenv('HANDLE_PREFIX')
    HANDLE_PASSWORD = os.getenv('HANDLE_PASSWORD')

    if HANDLE_PREFIX is None or HANDLE_PASSWORD is None:
        print("Error loading .env file! Exiting.")
        sys.exit()

    #
    # Open the TSV file created with aspace_ead_to_tab.xsl to gather variables to generate handle batch text
    #

    # read in tab_file: sys.argv[1]
    try:
        tab_in = open(tab_file, 'r')
    except OSError as e:
        print("❌ Could not open/read file: %s"  % tab_file)
        raise SystemExit(e)

    # load file
    try:
        ead_data = tab_in.read()
    except ValueError as e:
        print("❌ Could not load file: %s"  % tab_file)
        raise SystemExit(e)

    print("✓ Read in tab_file: %s\n" % tab_file)
    print("✓ Writing to file: %s" % output_file)
    
    # split file into lines
    ead_lines = ead_data.splitlines()

    print("\nnow creating %s Handle batches" % len(ead_lines) )

    #
    # Loop through EAD file
    #

    # EAD row format
    # aspace_id, ref_id, use_note, collection_dates, lang_code, genre
    # 0          1       2         3                 4          5

    for index, line in enumerate(ead_lines):
        
        # data from each line in the ead_lines file
        metadata = line.split("\t")  # creates a metadata array with individual tokens starting at metadata[0]
        component_id = metadata[0]   # BC1986_020E_3940
        iiif_URI = 'https://library.bc.edu/iiif/view/%s' % component_id

        print("[%s] %s" % (index + 1, component_id ))

        write_out("CREATE %s/%s" % (HANDLE_PREFIX, component_id))
        write_out("100 HS_ADMIN 86400 1110 ADMIN 300:111111111111:%s/%s" % (HANDLE_PREFIX, component_id))
        write_out("300 HS_SECKEY 86400 1100 UTF8 %s" % HANDLE_PASSWORD)
        write_out("201 URL 86400 1110 UTF8 %s\n" % iiif_URI)

    end_now = datetime.now()
    current_time = end_now.strftime("%Y-%m-%d, %H:%M:%S")

    print("\nscript stop time: %s\n" % current_time)
    print("elasped time: %s\n" % (end_now - start_now) )


main()
