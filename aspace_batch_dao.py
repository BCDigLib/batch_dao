# modified version of the script written by the Bentley Historical Library to automatically create Digital Objects by
# re-using archival object metadata in ArchivesSpace. Original script available at
# https://github.com/djpillen/bentley_scripts/blob/master/update_archival_object.py
#
# This script recreates the first steps in using a csv to locate archival objects and retrieve their metadata,
# but adds additional project-dependant metadata before creating the Digital Object, and creates Digital Object
# Components to hang file information off of rather than attaching files directly to the Digital Object.
# Also imports technical metadata from an exif file and stores it on the pertinent Digital Object Components.
# METS exports for every created Digital Object are also saved off in a folder labeled "METS".

# usage:
#    aspace_batch_dao.py [-h][--dryrun] {LOCAL|DEV|STAGE|PROD} tab_file.txt fits_file.json
#
# positional arguments:
#  {LOCAL,DEV,STAGE,PROD}  targeted ArchivesSpace environment
#  tab_file.tsv      output of aspace_ead_to_tab.xsl
#  fits_file.json    output of running fit-to-json.xsl over a FITS xml file

# optional arguments:
#  -h, --help        show this help message and exit
#  --dryrun          dry run; don't create any records
import re

import requests
import json
import sys
import os
import argparse
import progressbar
from datetime import datetime
from dotenv import load_dotenv
from typing import Union

# Parse command line arguments. Handles input validation and opening files.
parser = argparse.ArgumentParser()
parser.add_argument("target_environment", choices=["LOCAL", "DEV", "STAGE", "PROD"], help="targeted ArchivesSpace environment")
parser.add_argument("tab_file", help="tab file generated from EAD", metavar="tab_file.tsv",
                    type=argparse.FileType('r'))
parser.add_argument("fits_techmd_file", help="FITS file in JSON format", metavar="fits_file.json",
                    type=argparse.FileType('r'))
parser.add_argument('--dryrun', action='store_true', help="dry run; don't create or update any records")                    
args = parser.parse_args()

# read in secrets from .env file
load_dotenv()

# Load target ASpace environment from arguments. Set values for each target in the users
# env as ASPACE_DEV_URL, ASPACE_STAGE_USERNAME, ASPACE_PROD_PASSWORD, etc.
ASPACE_URL = os.getenv('ASPACE_' + args.target_environment + '_URL')
ASPACE_USERNAME = os.getenv('ASPACE_' + args.target_environment + '_USERNAME')
ASPACE_PASSWORD = os.getenv('ASPACE_' + args.target_environment + '_PASSWORD')

# URL parameter to expand the archival_objects/digital_object references when loading an AO record by refID.
# this reduces initial API calls from two to one, and greatly speeds up processing
ASPACE_RESOLVE_ARCHIVAL_OBJECT_PARAM = "?resolve[]=archival_objects&resolve[]=_resolved::instances::digital_object"

# set the handle URL prefix
ASPACE_HANDLE_URL_PREFIX = "http://hdl.handle.net/%s/" % os.getenv('HANDLE_PREFIX')

curr_date = datetime.now().strftime("%Y%m%d-%H%M%S")

# create local LOGS output directory if it doesn't exist
if not os.path.exists('LOGS'):
    os.makedirs('LOGS')

# make output log file
file_out = open('LOGS/log_aspace_batch_dao-%s.txt' % curr_date, 'w+')

# make file to save IDs in for IIIF manifest generator
ids_for_manifest = open('LOGS/ids_for_manifest-%s.txt' % curr_date, 'w+')

# flag to determine if write_out() also prints to STDOUT
IGNORE_STDOUT = False

# output messages to log and/or STDOUT
def write_out(str, write_to_stdout=True):
    file_out.write(str + "\n")
    if write_to_stdout:
        print(str)


def main():
    start_now = datetime.now()
    current_time = start_now.strftime("%Y-%m-%d, %H:%M:%S")
    write_out("script start time: %s\n" % current_time)

    #
    # Check arguments
    #

    if args.dryrun:
        write_out("\n*** This is a dryrun, and no ASpace records will be created or updated ***\n")

    # next, make sure we have proper target environment variables set
    missing_vars = []

    if not ASPACE_URL:
        missing_vars.append("ASPACE_" + args.target_environment + "_URL")
    if not ASPACE_USERNAME:
        missing_vars.append("ASPACE_" + args.target_environment + "_USERNAME")
    if not ASPACE_PASSWORD:
        missing_vars.append("ASPACE_" + args.target_environment + "_PASSWORD")

    if len(missing_vars) != 0:
        missing_vars_string = ', '.join(missing_vars)
        write_out("Missing required environment variables: %s. Exiting" % missing_vars_string)
        sys.exit(1)

    # If we are in production, prompt the user to confirm
    if args.target_environment == 'PROD':
        if not prompt_yes_no("You are running this script in production. Proceed?"):
            write_out("Exiting.")
            sys.exit()

    #
    # Collect ASpace session token
    #

    # log into ASpace and gather session info
    try:
        auth = requests.post(ASPACE_URL + '/users/' + ASPACE_USERNAME + '/login?password=' + ASPACE_PASSWORD)
        auth.raise_for_status()
    except requests.exceptions.Timeout as e:
        write_out("Timeout error. Is the server running, or do you need to connect through a VPN?")
        raise SystemExit()
    except requests.exceptions.HTTPError as e:
        write_out("Caught HTTP error.")
        raise SystemExit(e)
    except requests.exceptions.RequestException as e:
        write_out("Error loading ASpace session. Maybe check your login info, or connect through a VPN.")
        raise SystemExit()

    # convert auth request obj into a json formatted object
    try:
        auth_json = auth.json()
    except ValueError as e:
        write_out("Could not load auth response as a json file.")
        raise SystemExit(e)

    session = auth_json['session']
    headers = {'X-ArchivesSpace-Session': session}

    write_out("✓ Got ASpace session token: %s" % session)

    # the following group are based on assumptions and may need to be changed project-to-project.
    format_note = "reformatted digital"

    #
    # Load FITS data from JSON file. This dictionary is used for techMD calls, also create another dict
    # for file lists.
    #
    try:
        tech_data = json.load(args.fits_techmd_file)
    except ValueError as e:
        write_out("❌ Could not load json file: %s" % args.fits_techmd_file.name)
        raise SystemExit(e)

    write_out("✓ Read in fits_techmd_file: %s" % args.fits_techmd_file.name)

    # generate a dictionary of file names for each group of images.
    # this is dependent on a standard naming schema
    # {
    #    'BC2001_074_64862': [
    #      'BC2001_074_64862_0000.tif', 
    #      'BC2001_074_64862_0001.tif'
    #     ],
    #     'BC2001_074_64863': [
    #       'BC2001_074_64863_0000.tif', 
    #       'BC2001_074_64863_0001.tif'
    #      ]
    # }

    files_listing = dict()
    for key, values in tech_data.items():

        # Most file names are in the format: "BC2001_074_64862_0000.tif", but some are in the format
        # "bc-2001-074_64862_0000.tif"

        # Convert to all uppercase        
        normalized_key = key.upper()                               # "BC-2000-178_15087_0001.tif"

        # Normalize CUI strings that exist in a few different formats:
        #   Original                      Normalized
        #   BC-2000-178_15087_0001.tif => BC2000-178_15087_0001.tif
        #   BC_2000-178_15087_0001.tif => BC2000-178_15087_0001.tif
        #   MS-2000-178_15087_0001.tif => MS2000-178_15087_0001.tif
        #   MS-2000-178_15087_0001.tif => MS2000-178_15087_0001.tif
        #   FOO-200-178_15087_0001.tif => FOO200-178_15087_0001.tif
        
        normalized_key = re.sub('^([a-zA-Z]+)[-_](.*)', r'\1\2', normalized_key)

        # Replace dashes with underscores
        normalized_key = normalized_key.replace('-', '_')          # "BC2000_178_15087_0001.tif"

        # Extract the last portion of the CUI string
        cutoff = normalized_key.replace('_', "|", 2).find('_')     # "BC2001|074|64862_0000.tif"
        #                                                                             ^
        short_name = normalized_key[0:cutoff]                      # "BC2001_074_64862"

        # add short_name with and without underscores to dictionary
        if short_name not in files_listing:
            files_listing[short_name] = [key]
        else:
            files_listing[short_name].append(key)

        #
        # Sometimes the FITS image filenames don't exactly match the same string format as the CUI,
        # so we'll include two new key-pair that contains some common varieties: 
        #

        # 1) add an underscore to the prefix
        # BC2001_074_64862  => BC_2001_074_64862
        short_name_with_underscore = re.sub('^([a-zA-Z]+)([0-9])', r'\1_\2', short_name)

        # add short_name with underscore to dictionary
        if short_name_with_underscore not in files_listing:
            files_listing[short_name_with_underscore] = [key]
        else:
            files_listing[short_name_with_underscore].append(key)

        # 2) add a dash to the prefix
        # BC2001_074_64862  => BC-2001_074_64862
        short_name_with_dash = re.sub('^([a-zA-Z]+)([0-9])', r'\1_\2', short_name)

        # add short_name with dash to dictionary
        if short_name_with_dash not in files_listing:
            files_listing[short_name_with_dash] = [key]
        else:
            files_listing[short_name_with_dash].append(key)

    #
    # Read the TSV file created with aspace_ead_to_tab.xsl to gather variables and make the API calls
    #

    try:
        ead_data = args.tab_file.read()
    except ValueError as e:
        write_out("❌ Could not load json file: %s" % args.tab_file.name)
        raise SystemExit(e)

    write_out("✓ Read in tab_file: %s" % args.tab_file.name)

    # split file into lines
    ead_lines = ead_data.splitlines()

    write_out("\nnow creating %s DAO records" % len(ead_lines) )

    #
    # Loop through EAD file
    #

    # EAD row format
    # aspace_id, ref_id, use_note, collection_dates, lang_code, genre
    # 0          1       2         3                 4          5

    for index, line in enumerate(ead_lines):
        try:
            process_digital_archival_object(files_listing, format_note, headers, index, line, tech_data)
        except InvalidEADRecordError as e:
            write_out(str(e))

    end_now = datetime.now()
    current_time = end_now.strftime("%Y-%m-%d, %H:%M:%S")

    write_out("\nscript stop time: %s\n" % current_time)
    write_out("elasped time: %s\n" % (end_now - start_now) )


def process_digital_archival_object(files_listing, format_note, headers, index, line, tech_data):
    """Reads a single DAO and adds it to ArchiveSpace
    """

    # data from each line in the ead_lines file
    metadata = line.split("\t")                 # creates a metadata array with individual tokens starting at metadata[1]
    aspace_id = metadata[1]                     # aspace_03cca77bf5ecf4d4bfdf70bcaf738383
    dimensions_note = "1 " + metadata[2]        # 1 file
    use_note = metadata[3]                      # "These materials are made available for ..."
    collection_dates = metadata[4].split("/")   # [1875, 1879]
    lang_code = metadata[5]                     # eng
    genre = metadata[6]                         # Scrapbooks
    id_ref = aspace_id[7:len(aspace_id)]        # 03cca77bf5ecf4d4bfdf70bcaf738383

    write_out("\n\n###########")
    write_out("[%s] %s - %s" % (index + 1, aspace_id, metadata[4]))
    write_out("###########")

    # look up AO record by ref_id pulled from EAD
    params = {'ref_id[]': id_ref}

    # define AO record URL

    # URL with parameters: 
    # /repositories/2/find_by_id/archival_objects"?resolve[]=archival_objects&resolve[]=_resolved::instances::digital_object
    ao_record_url = ASPACE_URL + '/repositories/2/find_by_id/archival_objects' + ASPACE_RESOLVE_ARCHIVAL_OBJECT_PARAM
    write_out("⋅ fetching AO with URL: %s" % ao_record_url)
    write_out("⋅ fetching AO by ref_id: %s" % id_ref)

    # fetch AO record by refID
    try:
        archival_objects_json_raw = requests.get(ao_record_url, headers=headers, params=params)
        archival_objects_json_raw.raise_for_status()
        write_out("  ✓ found AO object")
    except requests.exceptions.Timeout as e:
        raise InvalidEADRecordError("  ❌ Timeout error. Is the server running, or do you need to connect through a VPN?"
                                    " Continuing to next AO record.")
    except requests.exceptions.HTTPError as e:
        raise InvalidEADRecordError("  ❌ Caught HTTP error. Continuing to next AO record.")
    except requests.exceptions.RequestException as e:
        raise InvalidEADRecordError("  ❌ Error loading ASpace record. Continuing to next AO record.")
    
    # convert response to json object
    try:
        archival_objects_json_full = archival_objects_json_raw.json()
    except ValueError:
        raise InvalidEADRecordError("  ❌ Could not load request response as a json file."
                                    "Continuing to next AO record.")

    # check that we have a single AO instance from this request
    if len(archival_objects_json_full['archival_objects']) == 0:
        raise InvalidEADRecordError("  ❌ Could not find an archival_object with ref_id: %s."  % id_ref)

    if len(archival_objects_json_full['archival_objects']) > 1:
        raise InvalidEADRecordError("  ❌ Multiple archival_objects with ref_id: %s."
                                    "Make sure ref_ids are unique." % id_ref)

    try:
        archival_object_uri = archival_objects_json_full['archival_objects'][0]['ref']
        write_out("  ✓ found AO URI: %s" % archival_object_uri)
    except ValueError:
        raise InvalidEADRecordError("  ❌ Could not find [archival_objects][0][ref] value."
                                    "Continuing to next AO record.")

    # simplify our work by pulling out the [archival_objects][0][_resolved] portion of the json object
    try:
        archival_object_json = archival_objects_json_full['archival_objects'][0]['_resolved']
    except ValueError:
        raise InvalidEADRecordError("  ❌ Could not find [archival_objects][0][_resolved] value."
                                    "Continuing to next AO record.")
   
    # check for necessary metadata & only proceed if it's all present.
    write_out("\n  ##### JSON OUTPUT BEGIN - FETCH AO #####", IGNORE_STDOUT)
    write_out(json.dumps(archival_object_json, indent=4, sort_keys=True), IGNORE_STDOUT)
    write_out("  ##### JSON OUTPUT END - FETCH AO #####\n", IGNORE_STDOUT)
    
    # look for component unique ID
    write_out("⋅ looking for component unique ID")
    try:
        unique_id = archival_object_json['component_id']
        write_out("  ✓ component unique ID: %s" % unique_id)
    except KeyError:
        raise InvalidEADRecordError("  ❌ Please make sure all items have a component unique ID before creating DAOs."
                                    "Continuing to next AO record.")
   
    # derive handle URI
    handle_URI = '%s%s' % (ASPACE_HANDLE_URL_PREFIX, unique_id)
    write_out("⋅ deriving handle URI for digital object:")
    write_out("  ✓ %s" % handle_URI)

    # look for existing digital_object_id IDs, if they exist
    # json object structure: "instances": { [...], ["digital_object": {"_resolved": {"digital_object_id"}}] }
    write_out("⋅ searching for existing digital object IDs:")
    digital_object_ids = []
    if "instances" in archival_object_json:
        for instance_do in archival_object_json["instances"]:
            try:
                digital_object_id = instance_do["digital_object"]["_resolved"]["digital_object_id"]
                write_out("  ! digital object ID found in this AO: %s" % digital_object_id)
                
                # check if handle_URI matches digital_object_id
                if digital_object_id == handle_URI:
                    write_out("  ! digital object ID matches our derived handle URI. Continuing to next AO record.")
                    digital_object_ids.append(digital_object_id)
                    return True
                else:
                    write_out("  ✓ digital object ID doesn't match our derived handle URI. This is unusual, but we'll accept it...")
            except KeyError:
                pass

        if not digital_object_ids:
            write_out("  ✓ no digital object ID found in this AO")
    
    write_out("⋅ looking for various metadata values:")

    # look for title
    try:
        obj_title = archival_object_json['title']
    except KeyError:
        write_out("    [did not find 'title', now looking for date expression...]")
        try:
            obj_title = archival_object_json['dates'][0]['expression']
        except KeyError:
            write_out("    [did not find 'date expression', now looking for date begin and end values...]")
            try:
                date_begin = archival_object_json['dates'][0]['begin']
                date_end = archival_object_json['dates'][0]['end']
                obj_title = "%s - %s" % (date_begin, date_end)
            except KeyError:
                raise InvalidEADRecordError("  ❌ Item %s has no title or date expression or date begin/end."
                                            " Please check the metadata & try again."
                                            " Continuing to next AO record." % unique_id)
    
    write_out("  ✓ object title: '%s'" % obj_title)
    
    # look for linked agents
    try:
        agent_data = archival_object_json['linked_agents']
        write_out("  ✓ linked agents found")
    except KeyError:
        write_out("  ✓ did not find any linked agents")
        agent_data = []
    
    # check for expression type 'single' before looking for both start and end dates
    date_json = create_date_json(archival_object_json, unique_id, collection_dates)

    # check if we have a valid date_jason object
    if date_json is None:
        write_out("  ❌ could not generate date json object. Continuing to next AO record.")
        return

    write_out("  ✓ generated date json object:")
    write_out("    %s" % date_json)
    
    # write_out(json.dumps(date_json, indent=4, sort_keys=True), IGNORE_STDOUT)
    write_out("⋅ gathering list of filenames from FITS dictionary by unique ID: %s" % unique_id)
    
    # pull in files list from FITS dictionary to get data to create digital object components and thumbnail
    try:
        file_names = files_listing[unique_id]
        file_names.sort()
    except KeyError as e:
        raise InvalidEADRecordError("  ❌ Can't find unique_id %s in dictionary of file names."
                                    " Check the FITS file and try again. Continuing to next AO record." % unique_id)
    write_out("    [number of file names pulled from FITS dictionary matching unique ID: %s]" % len(file_names))
    write_out("  ✓ first file name pulled from FITS dictionary matching unique ID: %s" % file_names[0])

    # derive resource type
    write_out("⋅ deriving resource type:")
    try:
        resource_type = get_resource_type(archival_object_json['instances'])
    except KeyError:
        write_out("  ❌ %s can't be assigned a typeOfResource based on the physical instance. "
                           "Please check the metadata & try again. Continuing to next AO record." % id_ref)
        return
    except IndexError:
        write_out("  ❌ %s  can't be assigned a typeOfResource based on the physical instance (%s). "
                           "Please check the metadata & try again. Continuing to next AO record." % (id_ref, archival_object_json['instances']) )
        return
    
    write_out("  ✓ %s" % resource_type)
    
    # derive file type
    file_type = get_file_type(files_listing[unique_id][0])
    write_out("⋅ deriving file type:")

    if file_type is None:
        write_out("  ❌ could not derive file type. Continuing to next AO record.")
        return

    write_out("  ✓ %s" % file_type)
    
    # derive thumbnail URI
    file_name_whole = file_names[0]  # BC2001_074_64862_0000.tif
    file_name_split = os.path.splitext(file_name_whole)  # ('BC2001_074_64862_0000', 'tif')
    if not file_name_split[0]:
        raise InvalidEADRecordError("  ❌ Couldn't derive thumbnail URI from file_names list."
                                    " Continuing to next AO record.")
    thumbnail_uri = 'https://iiif.bc.edu/%s.jp2/full/!200,200/0/default.jpg' % file_name_split[0]
    
    write_out("⋅ deriving thumbnail URI:")
    write_out("  ✓ %s" % thumbnail_uri)

    # derive genre type
    genre_type = get_genre_type(genre)
    write_out("⋅ deriving genre type:")

    if genre_type is None:
        write_out("  ❌ could not derive genre type. Continuing to next AO record.")
        return

    write_out("  ✓ %s" % genre_type)

    # generate JSON object for creating DAO
    dig_obj_component = {
        'jsonmodel_type': 'digital_object',
        'title': obj_title,
        'digital_object_type': resource_type,
        'lang_materials': [
            {
                'jsonmodel_type': 'lang_material',
                'language_and_script': {
                    'language': lang_code,
                    'jsonmodel_type': 'language_and_script'
                }
            }
        ],
        'file_versions': [
            {
                'file_uri': handle_URI,
                'publish': True,
                'is_representative': True,
                'jsonmodel_type': 'file_version',
                'xlink_actuate_attribute': 'onRequest',
                'xlink_show_attribute': 'new'
            },
            {
                'file_uri': thumbnail_uri,
                'publish': True,
                'xlink_actuate_attribute': 'onLoad',
                'xlink_show_attribute': 'embed',
                'is_representative': False,
                'jsonmodel_type': 'file_version'
            }
        ],
        'digital_object_id': handle_URI,
        'publish': True,
        'notes': [
            {
                'content': [use_note],
                'type': 'userestrict',
                'jsonmodel_type': 'note_digital_object'
            },
            {
                'content': [dimensions_note],
                'type': 'dimensions',
                'jsonmodel_type': 'note_digital_object'
            },
            {
                'content': [format_note],
                'type': 'note',
                'jsonmodel_type': 'note_digital_object'
            },
            {
                'content': [file_type],
                'type': 'note',
                'jsonmodel_type': 'note_digital_object'
            }
        ],
        'dates': [date_json],
        'linked_agents': agent_data,
        'subjects': [{"ref": genre_type}]
    }
    
    # format the JSON
    dig_obj_data = json.dumps(dig_obj_component)
    write_out("\n  ##### JSON OUTPUT BEGIN - CREATE DAO #####", IGNORE_STDOUT)
    write_out(json.dumps(dig_obj_component, indent=4, sort_keys=True), IGNORE_STDOUT)
    write_out("  ##### JSON OUTPUT END - CREATE DAO #####\n", IGNORE_STDOUT)

    #
    # create DAO records
    #

    write_out("⋅ creating DAO")

    # skip DAO creation if the --dryrun flag was set
    if args.dryrun:
        write_out("  ! Dry run: skipping DAO creation")
        return True

    # next, post the DAO
    try:
        dig_obj_post_raw = requests.post(ASPACE_URL + '/repositories/2/digital_objects', headers=headers,
                                         data=dig_obj_data)
        dig_obj_post_raw.raise_for_status()
    except requests.exceptions.Timeout as e:
        raise InvalidEADRecordError("  ❌ Timeout error. Is the server running, or do you need to connect through"
                                    " a VPN? Continuing to next AO record.") from e

    except requests.exceptions.HTTPError as e:
        raise InvalidEADRecordError("  ❌ Caught HTTP error. Continuing to next AO record.")

    except requests.exceptions.RequestException as e:
        raise InvalidEADRecordError("  ❌ Error posting DAO record. Continuing to next record.")
   
    # convert response to json object
    try:
        dig_obj_post = dig_obj_post_raw.json()
    except ValueError:
        raise InvalidEADRecordError("  ❌ Could not load request response as a json file."
                                    "Continuing to next AO record.")
    
    # grab the newly created DAO URI and only proceed if the DAO hasn't already been created
    try:
        dig_obj_uri = dig_obj_post['uri']
        write_out("  ✓ DAO created with URI: %s" % dig_obj_uri)
    except KeyError:
        # TODO: rather than continue to next record, go to next step to update AO?
        raise InvalidEADRecordError("  ❌ DAO for item %s already exists. Continuing to next AO record." % unique_id)
    
    # save the ID of the newly created DAO to feed the IIIF manifest generator
    id_start = dig_obj_uri.rfind('/')  # /repositories/2/archival_objects/1234567
    #                                 ^
    
    dig_obj_id = dig_obj_uri[(id_start + 1):len(dig_obj_uri)]  # 1234567
    ids_for_manifest.write(dig_obj_id + '\n')
    
    # next, build a new instance to add to the parent AO, linking to the newly created DAO record
    dig_obj_instance = {
        'instance_type': 'digital_object',
        'digital_object': {
            'ref': dig_obj_uri
        }
    }
    write_out("⋅ updating AO record to include this newly created DAO as an instance")
    
    # modify the AO record's to include this DAO as an instance
    archival_object_json['instances'].append(dig_obj_instance)
    archival_object_data = json.dumps(archival_object_json)
    
    #
    # post the AO with an updated DAO instance
    #
    
    try:
        archival_object_update_raw = requests.post(ASPACE_URL + archival_object_uri, headers=headers,
                                                   data=archival_object_data)
        archival_object_update_raw.raise_for_status()
    except requests.exceptions.Timeout as e:
        raise InvalidEADRecordError("  ❌ Timeout error. Is the server running, or do you need to connect through"
                                    " a VPN? Continuing to next AO record.") from e

    except requests.exceptions.HTTPError as e:
        raise InvalidEADRecordError("  ❌ Caught HTTP error. Continuing to next AO record.")

    except requests.exceptions.RequestException as e:
        raise InvalidEADRecordError("  ❌ Error updating AO record. Continuing to next AO record.")
   
    # convert response to json object
    try:
        archival_object_update = archival_object_update_raw.json()
    except ValueError:
        raise InvalidEADRecordError("  ❌ Could not load request response as a json file."
                                    " Continuing to next AO record.")
   
    # TODO: verify post by checking URI from request update to known AO URI
    write_out("  ✓ AO record updated")

    #
    # create DAO component records
    #
    
    # generate DAO components from list of file names for this AO
    write_out("⋅ generating DAO components")
    write_out("    [preparing to create %s DAO component records]" % len(file_names))
    
    completed_dao_component_records = 0
    bad_dao_records = []

    # se will use a progressbar to display our progress
    with progressbar.ProgressBar(max_value=len(file_names)) as bar:
        for index, file_name in enumerate(file_names):
            write_out("  [%s] %s" % (index, file_name), IGNORE_STDOUT)

            # derive base file name
            period_loc = file_name.index('.')
            base_name = file_name[0:period_loc]
            
            # create DAO component json object
            dig_obj_component = {
                'jsonmodel_type': 'digital_object_component',
                'publish': False,
                'label': base_name,
                'file_versions': build_comp_file_version(file_name, tech_data),
                'title': base_name,
                'display_string': file_name,
                'digital_object': {
                    'ref': dig_obj_uri
                }
            }

            try:
                dig_obj_component_uri = post_digital_object_component(dig_obj_component, headers)
                completed_dao_component_records += 1
                
                # Success adding the digital object!
                if dig_obj_component_uri:
                    write_out("    ✓ DAO component created with URI: %s" % dig_obj_component_uri, IGNORE_STDOUT)
                else:
                    write_out("    ✓ DAO component created [missing URI?]", IGNORE_STDOUT)

            except DAOCreationError as e:
                bad_dao_records.append(file_name)
                write_out(str(e))

            # update progressbar display
            bar.update(index)

    if bad_dao_records:
        write_out("  [Found %s DAOs that couldn't be created]" % len(bad_dao_records))
        write_out(' '.join(map(str, bad_dao_records)))


def post_digital_object_component(dig_obj_component: dict, headers: dict) -> Union[str, None]:
    """Post a single digital object component to ASpace

    Post the component to ASpace and return the new component's URI. Most of this function is
    spent understanding what error states we might have entered.
    """
    component_data = json.dumps(dig_obj_component)
    
    # post the DAO component
    try:
        dig_obj_post_raw = requests.post(ASPACE_URL + '/repositories/2/digital_object_components', headers=headers,
                                         data=component_data)
        dig_obj_post_raw.raise_for_status()
    except requests.exceptions.Timeout as e:
        raise DAOCreationError("    ❌ Timeout error. Is the server running, or do you need to connect"
                               " through a VPN? Continuing to next DAO component record.") from e
    except requests.exceptions.HTTPError as e:
        raise DAOCreationError("    ❌ Caught HTTP error. Continuing to next DAO component record.") from e
    except requests.exceptions.RequestException as e:
        raise DAOCreationError("    ❌ Error creating DAO component record."
                               " Continuing to next DAO component record.") from e

    # convert response to json object
    try:
        dig_obj_post = dig_obj_post_raw.json()
    except ValueError:
        raise DAOCreationError(
            "    ❌ Could not load request response as a json file. Continuing to next DAO component record.")

    # check if DAO component posted
    if "invalid_object" in dig_obj_post:
        raise DAOCreationError(
            "    ❌ Whoops, you tried to post an invalid object! Check your error logs and try again."
            " Continuing to next DAO component record.")

    # verify that we got a URI from successful post
    try:
        return dig_obj_post['uri']
    except KeyError:
        # don't know why we would get a successful post without a resulting URI
        return None

# put date json creation in a separate function because different types need different handling.
def create_date_json(jsontext, itemid, collection_dates):
    try:
        # we only care about the first instance of a date object
        date_obj = jsontext['dates'][0]
    except KeyError:
        # can't find the 'dates' object
        write_out("ERROR: can't find a 'dates' object for this AO record!")
        return None

    try:
        # pull out the date_expression field
        # this field may not exist in date_obj, but this is OK
        date_expression = date_obj['expression']
    except KeyError:
        date_expression = None

    try:
        # pull out the date_type field
        date_type = date_obj['date_type']
    except KeyError:
        # can't find the 'date_type' value
        write_out("ERROR: can't find a date 'date_type' value for this AO record!")
        return None

    # pull out beginning and end dates if they exist
    date_begin = date_obj['begin'] if 'begin' in date_obj else None
    date_end   = date_obj['end'] if 'end' in date_obj else None

    # set boolean flags to assess date structure.
    # we will look for two fields:
    #   - 'expression' is a free-text field
    #   - 'date_type' can be one of the following values:
    #       - bulk
    #       - inclusive
    #       - single
    has_expression = date_expression is not None
    is_undated = has_expression and ('undated' in date_expression)
    is_single = 'single' in date_type

    # return error if there isn't a date_expression nor beginning date
    if not date_begin and not has_expression:
        write_out("ERROR: AO record has no beginning date or date expression. " 
                        "Please check the metadata and try again.")
        return None

    # return error if there the date_expression is 'undated' and there isn't a beginning date
    if not date_begin and is_undated:
        write_out("ERROR: AO record has no beginning date and date expression is 'undated'. "
                        "Please check the metadata and try again.")
        return None

    # set default begin and end dates if date_expression is 'undated', 
    # and there are collection dates e.g., [1900, 1903]
    if is_undated and len(collection_dates) > 1:
        date_begin = collection_dates[0] if not date_begin else date_begin
        date_end   = collection_dates[1] if not date_end else date_end

    # Build expression text.
    if has_expression:
        dao_expression = date_expression
    elif is_single:
        dao_expression = date_begin
    elif date_end in date_begin:
        dao_expression = date_begin
    elif date_begin and date_end:
        dao_expression = "%s - %s" % (date_begin, date_end)
    else:
        dao_expression = date_begin

    # create DAO date_json object
    dao_date_json = {
                'date_type': date_type,
                'expression': dao_expression,
                'label': 'creation',
                'jsonmodel_type': 'date'
    }

    if date_begin:
        dao_date_json['begin'] = date_begin

    if not is_single:
        dao_date_json['end'] = date_begin

    return dao_date_json

def get_resource_type(instances: list) -> str:

    # We're looking for the original non-digital resource type.
    non_digital_resources = list(filter(lambda instance: 'digital' not in instance['instance_type'], instances))

    # Recognized instance types mapped to correct ASpace type.
    instance_type_map = {
        "text": "text",
        "books": "text",
        "scrapbooks": "text",
        "maps": "cartographic",
        "notated music": "notated_music",
        "audio": "sound_recording",
        "graphic_materials": "still_image",
        "photo": "still_image",
        "realia": "three dimensional object",
        "mixed_materials": "mixed_materials"
    }

    first_instance_type = non_digital_resources[0]['instance_type'].lower()
    return instance_type_map[first_instance_type]

# Sets a linked subject for the DAO to hold the Digital Commonwealth genre term based on the value set in the EAD-to-tab
# XSL. This mapping is based on database IDs for subjects in BC's production Aspace server and WILL NOT WORK for other
# schools/servers.
def get_genre_type(dc_genre_term: str) -> str:
    genre_term_to_subject_code = {
        "Albums": "656",
        "Books": "657",
        "Cards": "658",
        "Correspondence": "669",
        "Documents": "659",
        "Drawings": "660",
        "Ephemera": "661",
        "Manuscripts": "655",
        "Maps": "662",
        "Motion pictures": "668",
        "Music": "670",
        "Musical notation": "671",
        "Newspapers": "672",
        "Objects": "673",
        "Paintings": "663",
        "Periodicals": "664",
        "Photographs": "665",
        "Posters": "666",
        "Prints": "667",
        "Scrapbooks": "373",
        "Sound recordings": "674"
    }

    try:
        subject_code = genre_term_to_subject_code[dc_genre_term]
        return "/subjects/" + subject_code
    except KeyError:
        write_out(dc_genre_term + " is an invalid or improperly formatted genre term. "
                                  "Please check the Digital Commonwealth documentation and try again.")
        return None

# builds a [file version] segment for the Digital object component json that contains appropriate tech metadata from the
# FITS file. HARD CODED ASSUMPTIONS: Checksum type = MD5
def build_comp_file_version(filename, techmd_dict):
    check_value = techmd_dict[filename]['checksum']
    size = int(techmd_dict[filename]['filesize'])
    format_type = get_format_enum(techmd_dict[filename]['format'])
    use_statement = "master"
    if "INT" in filename:
        use_statement = "intermediate_copy"
    elif "ACC" in filename:
        use_statement = "access_copy"
    blob = [
        {
            'file_uri': filename, 
            'use_statement': use_statement, 
            'file_size_bytes': size, 
            'checksum_method': 'md5',
            'checksum': check_value, 
            'file_format_name': format_type, 
            'jsonmodel_type': 'file_version'
        }
    ]

    return blob


# translation table/function to turn FITS-reported file formats into ASpace enums
def get_format_enum(fits):
    filetype= ""
    if "TIFF" in fits:
        filetype = "tiff"
    if "Waveform" in fits:
        filetype = "wav"
    if "RF64" in fits:
        filetype = "rf64"
    if "Quicktime" in fits:
        filetype = "mov"
    if "Microsoft Word Binary File Format" in fits:
        filetype = "doc"
    if "Office Open XML Document" in fits:
        filetype = "docx"
    return filetype


# builds the notes section for the digital object component where techMD that can't live on the file version is stored.
# not all components have all metadata, try-catch blocks are needed to prevent key errors. Value > 0 tests are required
# because Aspace won't consider JSON valid if it contains an 'empty' note field.
# This function is currently commented out b/c decision was made not to store techMD in general notes. To re-enable,
# call from within 'dig_obj' variable definition at ~ line 325
"""
def build_comp_exif_notes(filename, techmd_dict):
    note_list = []
    try:
        if len(techmd_dict[filename]['duration-Ms']) > 0:
            note_list.append(note_builder(techmd_dict[filename]['duration-Ms'], 'duration Ms'))
    except KeyError:
        pass
    try:
        if len(techmd_dict[filename]['duration-H:M:S']) > 0:
            note_list.append(note_builder(techmd_dict[filename]['duration-H:M:S'], 'duration H:M:S'))
    except KeyError:
        pass
    try:
        if len(techmd_dict[filename]['sampleRate']) > 0:
            note_list.append(note_builder(techmd_dict[filename]['sampleRate'], 'sample rate'))
    except KeyError:
        pass
    try:
        if len(techmd_dict[filename]['bitDepth']) > 0:
            note_list.append(note_builder(techmd_dict[filename]['bitDepth'], 'bit depth'))
    except KeyError:
        pass
    try:
        if len(techmd_dict[filename]['pixelDimensions']) > 0:
            note_list.append(note_builder(techmd_dict[filename]['pixelDimensions'], 'pixel dimensions'))
    except KeyError:
        pass
    try:
        if len(techmd_dict[filename]['resolution']) > 0:
            note_list.append(note_builder(techmd_dict[filename]['resolution'], 'resolution'))
    except KeyError:
        pass
    try:
        if len(techmd_dict[filename]['bitsPerSample']) > 0:
            note_list.append(note_builder(techmd_dict[filename]['bitsPerSample'], 'bits per sample'))
    except KeyError:
        pass
    try:
        if len(techmd_dict[filename]['colorSpace']) > 0:
            note_list.append(note_builder(techmd_dict[filename]['colorSpace'], 'color space'))
    except KeyError:
        pass
    try:
        if len(techmd_dict[filename]['createDate']) > 0:
            note_list.append(note_builder(techmd_dict[filename]['createDate'], 'create date'))
    except KeyError:
        pass
    try:
        if len(techmd_dict[filename]['creatingApplicationName']) > 0:
            note_list.append(note_builder(techmd_dict[filename]['creatingApplicationName'], 'creating application name'))
    except KeyError:
        pass
    try:
        if len(techmd_dict[filename]['creatingApplicationVersion']) > 0:
            note_list.append(note_builder(techmd_dict[filename]['creatingApplicationVersion'], 'creating application version'))
    except KeyError:
        pass
    try:
        if len(techmd_dict[filename]['author']) > 0:
            note_list.append(note_builder(techmd_dict[filename]['author'], 'author'))
    except KeyError:
        pass
    try:
        if len(techmd_dict[filename]['title']) > 0:
            note_list.append(note_builder(techmd_dict[filename]['title'], 'title'))
    except KeyError:
        pass
    try:
        if len(techmd_dict[filename]['duration-Ms']) > 0:
            note_list.append(note_builder(techmd_dict[filename]['duration-Ms'], 'duration-Ms'))
    except KeyError:
        pass
    try:
        if len(techmd_dict[filename]['bitRate']) > 0:
            note_list.append(note_builder(techmd_dict[filename]['bitRate'], 'bit rate'))
    except KeyError:
        pass
    try:
        if len(techmd_dict[filename]['frameRate']) > 0:
            note_list.append(note_builder(techmd_dict[filename]['frameRate'], 'frame rate'))
    except KeyError:
        pass
    try:
        if len(techmd_dict[filename]['chromaSubsampling']) > 0:
            note_list.append(note_builder(techmd_dict[filename]['chromaSubsampling'], 'chroma subsampling'))
    except KeyError:
        pass
    return note_list
"""


def note_builder(list_index, label_value):
    note_text = {
        'jsonmodel_type': 'note_digital_object',
        'publish': False,
        'content': [list_index],
        'type': 'note',
        'label': label_value
    }
    return note_text


def get_file_type(filename):
    period_loc = filename.rfind('.')
    extension = filename[period_loc:len(filename)]
    if 'tif' in extension:
        value = 'image/tiff'
    elif 'wav' in extension:
        value = 'audio/und.wav'
    elif 'pdf' in extension:
        value = 'application/pdf'
    elif extension == 'doc':
        value = 'application/msword'
    elif 'docx' in extension:
        value = 'application/vnd.openxmlformats-officedocument.wordprocessingml.document'
    elif 'mov' in extension:
        value = 'video/quicktime'
    else:
        write_out("File extension for " + filename + " not recognized. "
                  "Please reformat files or add extension to get_file_type function")
        return None
    return value

def prompt_yes_no(question: str):
    valid = {"yes": True, "y": True, "ye": True, "no": False, "n": False}
    default = "n"

    while True:
        sys.stdout.write(question + " [y/N] ")
        choice = input().lower()
        if default is not None and choice == "":
            return valid[default]
        elif choice in valid:
            return valid[choice]
        else:
            sys.stdout.write("Please respond with 'yes' or 'no' " "(or 'y' or 'n').\n")

class InvalidEADRecordError(Exception):
    pass

class DAOCreationError(Exception):
    pass

class FatalRecordError(Exception):
    pass


main()
