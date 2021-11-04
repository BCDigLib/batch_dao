# modified version of the script written by the Bentley Historical Library to automatically create Digital Objects by
# re-using archival object metadata in ArchivesSpace. Original script available at
# https://github.com/djpillen/bentley_scripts/blob/master/update_archival_object.py
#
# This script recreates the first steps in using a csv to locate archival objects and retrieve their metadata,
# but adds additional project-dependant metadata before creating the Digital Object, and creates Digital Object
# Components to hang file information off of rather than attaching files directly to the Digital Object.
# Also imports technical metadata from an exif file and stores it on the pertinent Digital Object Components.
# METS exports for every created Digital Object are also saved off in a folder labeled "METS".

# USAGE: 
#    aspace_batch_dao.py tab_file.txt fits_file.json
# where tab_file.txt is the output of aspace_ead_to_tab.xsl
# and fits_file.json is the output of running fit-to-json.xsl over a FITS xml file.

import requests
import json
import sys
import os
from datetime import datetime
from dotenv import load_dotenv

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

    # check to see if we have the correct number of arguments
    if len(sys.argv) != 3:
        write_out("Please use the correct number of arguments.\nUsage: aspace_batch_dao.py tab_file.tsv fits_file.json")
        sys.exit()

    # next, check if first argument exists as a file
    tab_file = sys.argv[1]
    if not os.path.isfile(tab_file):
        write_out("The file '%s' does not exist. Exiting." % tab_file)
        sys.exit()

    # next, check if second argument exists as a file
    fits_techmd_file = sys.argv[2]
    if not os.path.isfile(fits_techmd_file):
        write_out("The file '%s' does not exist. Exiting." % fits_techmd_file)
        sys.exit()

    #
    # Collect ASpace session token
    #

    # read in secrets from .env file
    load_dotenv()

    ASPACE_URL      = os.getenv('ASPACE_URL')
    ASPACE_USERNAME = os.getenv('ASPACE_USERNAME')
    ASPACE_PASSWORD = os.getenv('ASPACE_PASSWORD')

    if ASPACE_URL is None or ASPACE_USERNAME is None or ASPACE_PASSWORD is None:
        write_out("Error loading .env file! Exiting.")
        sys.exit()

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
    # Open the FITS file and read in its contents. This dictionary is used for techMD calls, also create another dict
    # for file lists.
    #

    # read in fits_techmd_file: sys.argv[2]
    try:
        tech_in = open(fits_techmd_file, 'r')
    except OSError as e:
        write_out("❌ Could not open/read file: %s"  % fits_techmd_file)
        raise SystemExit(e)

    # load file as a json object
    try:
        tech_data = json.load(tech_in)
    except ValueError as e:
        write_out("❌ Could not load json file: %s"  % fits_techmd_file)
        raise SystemExit(e)

    write_out("✓ Read in fits_techmd_file: %s" % fits_techmd_file)

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
    for key, values in tech_data.items():               # "BC2001_074_64862_0000.tif"
        # integer in the line below may need updating 
        # for items with differently formatted CUIs
        cutoff = key.replace('_', "|", 2).find('_')     # "BC2001|074|64862_0000.tif"
                                                        #                  ^
        short_name = key[0:cutoff]                      # "BC2001_074_64862"

        # add short_name to dictionary
        if short_name not in files_listing:
            files_listing[short_name] = [key]
        elif short_name in files_listing:
            files_listing[short_name].append(key)


    #
    # Open the TSV file created with aspace_ead_to_tab.xsl to gather variables and make the API calls
    #

    # read in tab_file: sys.argv[1]
    try:
        tab_in = open(tab_file, 'r')
    except OSError as e:
        write_out("❌ Could not open/read file: %s"  % tab_file)
        raise SystemExit(e)

    # load file as a json object
    try:
        ead_data = tab_in.read()
    except ValueError as e:
        write_out("❌ Could not load json file: %s"  % tab_file)
        raise SystemExit(e)

    write_out("✓ Read in tab_file: %s" % tab_file)
    
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
        write_out("[%s] %s - %s" % (index + 1, aspace_id, metadata[4]) )
        write_out("###########")

        # look up AO record by ref_id pulled from EAD
        params  = {'ref_id[]': id_ref}

        # define AO record URL
        ao_record_url = ASPACE_URL + '/repositories/2/find_by_id/archival_objects'
        write_out("⋅ fetching AO by ref_id: %s" % id_ref)
        # write_out("    [using AO API url: %s]" % ao_record_url )

        # fetch record
        try:
            lookup_raw  = requests.get(ao_record_url, headers=headers, params=params)
            lookup_raw.raise_for_status()
        except requests.exceptions.Timeout as e:
            write_out("  ❌ Timeout error. Is the server running, or do you need to connect through a VPN? Continuing to next AO record.")
            write_out(str(e))
            continue
        except requests.exceptions.HTTPError as e:
            write_out("  ❌ Caught HTTP error. Continuing to next AO record.")
            write_out(str(e))
            continue
        except requests.exceptions.RequestException as e:
            write_out("  ❌ Error loading ASpace record. Continuing to next AO record.")
            continue

        # convert response to json object
        try:
            lookup = lookup_raw.json()
        except ValueError:
            write_out("  ❌ Could not load request response as a json file. Continuing to next AO record.")
            continue

        # get the URI of the AO
        try:
            archival_object_uri  = lookup['archival_objects'][0]['ref']
            write_out("  ✓ found AO URI: %s" % archival_object_uri)
        except ValueError:
            write_out("  ❌ Could not find ['archival_objects'][0]['ref'] value. Continuing to next AO record.")
            continue

        # define AO record URL
        ao_record_url = ASPACE_URL + archival_object_uri
        write_out("⋅ using AO URI to fetch individual AO record")
        write_out("    [using AO API url: %s]" % ao_record_url)

        # get the full AO json representation
        try:
            archival_object_json_raw = requests.get(ao_record_url, headers=headers)
            archival_object_json_raw.raise_for_status()
            write_out("  ✓ found AO object")
        except requests.exceptions.Timeout as e:
            write_out("  ❌ Timeout error. Is the server running, or do you need to connect through a VPN? Continuing to next AO record.")
            write_out(str(e))
            continue
        except requests.exceptions.HTTPError as e:
            write_out("  ❌ Caught HTTP error. Continuing to next AO record.")
            write_out(str(e))
            continue
        except requests.exceptions.RequestException as e:
            write_out("  ❌ Error loading ASpace record. Continuing to next AO record.")
            continue

        # convert response to json object
        try:
            archival_object_json = archival_object_json_raw.json()
        except ValueError:
            write_out("  ❌ Could not load request response as a json file. Continuing to next AO record.")
            continue

        # check for necessary metadata & only proceed if it's all present.
        write_out("\n  ##### JSON OUTPUT BEGIN - FETCH AO #####", IGNORE_STDOUT)
        write_out(json.dumps(archival_object_json, indent=4, sort_keys=True), IGNORE_STDOUT)
        write_out("  ##### JSON OUTPUT END - FETCH AO #####\n", IGNORE_STDOUT)

        write_out("⋅ looking for various metadata values:")

        # look for component unique ID
        try:
            unique_id = archival_object_json['component_id']
            write_out("  ✓ component unique ID: %s" % unique_id)
        except KeyError:
            write_out("  ❌ Please make sure all items have a component unique ID before creating DAOs. Continuing to next AO record.")
            #sys.exit()
            continue

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
                    date_end   = archival_object_json['dates'][0]['end']
                    obj_title = "%s - %s" % (date_begin, date_end)
                except KeyError:
                    write_out("  ❌ Item %s has no title or date expression or date begin/end. Please check the metadata & try again. Continuing to next AO record." % unique_id)
                    #sys.exit()
                    continue

        write_out("  ✓ object title: %s" % obj_title)

        # look for linked agents
        try:
            agent_data = archival_object_json['linked_agents']
            write_out("  ✓ linked agents found")
        except KeyError:
            write_out("  ✓ did not find any linked agents")
            agent_data = []

        # check for expression type 'single' before looking for both start and end dates
        date_json = create_date_json(archival_object_json, unique_id, collection_dates)
        write_out("  ✓ generated date json object")
        #write_out(json.dumps(date_json, indent=4, sort_keys=True), IGNORE_STDOUT)

        write_out("⋅ gathering list of filenames from FITS dictionary by unique ID: %s" % unique_id)

        # pull in files list from FITS dictionary to get data to create digital object components and thumbnail
        try:
            file_names = files_listing[unique_id]
            file_names.sort()
        except KeyError as e:
            write_out("  ❌ Can't find unique_id %s in dictionary of file names. Check the FITS file and try again. Continuing to next AO record." % unique_id)
            continue

        write_out("    [number of file names pulled from FITS dictionary matching unique ID: %s]" % len(file_names))
        write_out("  ✓ first file name pulled from FITS dictionary matching unique ID: %s" % file_names[0])

        # derive resource type
        resource_type = get_resource_type(archival_object_json, id_ref)
        write_out("⋅ deriving resource type:")
        write_out("  ✓ %s" % resource_type)

        # derive file type
        file_type = get_file_type(files_listing[unique_id][0])
        write_out("⋅ deriving file type:")
        write_out("  ✓ %s" % file_type)

        # derive thumbnail URI
        file_name_whole = file_names[0]                     # BC2001_074_64862_0000.tif
        file_name_split = os.path.splitext(file_name_whole) # ('BC2001_074_64862_0000', 'tif')
        if not file_name_split[0]:
            write_out("  ❌ Couldn't derive thumbnail URI from file_names list. Continuing to next AO record.")
            continue

        thumbnail_URI = 'https://iiif.bc.edu/%s.jp2/full/!200,200/0/default.jpg' % file_name_split[0]
        write_out("⋅ deriving thumbnail URI:")
        write_out("  ✓ %s" % thumbnail_URI)

        # derive handle URI
        handle_URI = 'http://hdl.handle.net/2345.2/%s' % unique_id
        write_out("⋅ deriving handle URI:")
        write_out("  ✓ %s" % handle_URI)

        # generate JSON object for creating DAO
        dig_obj = {
            'jsonmodel_type': 'digital_object',
            'title': obj_title, 
            'digital_object_type': resource_type, 
            'lang_materials':[
                {
                    'jsonmodel_type': 'lang_material',
                    'language_and_script': {
                        'language': lang_code, 
                        'jsonmodel_type': 'language_and_script'
                    }
                }
            ],
            'file_versions':[
                {
                    'file_uri': handle_URI, 
                    'publish': True, 
                    'is_representative': True, 
                    'jsonmodel_type': 'file_version',
                    'xlink_actuate_attribute': 'onRequest', 
                    'xlink_show_attribute': 'new'
                }, 
                {
                    'file_uri': thumbnail_URI,
                    'publish': True, 
                    'xlink_actuate_attribute': 'onLoad',
                    'xlink_show_attribute': 'embed', 
                    'is_representative': False, 
                    'jsonmodel_type': 'file_version'
                }
            ],
            'digital_object_id': handle_URI, 
            'publish': True, 
            'notes':[
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
            'dates': date_json,
            'linked_agents': agent_data, 
            'subjects': get_genre_type(genre)
        }

        # format the JSON
        dig_obj_data = json.dumps(dig_obj)

        write_out("\n  ##### JSON OUTPUT BEGIN - CREATE DAO #####", IGNORE_STDOUT)
        write_out(json.dumps(dig_obj, indent=4, sort_keys=True), IGNORE_STDOUT)
        write_out("  ##### JSON OUTPUT END - CREATE DAO #####\n", IGNORE_STDOUT)

        #continue

        # 
        # create DAO records
        #

        write_out("⋅ creating DAO")

        # next, post the DAO
        try:
            dig_obj_post_raw  = requests.post(ASPACE_URL + '/repositories/2/digital_objects', headers=headers, data=dig_obj_data)
            dig_obj_post_raw.raise_for_status()
        except requests.exceptions.Timeout as e:
            write_out("  ❌ Timeout error. Is the server running, or do you need to connect through a VPN? Continuing to next AO record.")
            write_out(str(e))
            continue
        except requests.exceptions.HTTPError as e:
            write_out("  ❌ Caught HTTP error. Continuing to next AO record.")
            write_out(str(e))
            continue
        except requests.exceptions.RequestException as e:
            write_out("  ❌ Error posting DAO record. Continuing to next record.")
            continue

        # convert response to json object
        try:
            dig_obj_post = dig_obj_post_raw.json()
        except ValueError:
            write_out("  ❌ Could not load request response as a json file. Continuing to next AO record.")
            continue

        # grab the newly created DAO URI and only proceed if the DAO hasn't already been created
        try:
            dig_obj_uri = dig_obj_post['uri']
            write_out("  ✓ DAO created with URI: %s" % dig_obj_uri)
        except KeyError:
            write_out("  ❌ DAO for item %s already exists. Continuing to next AO record." % unique_id)
            # TODO: rather than continue to next record, go to next step to update AO?
            continue

        # save the ID of the newly created DAO to feed the IIIF manifest generator
        id_start = dig_obj_uri.rfind('/')                           # /repositories/2/archival_objects/1234567
                                                                    #                                 ^
        dig_obj_id = dig_obj_uri[(id_start+1):len(dig_obj_uri)]     # 1234567
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
            archival_object_update_raw = requests.post(ASPACE_URL + archival_object_uri, headers=headers, data=archival_object_data)
            archival_object_update_raw.raise_for_status()
        except requests.exceptions.Timeout as e:
            write_out("  ❌ Timeout error. Is the server running, or do you need to connect through a VPN? Continuing to next AO record.")
            write_out(str(e))
            continue
        except requests.exceptions.HTTPError as e:
            write_out("  ❌ Caught HTTP error. Continuing to next AO record.")
            write_out(str(e))
            continue
        except requests.exceptions.RequestException as e:
            write_out("  ❌ Error updating AO record. Continuing to next AO record.")
            continue

        # convert response to json object
        try:
            archival_object_update = archival_object_update_raw.json()
        except ValueError:
            write_out("  ❌ Could not load request response as a json file. Continuing to next AO record.")
            continue

        # TODO: verify post by checking URI from request update to known AO URI
        write_out("  ✓ AO record updated")

        #
        # create DAO component records
        #

        # generate DAO components from list of file names for this AO
        write_out("⋅ generating DAO components")
        write_out("    [attempting to create %s records]" % len(file_names))

        completed_dao_component_records = 0
        bad_dao_records = []
        for index, file_name in enumerate(file_names):
            write_out("  [%s] %s" %(index, file_name), IGNORE_STDOUT)

            # derive base file name
            period_loc = file_name.index('.')
            base_name = file_name[0:period_loc]

            # create DAO component json object
            dig_obj = {
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

            dig_obj_data = json.dumps(dig_obj)
            #write_out(dig_obj_data, IGNORE_STDOUT)

            # next, post the DAO component
            try:
                dig_obj_post_raw = requests.post(ASPACE_URL + '/repositories/2/digital_object_components', headers=headers, data=dig_obj_data)
                dig_obj_post_raw.raise_for_status()
            except requests.exceptions.Timeout as e:
                write_out("    ❌ Timeout error. Is the server running, or do you need to connect through a VPN? Continuing to next DAO component record.")
                write_out(str(e))
                bad_dao_records.append(file_name)
                continue
            except requests.exceptions.HTTPError as e:
                write_out("    ❌ Caught HTTP error. Continuing to next DAO component record.")
                write_out(str(e))
                bad_dao_records.append(file_name)
                continue
            except requests.exceptions.RequestException as e:
                write_out("    ❌ Error creating DAO component record. Continuing to next DAO component record.")
                bad_dao_records.append(file_name)
                continue

            # convert response to json object
            try:
                dig_obj_post = dig_obj_post_raw.json()
            except ValueError:
                write_out("    ❌ Could not load request response as a json file. Continuing to next DAO component record.")
                bad_dao_records.append(file_name)
                continue

            # check if DAO component posted
            if "invalid_object" in dig_obj_post:
                write_out("    ❌ Whoops, you tried to post an invalid object! Check your error logs and try again. Continuing to next DAO component record.")
                bad_dao_records.append(file_name)
                continue

            # verify that we got a URI from successful post
            try:
                dig_obj_component_uri = dig_obj_post['uri']
                write_out("    ✓ DAO component created with URI: %s" % dig_obj_component_uri, IGNORE_STDOUT)
            except KeyError:
                # don't know why we would get a successful post without a resulting URI
                write_out("    ✓ DAO component created [missing URI?]", IGNORE_STDOUT)

            completed_dao_component_records += 1

        write_out("  ✓ created %s DAO component records" % completed_dao_component_records )
        if bad_dao_records:
            write_out("  [Found %s DAOs that couldn't be created]" % len(bad_dao_records) )
            write_out(' '.join(map(str, bad_dao_records)))

        """
        #
        # create METS output
        #

        # this portion is no longer required since we stopped adding collections data to the ILS

        write_out("⋅ saving local METS export file for AO")

        # create the mets call URI by modifying the DAO uri
        id_start = dig_obj_uri.rfind('/')   # /repositories/2/archival_objects/1234567
                                            #                                 ^
        mets_uri = dig_obj_uri[0:id_start] + '/mets' + dig_obj_uri[id_start:len(dig_obj_uri)] + '.xml'
                                            # /repositories/2/archival_objects/mets/1234567.xml
        
        write_out("    [METS endpoint URI: %s]" % mets_uri)

        # save the METS export for the completed and fully component-laden DAO
        try:
            mets_call = requests.get(ASPACE_URL + mets_uri, headers=headers)
            mets_call.raise_for_status()
        except requests.exceptions.RequestException as e:
            write_out("  ❌ Error fetching AO METS record. Continuing to next AO record.")
            continue
        except requests.exceptions.Timeout as e:
            write_out("  ❌ Timeout error. Is the server running, or do you need to connect through a VPN? Continuing to next AO record.")
            write_out(str(e))
            continue
        except requests.exceptions.HTTPError as e:
            write_out("  ❌ Caught HTTP error. Continuing to next AO record.")
            write_out(str(e))
            continue

        write_out("  ✓ fetched METS metadata")
        mets_file = mets_call.text

        # create local METS directory if it doesn't exist
        if not os.path.exists('METS'):
            os.makedirs('METS')

        # save local METS file
        mets_file_out = 'METS/%s.xml' % unique_id
        with open(mets_file_out, 'w') as outfile:
            outfile.write(mets_file)
            write_out("  ✓ saved to file: %s" % mets_file_out)
        outfile.close()
        """

    end_now = datetime.now()
    current_time = end_now.strftime("%Y-%m-%d, %H:%M:%S")

    write_out("\nscript stop time: %s\n" % current_time)
    write_out("elasped time: %s\n" % (end_now - start_now) )


# put date json creation in a separate function because different types need different handling.
def create_date_json(jsontext, itemid, collection_dates):
    if "single" in jsontext['dates'][0]['date_type']:
        try:
            start_date = jsontext['dates'][0]['begin']
        except KeyError:
            write_out(
                "Item " + itemid + " has a single-type date with no start value. Please check the metadata & try again")
            sys.exit()
        try:
            expression = jsontext['dates'][0]['expression']
        except KeyError:
            expression = start_date
        date_json = [
            {
                'begin': start_date, 
                'date_type': 'single', 
                'expression': expression, 
                'label': 'creation', 
                'jsonmodel_type': 'date'
            }
        ]
        return date_json
    elif "single" not in jsontext['dates'][0]['date_type']:
        date_type = jsontext['dates'][0]['date_type']
        try:
            start_date = jsontext['dates'][0]['begin']
        except KeyError:
            try:
                expression = jsontext['dates'][0]['expression']
            except KeyError:
                write_out(itemid + " has no start date or date expression. Please check the metadata and try again.")
                sys.exit()
            if "undated" in expression:
                start_date = collection_dates[0]
                end_date = collection_dates[1]
                date_json = [
                    {
                        'begin': start_date, 
                        'end': end_date, 
                        'date_type': date_type, 
                        'expression': expression,
                        'label': 'creation', 
                        'jsonmodel_type': 'date'
                    }
                ]
                return date_json
            else:
                write_out(itemid + " has no start date and date expression is not 'undated'. Please check the metadata and try again")
                sys.exit()
        try:
            end_date = jsontext['dates'][0]['end']
        except KeyError:
            write_out("Item " + itemid + " has no end date. Please check the metadata & try again")
            sys.exit()
        try:
            expression = jsontext['dates'][0]['expression']
        except KeyError:
            if end_date in start_date:
                expression = start_date
            else:
                expression = start_date + "-" + end_date
        date_json = [
            {
                'begin': start_date, 
                'end': end_date, 
                'date_type': date_type, 
                'expression': expression, 
                'label': 'creation', 
                'jsonmodel_type': 'date'
            }
        ]
        return date_json


# Sets an Aspace instance type for the DAO based on the physical instance of the AO
def get_resource_type(ao_json, item_id):
    instance_type = ""
    for instance in ao_json['instances']:
        if 'digital' in instance['instance_type']:
            pass
        else:
            instance_type = instance['instance_type']
            break
    if instance_type == "text" or instance_type == "books":
        return "text"
    elif instance_type == "maps":
        return "cartographic"
    elif instance_type == "notated music":
        return "notated_music"
    elif instance_type == "audio":
        return "sound_recording"
    elif instance_type == "graphic_materials" or instance_type == "photo":
        return "still_image"
    elif instance_type == "moving_images":
        return "moving_image"
    elif instance_type == "realia":
        return "three dimensional object"
    elif instance_type == "mixed_materials":
        return "mixed_materials"
    elif instance_type == "Scrapbooks":         # TODO: be sure to revisit this value and make sure it maps to the ASpace instance type
        return "text"
    else:
        write_out(item_id + " can't be assigned a typeOfResource based on the physical istance. Please check the metadata & try again.")
        sys.exit()


# Sets a linked subject for the DAO to hold the Digital Commonwealth genre term based on the value set in the EAD-to-tab
# XSL. This mapping is based on database IDs for subjects in BC's production Aspace server and WILL NOT WORK for other
# schools/servers.
def get_genre_type(dc_genre_term):
    if dc_genre_term == "Albums":
        return [{"ref": "/subjects/656"}]
    elif dc_genre_term == "Books":
        return [{"ref": "/subjects/657"}]
    elif dc_genre_term == "Cards":
        return [{"ref": "/subjects/658"}]
    elif dc_genre_term == "Correspondence":
        return [{"ref": "/subjects/669"}]
    elif dc_genre_term == "Documents":
        return [{"ref": "/subjects/659"}]
    elif dc_genre_term == "Drawings":
        return [{"ref": "/subjects/660"}]
    elif dc_genre_term == "Ephemera":
        return [{"ref": "/subjects/661"}]
    elif dc_genre_term == "Manuscripts":
        return [{"ref": "/subjects/655"}]
    elif dc_genre_term == "Maps":
        return [{"ref": "/subjects/662"}]
    elif dc_genre_term == "Motion pictures":
        return [{"ref": "/subjects/668"}]
    elif dc_genre_term == "Music":
        return [{"ref": "/subjects/670"}]
    elif dc_genre_term == "Musical notation":
        return [{"ref": "/subjects/671"}]
    elif dc_genre_term == "Newspapers":
        return [{"ref": "/subjects/672"}]
    elif dc_genre_term == "Objects":
        return [{"ref": "/subjects/673"}]
    elif dc_genre_term == "Paintings":
        return [{"ref": "/subjects/663"}]
    elif dc_genre_term == "Periodicals":
        return [{"ref": "/subjects/664"}]
    elif dc_genre_term == "Photographs":
        return [{"ref": "/subjects/665"}]
    elif dc_genre_term == "Posters":
        return [{"ref": "/subjects/666"}]
    elif dc_genre_term == "Prints":
        return [{"ref": "/subjects/667"}]
    elif dc_genre_term == "Scrapbooks":         # TODO: verify this is the correct subject object ID
        return [{"ref": "/subjects/373"}]
    elif dc_genre_term == "Sound recordings":
        return [{"ref": "/subjects/674"}]
    else:
        write_out(dc_genre_term + " is an invalid or improperly formatted genre term. Please check the Digital Commonwealth documentation and try again.")
        sys.exit()


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
        write_out("File extension for " + filename + " not recognized. Please reformat files or add extension to get_file_type function")
        sys.exit()
    return value


main()