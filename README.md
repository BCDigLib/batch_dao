# Boston College ArchivesSpace Batch DAO Process

## Object
To automatically generate description in ArchivesSpace for digitized archival materials, and to re-use that description to ingest files into a Digital Libraries repository.

## Steps:

### Select for digitization
When material is selected for digitization, it is identified in ArchivesSpace by the addition of a **Component Unique Identifier** (CUI) at the level of digitization (usually but not always the File level). The format of the Component Unique Identifier is as follows: the collection identifier reformatted as `LLYYYY_NNN_` followed by the unique database ID number of the object in question. This is present in the ASpace URL when you are viewing the object as `"tree::archival_object_NNNN"` - we are only interested in the number at the end of the URL, and it may consist of any number of digits. As an example, a properly formatted Component Unique Identifier may look like this: `MS2013_043_54063`.

### Export EAD
Once all objects selected for digitization within a given collection have been marked with CUIs, export the EAD for the entire collection with only the "include <dao> tags" option checked.

### Transform EAD
Transform the collection EAD into two different tab delimited files, using aspace_ead_to_tab.xsl and scanners_tsv.xsl. 

* The output from [aspace_ead_to_tab.xsl](aspace_ead_to_tab.xsl) will be used to run aspace_batch_dao.py once digitization is complete. The output of this transofrmation can be named `tab_file.tsv`.
* The output from [scanners_tsv.xsl](scanners_tsv.xsl) should be sent to the digitization staff as a tracking worksheet for digitization. The output of this transformation can be named `scanner-worsksheet.tsv`.

### Generate FITS and transform into a JSON file
When the digitization is complete, run FITS over the image files. Run the [fits-to-json.xsl](fits-to-json.xsl) XSL over the FITS xml file, to create a JSON file of technical metadata. The output of this transformation can be named `fits-file.json`.

### Run batch DAO script
Run [aspace_batch_dao.py](aspace_batch_dao.py) from the command line with the following usage: 

`aspace_batch_dao.py tab_file.tsv fits-file.json` 

* where `tab_file.tsv` is the output of aspace_ead_to_tab.xsl,
* and `fits-file.json` is the output of the FITS transformation. 

This script will call on the ArchivesSpace API to create a Digital Object, and one or more Digital Object Components for each object and the image files that represent it. If there are errors, the object metadata in ArchivesSpace or various aspects of the python script may need editing.

### Batch script METS output
After running aspace_batch_dao.py, there will now be a folder in the directory the script was run from named `METS`, which contains METS metadata files for each DAO created. See the BC wiki for documentation on how to use these files in repository ingest.