Boston College ArchivesSpace Batch DAO Process

Object: To automatically generate description in ArchivesSpace for digitized archival materials, and to re-use that description to ingest files into the Digital Libraries repository (currently DigiTool).

Steps:
1. When material is selected for digitization, it is identified in ArchivesSpace by the addition of a Component Unique Identifier at the level of digitization (usually but not always the File level). The format of the Compnent Unique Identifier is as follows: the collection identifier reformatted as LLYYYY_NNN_ followed by the unique database ID number of the object in question (which is present in the ASpace URL when you are viewing the object as "tree::archival_object_NNNN - we are only interested in the number at the end of the URL, and it may consist of any number of digits). As an example, a properly formatted Component Unique Identifier may look like this: MS2013_043_54063.

2. Once all objects selected for digitization within a given collection have been marked with CUIs, export the EAD for the entire collection with only the "include <dao> tags" option checked.

3. Transform the collection EAD into two different tab delimited files, using aspace_ead_to_tab.xsl and scanners_tsv.xsl. The output from scanners_tsv.xsl should be sent to the digitization staff as a tracking worksheet for digitization, and the output from aspace_ead_to_tab.xsl will be used to run aspace_batch_dao.py once digitization is complete.

4. When the digitization is complete, run FITS over the image files. Run the fits-to-json XSL over the FITS xml file, to create a .json file of technical metadata.

5. Run aspace_batch_dao.py from the command line with the following usage: "aspace_batch_dao.py tab_file.txt fits-file.json" where tab_file.txt is the output of aspace_ead_to_tab.xsl, and fits-file.json is the output of step 4. This will call on the ArchivesSpace API to create a Digital Object and Digital Object Components for each object and the image files that represent it. If there are errors, the object metadata in ArchivesSpace or various aspects of the python script may need editing.

6. After running aspace_batch_dao.py, there will now be a folder in the directory the script was run from, containing METS files for each DAO created. See the wiki for documentation on how to use these files in repository ingest.