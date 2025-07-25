# medicover_aws

Script to import medicover data into the INCA database for final import to Clinvar: https://cuhbioinformatics.atlassian.net/browse/DI-1456

## Files

- Medisend_Manifest excel file obtained from Wook
- Panelapp dump for the signedoff version of the panels (obtained using the Panelapp package in pypi) -> tsv with panel_id, panel name, relevant_disorders
- Mapping used to map panels that couldn't be assigned r-codes. Manually created to handle ~200 cases where the panel was not suitable for Clinvar

## How to run

```bash
python parse_and_import_medicover.py \
${report} [${report} ...] \
-x ${medicover_manifest}$ \
-p ${panelapp_dump}$ \
-c ${db_json_config} \
-mj ${mapping_for_keys_in_report_json} \
-mp ${manually_created_mapping_file} \
-w # to write the output of the parsing (can be useful to find issues)
-db # to import in the database
-d ${previously_created_dump} # to bypass the parsing and import the previously written dump
```
