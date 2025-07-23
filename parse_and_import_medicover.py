import argparse
import datetime
import json
from pathlib import Path
import re
import uuid

import jq

from medicover_aws import db, utils


ACGS_CODES = [
    "PVS1",
    "PS1",
    "PS2",
    "PS3",
    "PS4",
    "PM1",
    "PM2",
    "PM3",
    "PM4",
    "PM5",
    "PM6",
    "PP1",
    "PP2",
    "PP3",
    "PP4",
    "BA1",
    "BS1",
    "BS2",
    "BS3",
    "BS4",
    "BP1",
    "BP2",
    "BP3",
    "BP4",
    "BP5",
    "BP7",
]


def main(
    reports,
    xlsx,
    panelapp_file,
    config_file,
    mapping_json_keys_file,
    mapping_rescued_panels,
    write,
    db_import,
    dump,
):
    db_creds = utils.parse_json(config_file)
    session, meta = db.connect_to_db(db_creds)
    inca_table = meta.tables["testdirectory.inca"]

    if dump:
        dump_data = utils.parse_json(dump)
        db.insert_in_db(session, inca_table, dump_data)
        exit()

    mapping_json_keys = utils.parse_json(mapping_json_keys_file)
    mapping_panels = utils.parse_xlsx(xlsx)
    panelapp_dump = utils.parse_tsv(
        panelapp_file, "id", "name", "relevant_disorders"
    )
    mapping_rescued_panels = utils.parse_tsv(
        mapping_rescued_panels, "raw_panel", "new_panel", "r_code"
    )

    medicover_data = mapping_panels[["CUH sample number", "Panels"]].to_dicts()

    sample_as_key = {
        row["CUH sample number"].upper(): {"Panels": row["Panels"]}
        for row in medicover_data
    }

    # insert a none column called r_code
    # loop through the panels
    for panel_data in panelapp_dump:
        relevant_disorders = eval(panel_data["relevant_disorders"])
        panel_name = panel_data["name"]
        r_code = r_code_info = []

        # find the r code
        for disorder in relevant_disorders:
            if re.search(r"R[0-9]+", disorder):
                r_code.append(disorder)

        if r_code:
            r_code_info = ", ".join(r_code)

        for sample, panels in sample_as_key.items():
            rescued = False

            # use the mapping to rescue some panels that aren't automatically
            # attributed a r-code using the panelapp dump
            for data in mapping_rescued_panels:
                raw_panel_data_to_match_rescue_mapping = ", ".join(
                    [ele.lstrip("_") for ele in panels["Panels"]]
                )

                if raw_panel_data_to_match_rescue_mapping == data["raw_panel"]:
                    if data["r_code"]:
                        sample_as_key[sample].setdefault("r_code", set()).add(
                            f"R{data['r_code']}"
                        )

                    sample_as_key[sample].setdefault("panel_name", set()).add(
                        data["new_panel"]
                    )

                    rescued = True
                    break

            if rescued:
                continue

            for panel in panels["Panels"]:
                matched = False

                if panel_name in panel:
                    matched = True
                    break

                for disorder in relevant_disorders:
                    if disorder in panel:
                        matched = True
                        break

            if matched and r_code_info:
                sample_as_key[sample].setdefault("r_code", set()).add(
                    r_code_info
                )
                sample_as_key[sample].setdefault("panel_name", set()).add(
                    panel_name
                )

    data_to_import = []

    nb_reports = len(reports)

    for i, report in enumerate(reports, 1):
        report_data = utils.parse_json(report)

        if jq.compile("keys").input_value(report_data).all() != [[0, 1, 2]]:
            print(f"Skipping {report} as it doesn't have any data")
            continue

        evaluations = utils.get_evaluations(report_data)

        for j, evaluation in enumerate(evaluations, 1):
            report_evaluation = f"{Path(report).stem}-{j}"

            if not evaluation:
                continue

            for variant_data in evaluation["variants"]:
                parsed_variant_data = {}

                # look for data in the report json
                for key, value in mapping_json_keys.items():
                    # hgvsc is not directly available to parse from the
                    # medicover data
                    # it can be obtained by combining 2 fields
                    if key == "hgvsc":
                        hgvsc = []

                        for jq_query in value:
                            jq_output = (
                                jq.compile(jq_query)
                                .input_value(variant_data)
                                .first()
                            )

                            if jq_output:
                                hgvsc.append(jq_output)

                        if hgvsc:
                            parsed_variant_data[key] = ":".join(hgvsc)
                        else:
                            parsed_variant_data[key] = None

                    # refalt contains the reference and alternate so it needs
                    # splitting out
                    elif key == "refalt":
                        jq_query = list(value.keys())[0]
                        db_key = list(value.values())[0]
                        ref_key, alt_key = db_key

                        jq_output = (
                            jq.compile(jq_query)
                            .input_value(variant_data)
                            .first()
                        )
                        ref, alt = jq_output.split("/")

                        parsed_variant_data[ref_key] = ref
                        parsed_variant_data[alt_key] = alt

                    elif key == "date_last_evaluated":
                        jq_query = value
                        jq_output = (
                            jq.compile(jq_query)
                            .input_value(evaluation)
                            .first()
                        )

                        if jq_output:
                            parsed_variant_data[key] = (
                                datetime.datetime.strptime(
                                    jq_output, "%m/%d/%Y"
                                ).strftime("%Y-%m-%d")
                            )
                        else:
                            parsed_variant_data[key] = None

                    # handle the ACGS codes
                    elif key == "code":
                        jq_query = value
                        jq_output = (
                            jq.compile(jq_query)
                            .input_value(variant_data)
                            .all()
                        )

                        for criteria in jq_output:
                            for code, strength in list(
                                zip(criteria, criteria[1:])
                            )[::2]:
                                strength = " ".join(
                                    strength.lower().capitalize().split("_")
                                )

                                if strength == "Standalone":
                                    strength = "Stand-Alone"

                                reformatted_code = code.split("_")[0]

                                if reformatted_code.upper() in ACGS_CODES:
                                    parsed_variant_data[
                                        reformatted_code.lower()
                                    ] = strength

                    elif key == "reported":
                        jq_query = value
                        jq_output = (
                            jq.compile(jq_query)
                            .input_value(variant_data)
                            .all()
                        )

                        if len(jq_output) == 1:
                            output = jq_output[0]

                            if output == "REPORTING":
                                output = "yes"
                            else:
                                output = "no"

                            parsed_variant_data["reported"] = output
                    else:
                        jq_query = key

                        jq_output = (
                            jq.compile(jq_query)
                            .input_value(variant_data)
                            .all()
                        )

                        formatted_output = " | ".join(
                            [
                                str(ele).replace(", which is", "")
                                for ele in jq_output
                            ]
                        )

                        if (
                            formatted_output
                            == "GRCh_37_g1k,Chromosome,Homo sapiens"
                        ):
                            formatted_output = "GRCh37.p13"

                        # rescue gene symbol when geneName field doesn't exist
                        if (
                            formatted_output == "None"
                            and value == "gene_symbol"
                        ):
                            jq_output = (
                                jq.compile(".acmgScoring.interpretedGene")
                                .input_value(variant_data)
                                .all()
                            )

                            formatted_output = " ".join(jq_output)

                        # need to keep the gene symbol uppercase
                        elif value == "gene_symbol":
                            formatted_output = " ".join(
                                formatted_output.split("_")
                            )
                        else:
                            formatted_output = " ".join(
                                formatted_output.lower()
                                .capitalize()
                                .split("_")
                            )

                        parsed_variant_data[value] = formatted_output

                match = re.search(
                    r"(?P<gm_number>GM[0-9]{2}_[0-9]+)", report, re.IGNORECASE
                )

                if match:
                    gm_number = match.group("gm_number")
                    gm_number = gm_number.replace("_", ".")
                    gm_number_data = sample_as_key.get(gm_number, None)

                    if gm_number_data:
                        r_codes = gm_number_data.get("r_code", None)
                        panels = ", ".join(
                            [
                                panel.strip("_")
                                for panel in gm_number_data["Panels"]
                            ]
                        )

                        if gm_number_data.get("panel_name"):
                            parsed_variant_data["preferred_condition_name"] = (
                                ", ".join(gm_number_data["panel_name"])
                            )

                        if r_codes:
                            parsed_variant_data["r_code"] = ", ".join(r_codes)

                        parsed_variant_data["panel"] = panels

                    else:
                        parsed_variant_data["panel"] = (
                            "Sample not in Medicover data"
                        )

                unique_id = f"uid_{uuid.uuid1().time}"
                parsed_variant_data["local_id"] = unique_id
                parsed_variant_data["linking_id"] = unique_id
                parsed_variant_data["institution"] = (
                    "East Genomic Laboratory Hub, NHS Genomic Medicine Service"
                )
                parsed_variant_data["organisation"] = (
                    "Cambridge Genomics Laboratory"
                )
                parsed_variant_data["organisation_id"] = 288359
                parsed_variant_data["collection_method"] = "clinical testing"
                parsed_variant_data["allele_origin"] = "germline"
                parsed_variant_data["affected_status"] = "yes"
                parsed_variant_data["interpreted"] = "yes"
                parsed_variant_data["probeset_id"] = "Medicover TWE"
                parsed_variant_data["report_evaluation"] = report_evaluation

                data_to_import.append(parsed_variant_data)

        print(f"{i}/{nb_reports} reports have processed")

    correct_data_to_import = utils.add_missing_keys(data_to_import)

    if write:
        with open("json_dump_ready_for_import.json", "w") as f:
            json.dump(correct_data_to_import, f, indent=2)

    if db_import:
        db.insert_in_db(session, inca_table, correct_data_to_import)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("report", nargs="+", help="Medicover report JSON file")
    parser.add_argument(
        "-p",
        "--panelapp_dump",
        help="Panelapp dump file so that the code doesn't have to rerun the heavy API query",
    )
    parser.add_argument(
        "-x",
        "--xlsx",
        help="Medicover xlsx file containing mapping from GM number to panels",
    )
    parser.add_argument(
        "-c",
        "--config",
        help="Config JSON file containing the credential info for the AWS db",
    )
    parser.add_argument(
        "-mj",
        "--mapping_json",
        help=(
            "JSON file containing the mapping between fields in the "
            "Medicover report and the db",
        ),
    )
    parser.add_argument(
        "-mp",
        "--mapping_panels",
        help=(
            "TSV file containing a mapping between the medicover panels and "
            "panelapp panels",
        ),
    )
    parser.add_argument(
        "-db",
        "--db",
        action="store_true",
        default=False,
        help="Import in the database",
    )
    parser.add_argument(
        "-w",
        "--write",
        action="store_true",
        default=False,
        help="Write data into a file",
    )
    parser.add_argument(
        "-d",
        "--dump",
        help="Dump of data to import, bypasses all the processing to do only the import",
    )

    args = parser.parse_args()
    main(
        args.report,
        args.xlsx,
        args.panelapp_dump,
        args.config,
        args.mapping_json,
        args.mapping_panels,
        args.write,
        args.db,
        args.dump,
    )
