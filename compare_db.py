import argparse

from sqlalchemy import select

from medicover_aws import db, utils


col_to_check = [
    "germline_classification",
    "collection_method",
    "allele_origin",
    "consequence",
    "probeset_id",
    "ref_genome",
    "pvs1",
    "ps1",
    "ps2",
    "ps3",
    "ps4",
    "pm1",
    "pm2",
    "pm3",
    "pm4",
    "pm5",
    "pm6",
    "pp1",
    "pp2",
    "pp3",
    "pp4",
    "bs1",
    "bs2",
    "bs3",
    "bs4",
    "bp1",
    "bp2",
    "bp3",
    "bp4",
    "bp5",
    "bp7",
]


def main(config_dev, config_prod):
    dev_db_creds = utils.parse_json(config_dev)
    prod_db_creds = utils.parse_json(config_prod)
    dev_session, dev_meta = db.connect_to_db(dev_db_creds)
    prod_session, prod_meta = db.connect_to_db(prod_db_creds)

    dev_inca_table = dev_meta.tables["testdirectory.inca"]
    prod_inca_table = prod_meta.tables["testdirectory.inca"]

    data = {"dev": {}, "prod": {}}

    for col in dev_inca_table.c:
        data["dev"][col.name] = list(
            set(dev_session.execute(select(col)).all())
        )

    for col in prod_inca_table.c:
        data["prod"][col.name] = list(
            set(prod_session.execute(select(col)).all())
        )

    cols = []

    for col in col_to_check:
        dev_values = set(
            sorted([str(value) for ele in data["dev"][col] for value in ele])
        )
        prod_values = set(
            sorted([str(value) for ele in data["prod"][col] for value in ele])
        )

        cols.append(
            (
                col,
                ", ".join(sorted(list(dev_values.intersection(prod_values)))),
                ", ".join(sorted(list(dev_values.difference(prod_values)))),
                ", ".join(sorted(list(prod_values.difference(dev_values)))),
            )
        )

    lines = [
        [col[0] for col in cols],
        [col[1] for col in cols],
        [col[2] for col in cols],
        [col[3] for col in cols],
    ]

    with open("db_comparison.tsv", "w") as f:
        for line in lines:
            f.write(f"{'\t'.join(line)}\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("config_dev", help="Db config for INCA")
    parser.add_argument("config_prod", help="Db config for INCA")
    args = parser.parse_args()
    main(args.config_dev, args.config_prod)
