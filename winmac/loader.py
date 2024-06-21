import json
import medspacy
import os
import util

import pandas as pd

from data_read import collect_data
from dbwriter import write_to_db
from enact_module import enact_transform

CONTEXT_ATTRS = {
    "NEG": {"is_negated": True},
    "POSS": {"is_possible": True},
    "HYPO": {"is_hypothetical": True},
    "HIST": {"is_historical": True},
    "EXP_FAMILY": {"fam_experiencer": True},
    "HISTEXP": {"hist_experienced": True},
    "HYPOEXP": {"hypo_experienced": True},
    "DOSE": {"dose_dec": True},
}


def main():
    args = util.parse_args()
    with open(args.db_conf, "r") as f:
        conf = json.load(f)
    nlp = medspacy.load()
    nlp.remove_pipe("medspacy_context")
    util.set_extensions(CONTEXT_ATTRS)

    context_rules_list = util.get_context_rules(args.context_file)
    context = nlp.add_pipe("medspacy_context", config={"rules": None, "span_attrs": CONTEXT_ATTRS})
    context.add(context_rules_list)

    rule_files = [
        os.path.join(conf["ruleset_dir"], file) for file in os.listdir(conf["ruleset_dir"])
    ]
    target_matcher = nlp.get_pipe("medspacy_target_matcher")

    for file in rule_files:
        if file.endswith(".txt"):
            target_matcher.add(util.compile_target_rules(file))

    data_to_collate = collect_data(nlp)

    df = pd.DataFrame.from_dict(data_to_collate)
    if conf["enact"] == "True":
        df = enact_transform(df)
    if conf["write_to"]["to_csv"] == "True":
        df.to_csv("medspacy_results_sample.csv", index=False)
    if conf["write_to"]["to_table"] != "None":
        write_to_db(df, args.db_conf)


if __name__ == "__main__":
    main()
