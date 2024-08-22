import json
import medspacy
import util
import platform

import winmac.data_read as wmdr
import pandas as pd

from data_read import collect_data
from enact_module import enact_transform
from write_to_db import dbwriter
from util import conv_time

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

platform = platform.platform()


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

    target_matcher = nlp.get_pipe("medspacy_target_matcher")

    target_matcher.add(util.compile_target_rules(conf["ruleset_dir"]))

    if "linux" in platform.lower():
        data_to_collate = collect_data(nlp)
    else:
        data_to_collate = wmdr.collect_data(nlp)

    df = pd.DataFrame.from_dict(data_to_collate)
    if conf["enact"] == "True":
        df = enact_transform(df)
    timestamp = conv_time()
    if conf["write_to"]["to_csv"] == "True":
        df.to_csv(f"./pyOHNLP_output_{timestamp}.csv", index=False)
    if conf["write_to"]["to_table"] != "None":
        dbwriter(df, args.db_conf)


if __name__ == "__main__":
    main()
