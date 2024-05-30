import pandas as pd
import os
import json
import medspacy

import util
from data_read import collect_data

CONTEXT_ATTRS = {
    "NEG": {"is_negated": True},
    "POSS": {"is_possible": True},
    "HYPO": {"is_hypothetical": True},
    "HIST": {"is_historical": True},
    "EXP": {"is_experiencer": True},
    "HISTEXP": {"hist_experienced": True},
    "HYPOEXP": {"hypo_experienced": True},
}

def main():
    args = util.parse_args()
    util.set_extensions(CONTEXT_ATTRS)
    nlp = medspacy.load()
    nlp.remove_pipe("medspacy_context")
    context_rules_list = util.get_context_rules(args.context_file)
    context = nlp.add_pipe("medspacy_context", config={"rules": None, "span_attrs": CONTEXT_ATTRS})
    context.add(context_rules_list)

    rule_files = [os.path.join(args.ruleset_dir, file) for file in os.listdir(args.ruleset_dir)]
    target_matcher = nlp.get_pipe("medspacy_target_matcher")

    for file in rule_files:
        target_matcher.add(util.compile_target_rules(file))

    data_to_collate = collect_data(nlp)

    if args.db_conf is None:
        df = pd.DataFrame.from_dict(data_to_collate)
        df.to_csv("medspacy_results_sample.csv", index=False)
    else:
        with open(args.db_conf, 'r') as f:
            config = json.load(f)
        if config['write_to']['to_csv'] == "True":
            df = pd.DataFrame.from_dict(data_to_collate)
            df.to_csv("medspacy_results_sample.csv", index=False)
        else:
            raise ValueError("I haven't implemented this yet!!")


if __name__ == "__main__":
    main()
