import pandas as pd
import os
from medspacy.context import ConTextRule, ConText
import medspacy
import tqdm

import util
from data_read import collect_data

CONTEXT_ATTRS = {
'NEG': {'is_negated': True},
'POSS': {'is_possible': True},
'HYPO': {'is_hypothetical': True},
'HIST': {'is_historical': True},
'EXP': {'is_experiencer': True},
'HISTEXP': {'hist_experienced': True},
'HYPOEXP': {'hypo_experienced': True}
}

def main():
    args = util.parse_args()
    util.set_extensions(CONTEXT_ATTRS)
    nlp = medspacy.load()
    nlp.remove_pipe("medspacy_context")
    context_rules_list = util.get_context_rules(args.context_file)
    context = nlp.add_pipe("medspacy_context", config={"rules": None, "span_attrs":CONTEXT_ATTRS})
    context.add(context_rules_list)

    rule_files = [os.path.join(args.ruleset_dir, file) for file in os.listdir(args.ruleset_dir)]
    target_matcher = nlp.get_pipe("medspacy_target_matcher")

    for file in rule_files:
        target_matcher.add(util.compile_target_rules(file))

    data_to_collate = collect_data(nlp)

    df = pd.DataFrame.from_dict(data_to_collate)
    df.to_csv('medspacy_results_sample.csv', index=False)

if __name__ == "__main__":
    main()
