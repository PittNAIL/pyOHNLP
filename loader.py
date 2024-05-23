import pandas as pd
import os
from medspacy.context import ConTextRule, ConText
import medspacy
import tqdm

import util

CONTEXT_ATTRS = {
'NEG': {'is_negated': True},
'POSS': {'is_possible': True},
'HYPO': {'is_hypothetical': True},
'HIST': {'is_historical': True},
'EXP': {'is_experiencer': True},
'HISTEXP': {'hist_experienced': True},
'HYPOEXP': {'hypo_experienced': True}
}

data_to_collate = {
'ent' : [], 'negated': [], 'possible' : [], 'hypothetical': [], 'historical': [], 'is exp': [], 'hist exp': [], 'hypo exp': [], 'source': [], 'rule': []}

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

    file_source = 'note_93.txt'
    if os.path.isdir(args.input_files):
        for file in tqdm.tqdm(os.listdir(args.input_files)):
            with open(os.path.join(args.input_files, file), 'r') as f:
                txt = f.read()

            doc = nlp(txt)
    #with open(f'/home/jordan/Downloads/rows/{file_source}', 'r') as f:
    #    txt = f.read()

   # doc = nlp(txt)

            for ent in doc.ents:
                data_to_collate['ent'].append(ent),
                data_to_collate['negated'].append(ent._.is_negated),
                data_to_collate['possible'].append(ent._.is_possible),
                data_to_collate['hypothetical'].append(ent._.is_hypothetical),
                data_to_collate['historical'].append(ent._.is_historical),
                data_to_collate['is exp'].append(ent._.is_experiencer),
                data_to_collate['hist exp'].append(ent._.hist_experienced),
                data_to_collate['hypo exp'].append(ent._.hypo_experienced),
                data_to_collate['source'].append(file),
                data_to_collate['rule'].append(ent._.literal)

    df = pd.DataFrame.from_dict(data_to_collate)
    df.to_csv('medspacy_results_sample.csv', index=False)

if __name__ == "__main__":
    main()
