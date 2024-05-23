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
    with open(f'/home/jordan/Downloads/rows/{file_source}', 'r') as f:
        txt = f.read()

    doc = nlp(txt)

    for ent in doc.ents:
        print(ent)
        print('==========')
        if ent._.is_negated == True:
            print("Is Negated")
        if ent._.is_possible == True:
            print("Is Possible")
        if ent._.is_hypothetical == True:
            print("Is hypothetical.")
        if ent._.is_historical == True:
            print("is historical")
        if ent._.is_experiencer == True:
            print("is experiencer")
        if ent._.hist_experienced == True:
            print("Hist exp")
        if ent._.hypo_experienced == True:
            print("hypo exp")
        print('===========')
        print(ent._.modifiers)
        print(ent._.target_rule)
    data_to_collate = {
'ent' : [], 'negated': [], 'possible' : [], 'hypothetical': [], 'historical': [], 'is exp': [], 'hist exp': [], 'hypo exp': [], 'source': [], 'rule': []}

    for ent in doc.ents:
        data_to_collate['ent'].append(ent),
        data_to_collate['negated'].append(ent._.is_negated),
        data_to_collate['possible'].append(ent._.is_possible),
        data_to_collate['hypothetical'].append(ent._.is_hypothetical),
        data_to_collate['historical'].append(ent._.is_historical),
        data_to_collate['is exp'].append(ent._.is_experiencer),
        data_to_collate['hist exp'].append(ent._.hist_experienced),
        data_to_collate['hypo exp'].append(ent._.hypo_experienced),
        data_to_collate['source'].append(file_source),
        data_to_collate['rule'].append(ent._.literal)

    df = pd.DataFrame.from_dict(data_to_collate)
    df.to_csv('medspacy_results_sample.csv', index=False)

if __name__ == "__main__":
    main()
