import pandas as pd
import tqdm
import argparse
from medspacy.context import ConText, ConTextRule
from medspacy.ner import TargetRule

def parse_args() -> argparse.Namespace:
    """Parses the command line arguments."""

    parser = argparse.ArgumentParser("MedSpaCy annotation of Clinical Notes")

    parser.add_argument("--ruleset_dir", type=str, help="path to target match rules",
                        required=True)
    parser.add_argument("--context_file", type=str, help="path to context rules", required=False,
                        default = None)
#    parser.add_argument("--data_source", type=str, help="Note source", required=True, choices =
#                        ['filesystem', 'database'])

    parser.add_argument("--input_files", type=str, help="Note source", required=True, default = None)

    return parser.parse_args()


def get_context_rules(CONTEXT_FILE):
    context_rules_list = []
    df = pd.read_csv(CONTEXT_FILE)
    for _, row in tqdm.tqdm(df.iterrows(), total=len(df), desc="Getting ConTextRule"):
        context_rules_list.append(ConTextRule(literal=row['literal'], direction=row['direction'],
                                          category=row['category']))
    return context_rules_list

def compile_target_rules(rule_path):
    with open(rule_path, 'r') as f:
        rules = f.readlines()
    target_rules = []
    for line in rules:
        rule = line.strip()
        rulename = rule_path.split('_re')[-1].split('.txt')[0]
        target_rules.append(TargetRule(f"{rulename}", "PROBLEM", pattern=rf"{rule}"))
    return target_rules

def set_extensions(CONTEXT_ATTRS):
    from spacy.tokens import Doc, Span, Token
    Token.set_extension("concept_tag", default="", force=True)
    for (_, attr_dict) in CONTEXT_ATTRS.items():
        for (attr_name, attr_value) in attr_dict.items():
            Span.set_extension(attr_name, default=False, force=True)
    Doc.set_extension("document_classification", default=None, force=True)
    Span.set_extension("is_template", default=False, force=True)
    Span.set_extension("is_classifier", default=False, force=True)
    Span.set_extension("literal", getter=get_literal, force=True)
    Span.set_extension("is_asserted", getter= lambda x: is_asserted(x), force=True)
    Span.set_extension("is_possible", default=False, force=True)

def get_literal(span):
    rule = span._.target_rule
    if rule is None:
        return
    literal = rule.literal
    if literal.lower() == span.text.lower():
        return
    return literal
