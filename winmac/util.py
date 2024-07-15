import argparse
import os
import json
import tqdm
import medspacy

import pandas as pd

from medspacy.context import ConTextRule
from medspacy.ner import TargetRule

from spacy.tokens import Doc, Span, Token


def parse_args() -> argparse.Namespace:
    """Parses the command line arguments."""

    parser = argparse.ArgumentParser("MedSpaCy annotation of Clinical Notes")

    parser.add_argument(
        "--context_file",
        type=str,
        help="path to context rules",
        required=False,
        default="context_rules_v1.txt",
    )
    parser.add_argument(
        "--db_conf",
        type=str,
        help="path to database config .json file",
        required=False,
        default=None,
    )
    parser.add_argument(
        "--file_path",
        type=str,
        help="Path to input .csv, .zip, or folder",
        required=False,
        default=None,
    )

    return parser.parse_args()


def get_context_rules(CONTEXT_FILE):
    context_rules_list = []
    df = pd.read_csv(CONTEXT_FILE)
    for _, row in tqdm.tqdm(df.iterrows(), total=len(df), desc="Getting ConTextRule"):
        context_rules_list.append(
            ConTextRule(
                literal=row["literal"], direction=row["direction"], category=row["category"]
            )
        )
    return context_rules_list


def quiet_get_context_rules(CONTEXT_FILE):
    context_rules_list = []
    df = pd.read_csv(CONTEXT_FILE)
    for _, row in df.iterrows():
        context_rules_list.append(
            ConTextRule(
                literal=row["literal"], direction=row["direction"], category=row["category"]
            )
        )
    return context_rules_list


def quiet_compile_target_rules(rule_path):
    with open(rule_path, "r") as f:
        rules = f.readlines()
    target_rules = []
    for line in rules:
        rule = line.strip()
        if "_re" in rule_path:
            rulename = rule_path.split("_re")[-1].split(".txt")[0]
        elif "/" in rule_path:
            rulename = rule_path.split("/")[-1].split(".txt")[0]
        target_rules.append(TargetRule(f"{rulename}", "PROBLEM", pattern=rf"{rule}"))
    return target_rules


def compile_target_rules(rule_path):
    print(f"Getting matcher rules from {rule_path}")
    with open(rule_path, "r") as f:
        rules = f.readlines()
    target_rules = []
    for line in rules:
        rule = line.strip()
        if "_re" in rule_path:
            rulename = rule_path.split("_re")[-1].split(".txt")[0]
        elif "/" in rule_path:
            rulename = rule_path.split("/")[-1].split(".txt")[0]
        target_rules.append(TargetRule(f"{rulename}", "PROBLEM", pattern=rf"{rule}"))
    return target_rules


def set_extensions(CONTEXT_ATTRS):
    Token.set_extension("concept_tag", default="", force=True)
    for _, attr_dict in CONTEXT_ATTRS.items():
        for attr_name, _ in attr_dict.items():
            Span.set_extension(attr_name, default=False, force=True)
    Doc.set_extension("document_classification", default=None, force=True)
    Span.set_extension("is_template", default=False, force=True)
    Span.set_extension("is_classifier", default=False, force=True)
    Span.set_extension("literal", getter=get_literal, force=True)
    Span.set_extension("is_possible", default=False, force=True)
    Span.set_extension("is_patient", default=True, force=True)
    Span.set_extension("is_present", default=True, force=True)
    Span.set_extension("is_positive", default=True, force=True)


def get_literal(span):
    rule = span._.target_rule
    if rule is None:
        return
    literal = rule.literal
    if literal.lower() == span.text.lower():
        return
    return literal


def get_versioning(software, config):
    with open(software, "r") as f:
        mt_versions = json.load(f)
    with open(config, "r") as f:
        conf = json.load(f)
    ruleset_dir = conf["ruleset_dir"]
    if "VERSION.json" in os.listdir(ruleset_dir):
        with open(os.path.join(ruleset_dir, "VERSION.json"), "r") as f:
            rule_version = json.load(f)
    else:
        rule_version = {"Version": "Not given"}
    pyohnlp_version = mt_versions["pyOHNLP Toolkit"]
    context_version = mt_versions["ConText"]
    ruleset_version = list(rule_version.items())[0]
    versioning = f"MediTag:{pyohnlp_version}|ConText:{context_version}|Ruleset:{ruleset_version}"
    return versioning


def fixit():
    args = parse_args()
    with open(args.db_conf, "r") as f:
        conf = json.load(f)
    nlp = medspacy.load()
    nlp.remove_pipe("medspacy_context")
    CONTEXT_ATTRS = {
        "NEG": {"is_negated": True},
        "POSS": {"is_possible": True},
        "HYPO": {"is_hypothetical": True},
        "HIST": {"is_historical": True},
        "EXP_FAMILY": {"fam_experiencer": True},
        "HISTEXP": {"hist_experienced": True},
        "HYPOEXP": {"hypo_experienced": True},
        "DOSE": {"dose_exp": True},
    }
    set_extensions(CONTEXT_ATTRS)
    context_rules_list = get_context_rules(args.context_file)
    context = nlp.add_pipe("medspacy_context", config={"rules": None, "span_attrs": CONTEXT_ATTRS})
    context.add(context_rules_list)

    rule_files = [
        os.path.join(conf["ruleset_dir"], file) for file in os.listdir(conf["ruleset_dir"])
    ]
    target_matcher = nlp.get_pipe("medspacy_target_matcher")
    for file in rule_files:
        target_matcher.add(compile_target_rules(file))


def quiet_fix():
    args = parse_args()
    with open(args.db_conf, "r") as f:
        conf = json.load(f)
    nlp = medspacy.load()
    nlp.remove_pipe("medspacy_context")
    CONTEXT_ATTRS = {
        "NEG": {"is_negated": True},
        "POSS": {"is_possible": True},
        "HYPO": {"is_hypothetical": True},
        "HIST": {"is_historical": True},
        "EXP_FAMILY": {"fam_experiencer": True},
        "HISTEXP": {"hist_experienced": True},
        "HYPOEXP": {"hypo_experienced": True},
        "DOSE": {"dose_exp": True},
    }
    set_extensions(CONTEXT_ATTRS)
    context_rules_list = quiet_get_context_rules(args.context_file)
    context = nlp.add_pipe("medspacy_context", config={"rules": None, "span_attrs": CONTEXT_ATTRS})
    context.add(context_rules_list)

    rule_files = [
        os.path.join(conf["ruleset_dir"], file) for file in os.listdir(conf["ruleset_dir"])
    ]
    target_matcher = nlp.get_pipe("medspacy_target_matcher")
    for file in rule_files:
        target_matcher.add(quiet_compile_target_rules(file))
