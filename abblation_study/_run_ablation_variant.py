import argparse
import os

parser = argparse.ArgumentParser()
parser.add_argument("--variant", required=True)
parser.add_argument("--dataset", required=True)
parser.add_argument("--db-path", required=True)
parser.add_argument("--log", required=True)
parser.add_argument("--metrics", required=True)
parser.add_argument("--delay", type=float, default=0.5)
args = parser.parse_args()

# Keep ablation DB stable regardless of .env defaults.
os.environ["SPTS_DATABASE_URL"] = ""
os.environ["SPTS_MAIN_DB_PATH"] = args.db_path

if args.variant == "no_reflection":
    os.environ["SPTS_SQL_REFLECTION_ENABLED"] = "false"
else:
    os.environ["SPTS_SQL_REFLECTION_ENABLED"] = "true"

os.environ["SPTS_SQL_REFLECTION_SCOPE"] = "spts"

import evaluate

if args.variant == "baseline":

    def _no_vlkg(query: str):
        return query, []

    def _baseline_as_spts(query: str, mappings=None):
        return evaluate.baseline_text_to_sql(query)

    evaluate.ground_query = _no_vlkg
    evaluate.spts_text_to_sql = _baseline_as_spts
elif args.variant == "no_vlkg":

    def _no_vlkg(query: str):
        return query, []

    evaluate.ground_query = _no_vlkg
elif args.variant == "no_synonyms":
    _orig = evaluate.ground_query

    def _exact_only(query: str):
        q, mappings = _orig(query)
        keep = []
        for mapping in mappings or []:
            mapping_type = str(mapping.get("type", "")).lower()
            if "exact" in mapping_type:
                keep.append(mapping)
        return q, keep

    evaluate.ground_query = _exact_only

print(f"Running variant={args.variant}")
evaluate.run_evaluation(
    test_data_path=args.dataset,
    output_log_path=args.log,
    final_metrics_path=args.metrics,
    db_path=args.db_path,
    db_url="",
    delay_seconds=args.delay,
)
print(f"Finished variant={args.variant}")
