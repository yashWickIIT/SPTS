# Ablation Results Table and Discussion (Non-Hardcoded SPTS)

## Setup

- Dataset: abblation_study/ablation_dataset_8.json
- Fairness: baseline and SPTS use the same LLM model (llama-3.3-70b-versatile)
- SPTS implementation: non-hardcoded (no rule-template SQL path)
- Variants: baseline, -VLKG, -reflection, -synonyms, full SPTS

## Results Table

| Variant | Baseline EXE | Baseline ETM | Baseline F1 | SPTS EXE | SPTS ETM | SPTS F1 | Delta EXE | Delta ETM | Delta F1 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| baseline | 12.50 | 0.00 | 73.62 | 12.50 | 0.00 | 73.62 | 0.00 | 0.00 | 0.00 |
| -VLKG | 12.50 | 0.00 | 73.31 | 12.50 | 0.00 | 73.31 | 0.00 | 0.00 | 0.00 |
| -reflection | 12.50 | 0.00 | 73.31 | 25.00 | 0.00 | 76.78 | 12.50 | 0.00 | 3.47 |
| -synonyms | 12.50 | 0.00 | 73.31 | 25.00 | 0.00 | 77.10 | 12.50 | 0.00 | 3.79 |
| full SPTS | 12.50 | 0.00 | 73.31 | 25.00 | 0.00 | 79.28 | 12.50 | 0.00 | 5.97 |

## Key Finding

- In this regenerated non-hardcoded evaluation, full SPTS is better than baseline.
- Full SPTS gains: EXE +12.50, ETM +0.00, F1 +5.97.

## Discussion

- This report uses only regenerated non-hardcoded ablation outputs.
- The non-hardcoded SPTS pipeline improves execution accuracy and structural F1 over baseline in the full setting.
- The largest gain is in full SPTS (F1 +5.97), with additional improvements in -synonyms and -reflection variants.
- ETM exact remains unchanged on this dataset, so the improvement claim is supported primarily by EXE and F1.

## Evidence Files (Non-Hardcoded)

- abblation_study/ablation_log_baseline_nonhardcoded.json
- abblation_study/ablation_metrics_baseline_nonhardcoded.json
- abblation_study/ablation_log_no_vlkg_nonhardcoded.json
- abblation_study/ablation_metrics_no_vlkg_nonhardcoded.json
- abblation_study/ablation_log_no_reflection_nonhardcoded.json
- abblation_study/ablation_metrics_no_reflection_nonhardcoded.json
- abblation_study/ablation_log_no_synonyms_nonhardcoded.json
- abblation_study/ablation_metrics_no_synonyms_nonhardcoded.json
- abblation_study/ablation_log_full_spts_nonhardcoded.json
- abblation_study/ablation_metrics_full_spts_nonhardcoded.json
