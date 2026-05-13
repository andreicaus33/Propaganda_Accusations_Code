# Propaganda_Accusations_Code

Replication materials for my MA thesis on the use of "propaganda" as a political accusation in Romanian Senate and Chamber of Deputies debates.

## Contents

- `scraper.py` — scrapes Senate stenograms from senat.ro
- `monitorul_scraper.py` — scrapes Monitorul Oficial PII (Chamber of Deputies sessions)
- `dualcoder_coding.py` — rule-based coding of snippets across 5 binary categories
- `analysis.py` — descriptive stats, logistic regressions, OLS on the weaponization score
- `generate_thesis_figures.py` — generates the figures used in the thesis
- `propaganda_contexts_min.json` — extracted snippet dataset (raw scraped output)
- `FINAL_CODING_RESULTS.xlsx` — final coded dataset (2498 unique snippets after deduplication and restriction to 2024)

## Running

```
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
python analysis.py
python generate_thesis_figures.py
```

The scrapers are included for transparency. They do not need to be re-run since the extracted snippets are already in `propaganda_contexts_min.json`.

## Data

Source: parliamentary transcripts publicly available at https://monitoruloficial.ro, or separately at https://www.senat.ro/ and https://monitoruloficial.ro/.
Period: 1 Jan 2000 – 31 Dec 2024.
Selection: snippets of 41 words centered on the keyword "propagandă".
N after deduplication: 2,673 snippets.

## Coding

Each snippet receives 5 binary codes:

- delegitimization
- polarization
- scapegoating
- conspiracy
- anti-media

Full coding rules in the thesis appendix. Inter-coder reliability against a 10% human-coded subsample (Cohen's κ): delegitimization 0.82, anti-media 0.87, conspiracy 0.74, polarization 0.55. Scapegoating κ is degenerate due to near-zero prevalence — see thesis discussion.

## Citation

Caus, A. (2025). *Propaganda accusations in Romanian parliamentary discourse, 2000–2024* [Master's thesis]. Leiden University.

