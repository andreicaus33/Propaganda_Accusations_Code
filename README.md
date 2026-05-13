# Propaganda_Accusations_Code

Replication materials for my BSc thesis on the use of "propaganda" as a political accusation in Romanian Senate and Chamber of Deputies debates.

## Contents

- `scraper.py`, `monitorul_scraper.py`, `extractor.py`, `downloader.py`, `export.py`, `main.py` — scraping pipeline that collects PDF transcripts from senat.ro and monitoruloficial.ro and extracts 41-word snippets around the keyword "propagandă"
- `coding.py` — rule-based coding of snippets across 5 binary categories
- `analysis.py` — descriptive stats, logistic regressions, OLS on the weaponization score
- `figures.py` — generates the figures used in the thesis
- `propaganda_contexts_min.json` — extracted snippet dataset
- `FINAL_CODING_RESULTS.xlsx` — final coded dataset (2,673 unique snippets after deduplication)
- `codebook.md` — coding frame: 5 categories with rules and examples
- `figures/`, `tables/` — outputs



## Data

Source: parliamentary transcripts publicly available at https://monitoruloficial.ro for both Senate and Deputies, or separetely on https://www.senat.ro/ and https://www.cdep.ro/.
Period: 1 Jan 2000 – 31 Dec 2024.
Selection: snippets of 41 words centered on the keyword "propagandă" / "propaganda".
N after deduplication: 2,673 snippets.

## Coding

Each snippet receives 5 binary codes:

- delegitimization
- polarization
- scapegoating
- conspiracy
- anti-media

Full coding rules in `codebook.md`. Inter-coder reliability against a 10% human-coded subsample (Cohen's κ): delegitimization 0.82, anti-media 0.87, conspiracy 0.74, polarization 0.55. Scapegoating κ is degenerate due to near-zero prevalence — see thesis section 4.4.

## Citation

Caus, A. (2025). *Propaganda accusations in Romanian parliamentary discourse, 2000–2024* [Bachelor's thesis]. Leiden University.



