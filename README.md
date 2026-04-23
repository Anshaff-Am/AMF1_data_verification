# AMF1 Partner Survey Dashboard — Data Verification

Phase 0 reference document builder for the AMF1 Partner Survey Dashboard verification project.

## Project structure

```
AMF1 Data Verification/
├── data/
│   ├── Wave1/          # Banner1-3.xlsx (Wave 1 source data)
│   └── Wave2/          # Banner1-3.xlsx (Wave 2 source data)
├── docs/               # Verification brief + PRD PDFs
├── outputs/
│   ├── reference_document.csv   # Ground truth — 166 MB, gitignored
│   └── ambiguities_report.txt   # Data model ambiguities for PM review
└── scripts/
    └── build_reference_doc.py   # Parses banner xlsx → reference_document.csv
```

## Setup

```bash
pip install -r requirements.txt
```

## Usage

```bash
python scripts/build_reference_doc.py
```

Reads `data/Wave1/Banner1-3.xlsx` and `data/Wave2/Banner1-3.xlsx`.  
Outputs `outputs/reference_document.csv` (~850k rows) and `outputs/ambiguities_report.txt`.

## Adding a new wave (Wave 3+)

1. Drop the three banner xlsx files into `data/Wave3/`
2. Add a `"W3"` entry to the `FILES` dict in `scripts/build_reference_doc.py`
3. Re-run the script

> The parser uses dynamic header detection and handles both 1-line and 2-line question text blocks, tolerant of minor structural variations. If the banner file layout changes significantly, review `parse_t1_sheet()`.

## Data model

Each row in `reference_document.csv`:

| Column | Description |
|--------|-------------|
| `wave` | W1 or W2 |
| `banner` | Banner1 / Banner2 / Banner3 |
| `table_num` | Table number within the banner file |
| `question_code` | e.g. D1, ARC2, TTK5 |
| `question_text` | Full question text |
| `response_label` | Response option label |
| `filter_group` | Filter group header (e.g. Country, Gender) |
| `filter_option` | Filter value (e.g. Australia, Men) |
| `col_letter` | Source column letter in the xlsx sheet |
| `base_n` | Base N for that filter slice |
| `count` | Raw count |
| `value` | Percentage (0-100) or score |
| `value_type` | `percentage` or `score_or_mean` |
