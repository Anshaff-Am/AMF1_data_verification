#!/usr/bin/env python3
"""
AMF1 Partner Survey - Phase 0 Ambiguities Report Builder

Reads outputs/reference_document.csv and produces outputs/ambiguities_report.txt.
Run after build_reference_doc.py.

Usage: python scripts/build_ambiguity_report.py
"""

import csv
import sys
import io
from pathlib import Path
from collections import defaultdict

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

BASE_DIR     = Path(__file__).parent.parent
INPUT_CSV    = BASE_DIR / "outputs" / "reference_document.csv"
AMBIG_REPORT = BASE_DIR / "outputs" / "ambiguities_report.txt"

IN_SCOPE_PREFIXES = {
    "D1", "D2", "D3", "D4", "D5", "D6",
    "D7", "D8", "D9",
    "D14", "D15", "D16",
    "B1", "B2", "B2A", "B3", "B5", "B7",
    "C5", "C6",
    "ARC1", "ARC2", "ARC5", "ARC6", "ARC7", "ARC8",
    "CON1", "CON2", "CON3", "CON4", "CON5", "CON6", "CON7", "CON8",
    "COR1",
    "MAA1", "MAA2", "MAA3", "MAA4", "MAA5", "MAA6", "MAA7", "MAA8",
    "VAL1",
    "PEP1", "PEP2",
    "NEX1",
    "GLN1",
    "SER1",
    "ATL1", "ATL2",
    "COG1", "COG2", "COG3",
    "TTK1", "TTK2", "TTK3", "TTK4", "TTK5", "TTK6", "TTK7",
}

KNOWN_AMBIGUITIES = [
    "KNOWN DATA MODEL AMBIGUITIES — NEEDS SURAEN REVIEW:",
    "-" * 50,
    "",
    "1. D6 DUAL TABLE",
    "   Wave 1 has Table 200 (D6 Total Sample) AND Table 201 (D6 D6-aware base).",
    "   Dashboard base for D6 is listed as 'All aware of >1 brand at D1'.",
    "   QUESTION: Which table does the dashboard use as its source?",
    "   Does it filter to respondents aware of at least one D1 brand?",
    "",
    "2. D14/D15/D16 BRANDS IN TABLES vs DASHBOARD PARTNER PAGES",
    "   Global tables include D14/D15/D16 for brands NOT on the 14 partner pages",
    "   (e.g. Citi, Regent Seven Seas Cruises, NetApp, Xerox, ARM, Elemis, Public,",
    "   The Financial Times). Dashboard only has 14 partner pages.",
    "   QUESTION: Are these extra brands displayed anywhere in the dashboard?",
    "   Or are they excluded from the dashboard entirely?",
    "",
    "3. D1 AWARENESS — 31 TRACKED BRANDS vs 18 PARTNER BRANDS",
    "   The D1 question tracks 31+ brands (including non-partner brands like Pirelli,",
    "   Puma, Bombardier, Oakley, Stilo, etc.). Dashboard Overview shows all brands.",
    "   PRD says 'D1 Brand Awareness (SUM of D1_1r + D1_2r per brand)'.",
    "   QUESTION: The global tables show D1 as a single question (spontaneous +",
    "   prompted combined). Is there a D1_1r / D1_2r split in the raw data that",
    "   doesn't appear in these pre-aggregated banner tables?",
    "",
    "4. W1 PARTNER-SPECIFIC QUESTIONS AVAILABILITY",
    "   Wave 1 banner files (tables 1-855) do NOT appear to contain:",
    "   B2a, B5, B7, MAA1-8, ATL1-2, TTK1-7.",
    "   These are found in Wave 2 only (or at very different table numbers in W1).",
    "   QUESTION: Were these questions added in Wave 2? How should the dashboard",
    "   display these for Wave 1? Show N/A, or are they present in W1 elsewhere?",
    "",
    "5. FILTER MAPPING: F1 FAN 'AVID' OPTION",
    "   PRD defines F1 Fan filter as: Yes (A3 opt 4/5), Avid (A3 opt 5 only),",
    "   Non-fans (A3 opt 1/2/3).",
    "   Banner 2 has 'F1 Fans' (=Yes) and 'Non-F1 Fans' but no explicit 'Avid' column.",
    "   W2 Banner2 has 'F1 2025 Followers' which may correspond to 'Avid'.",
    "   QUESTION: Does 'F1 2025 Followers' = 'Avid F1 Fan' filter in the dashboard?",
    "",
    "6. TEAM FAN FILTER",
    "   PRD lists Team Fan as a multi-select filter with 10 F1 teams.",
    "   No dedicated per-team fan columns found in the banner tables.",
    "   QUESTION: How is the Team Fan filter implemented? Are team-fan segments",
    "   built from C2 (team support) question responses in the raw SPSS data?",
    "   If so, these values won't be in the pre-aggregated banner tables.",
    "",
    "7. DEMOGRAPHIC FILTERS: MEN UNDER 35, WOMEN 35-54, ETC.",
    "   PRD lists detailed demographic sub-segments (Men Under 35, Men 35-54, Men 55+,",
    "   Women Under 35, Women 35-54, Women 55+).",
    "   Not clearly mapped to specific banner columns.",
    "   QUESTION: Which specific banner columns correspond to these sub-segments?",
    "",
    "8. TECH ADOPTERS (D17a OPT 1) — WAVE 2 ONLY",
    "   PRD lists Tech Adopters as a Demographic filter option.",
    "   Banner 1 Wave 2 has this as a 'Students > F1 Fans' subsegment perhaps.",
    "   QUESTION: Is Tech Adopters a Wave 2 only filter? Should it be hidden in W1?",
    "",
    "9. CON7 EXCHANGE PREFERENCE",
    "   PRD shows CON7 as 'Which cryptocurrency exchange are you most likely to use?'",
    "   as a horizontal bar chart. Table 479 in W1 only shows Coinbase as a brand.",
    "   QUESTION: Does CON7 show all exchanges (like CON6) or just Coinbase brand row?",
    "",
    "10. D3a vs D3b SPLIT",
    "    PRD shows D3a (Currently own/use) and D3b (Consider using) as separate charts.",
    "    In global tables, these are rows within a single D3 table per brand.",
    "    Both rows ('Currently use/own' and 'Consider using/owning') are captured",
    "    in the reference document — verify exact row label wording matches dashboard.",
    "",
]


def main():
    if not INPUT_CSV.exists():
        print(f"ERROR: {INPUT_CSV} not found. Run build_reference_doc.py first.", file=sys.stderr)
        sys.exit(1)

    print(f"Reading {INPUT_CSV} ...", file=sys.stderr)
    question_coverage = defaultdict(set)
    total = 0
    with open(str(INPUT_CSV), encoding="utf-8") as f:
        for row in csv.DictReader(f):
            qcode = row["question_code"]
            if qcode:
                question_coverage[qcode].add((row["wave"], row["banner"]))
            total += 1
    print(f"  {total:,} records", file=sys.stderr)

    found_codes = set(question_coverage.keys())
    missing_qs  = sorted(IN_SCOPE_PREFIXES - found_codes)
    w1_only = {c for c, waves in question_coverage.items() if all(w == "W1" for w, _ in waves)}
    w2_only = {c for c, waves in question_coverage.items() if all(w == "W2" for w, _ in waves)}

    lines = [
        "AMF1 Partner Survey Dashboard — Phase 0 Ambiguities Report",
        "=" * 65,
        f"Generated from: {total:,} records across 6 banner files",
        "",
    ]

    if missing_qs:
        lines += [
            "IN-SCOPE QUESTIONS WITH NO DATA FOUND:",
            "(May not exist in Wave 1, or use different question codes)",
            *[f"  {q}" for q in missing_qs],
            "",
        ]

    if w1_only:
        lines += [
            "WAVE 1 ONLY QUESTIONS (not found in Wave 2 data):",
            *[f"  {q}" for q in sorted(w1_only)],
            "",
        ]
    if w2_only:
        lines += [
            "WAVE 2 ONLY QUESTIONS (not found in Wave 1 data):",
            "(Dashboard should show N/A for these in Wave 1 context)",
            *[f"  {q}" for q in sorted(w2_only)],
            "",
        ]

    lines += KNOWN_AMBIGUITIES

    with open(str(AMBIG_REPORT), "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

    print(f"Ambiguities report: {AMBIG_REPORT}", file=sys.stderr)


if __name__ == "__main__":
    main()
