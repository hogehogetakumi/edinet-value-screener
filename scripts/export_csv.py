# -*- coding: utf-8 -*-
"""
SQLiteの分析データをCSVに書き出すスクリプト
"""
import argparse
import os

import config
import database


def main() -> None:
    parser = argparse.ArgumentParser(description="Export latest financial snapshot CSV.")
    parser.add_argument(
        "--output",
        default=os.path.join(config.OUTPUT_DIR, "company_financials.csv"),
        help="Output CSV path.",
    )
    args = parser.parse_args()

    os.makedirs(os.path.dirname(args.output), exist_ok=True)
    database.export_company_financials_csv(args.output)


if __name__ == "__main__":
    main()
