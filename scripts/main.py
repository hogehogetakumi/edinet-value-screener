# -*- coding: utf-8 -*-
"""
EDINET XBRLデータ収集・分析メインスクリプト

指定されたターゲットリストに基づき、EDINETから最新の年次報告書(XBRL)を取得し、
PL/BSの主要項目を抽出してSQLiteデータベースに保存します。
"""
import os
import json
import pandas as pd
import argparse
import glob
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List
import sys

# プロジェクト内モジュール
import config
import database
import processor

def run_single_test(test_code: str, api_key: str, days_back: int):
    """
    単一のEDINETコードに対してテスト実行し、結果をコンソールに出力する
    """
    print(f"--- Running in Test Mode for EDINET Code: {test_code} ---")
    
    # --- 1. 企業マスタの読み込み ---
    if not os.path.exists(config.EDINET_CODE_LIST_PATH):
        print(f"!!! ERROR: EDINET code list not found at: {config.EDINET_CODE_LIST_PATH} !!!")
        return
    master_df = pd.read_csv(config.EDINET_CODE_LIST_PATH, encoding='cp932', skiprows=1, dtype=str).set_index('ＥＤＩＮＥＴコード')
    
    if test_code not in master_df.index:
        print(f"!!! ERROR: EDINET code '{test_code}' not found in master list. !!!")
        return

    # --- 2. 日次インデックスの構築 ---
    doc_index_df = processor.build_daily_index(api_key, days_back=days_back)
    if doc_index_df.empty:
        print("Could not build document index. Aborting.")
        return
        
    # --- 3. データ抽出の実行 ---
    print(f"Analyzing {test_code}...")
    try:
        data = processor.analyze_company_latest(test_code, api_key, master_df, doc_index_df)
        
        if data:
            print("\n--- ✅ Extracted Data ---")
            print(json.dumps(data, indent=2, ensure_ascii=False))
            print("--- ✅ End of Data ---")
        else:
            print(f"\n--- ❗️ No data could be extracted for {test_code}. ---")
            print("This could be because no recent annual reports were found.")

    except Exception as e:
        print(f"!!! An error occurred during analysis for {test_code}: {e} !!!")

def main():
    """メイン処理"""
    parser = argparse.ArgumentParser(description="Extract latest PL/BS from EDINET annual filings.")
    parser.add_argument("--num-files", type=int, default=1,
                        help="Number of target list files to process in one run.")
    parser.add_argument("--days-back", type=int, default=config.DAYS_BACK,
                        help=f"Number of days back to build the document index (default: {config.DAYS_BACK})")
    # --- Test Mode Argument ---
    parser.add_argument("--test-code", type=str, default=None,
                        help="Run in test mode for a single EDINET code. Bypasses DB writes.")
    parser.add_argument("--api-key", type=str, default=config.SUBSCRIPTION_KEY,
                        help="EDINET API key. Overrides environment variable if provided.")

    args = parser.parse_args()
    
    # --- APIキーのチェック ---
    api_key_to_use = args.api_key
    if not api_key_to_use or api_key_to_use == "YOUR_SUBSCRIPTION_KEY":
        print("!!! ERROR: EDINET_API_KEY is not set. Please provide it via --api-key argument or environment variable. !!!")
        return

    # --- Test Mode ---
    if args.test_code:
        run_single_test(args.test_code, api_key_to_use, args.days_back)
        return

    # --- Production (Batch) Mode ---
    print("--- Running in Production (Batch) Mode ---")

    # --- 1. 初期設定・バリデーション ---
    if not os.path.exists(config.EDINET_CODE_LIST_PATH):
        print(f"!!! ERROR: EDINET code list not found at: {config.EDINET_CODE_LIST_PATH} !!!")
        print("Please download 'EdinetcodeDlInfo.csv' from the EDINET website and place it in the project root.")
        return
        
    database.init_database()

    # --- 2. ターゲット企業の特定 ---
    all_target_files = sorted(glob.glob(os.path.join(config.TARGET_LISTS_DIR, 'target_list_*.json')))
    if not all_target_files:
        print(f"No target lists found in '{config.TARGET_LISTS_DIR}'.")
        return

    files_to_process = all_target_files[:args.num_files]
    print(f"Processing {len(files_to_process)} target file(s):")
    for f in files_to_process:
        print(f"  - {os.path.basename(f)}")

    # 企業マスタ読み込み
    master_df = pd.read_csv(config.EDINET_CODE_LIST_PATH, encoding='cp932', skiprows=1, dtype=str).set_index('ＥＤＩＮＥＴコード')
    
    # ターゲットリストからEDINETコードを読み込み
    target_codes: List[str] = []
    for file_path in files_to_process:
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                codes_in_file = json.load(f)
                valid_codes = [c for c in codes_in_file if c in master_df.index]
                target_codes.extend(valid_codes)
        except (json.JSONDecodeError, FileNotFoundError) as e:
            print(f"Could not process target file {os.path.basename(file_path)}: {e}")

    unique_target_codes = sorted(list(set(target_codes)))
    print(f"Found {len(unique_target_codes)} unique, valid companies to analyze.")
    if not unique_target_codes:
        return

    # --- 3. 日次インデックスの構築 ---
    doc_index_df = processor.build_daily_index(api_key_to_use, days_back=args.days_back)
    if doc_index_df.empty:
        print("Could not build document index. Aborting.")
        return

    # --- 4. 並列処理によるデータ抽出とDB保存 ---
    with ThreadPoolExecutor(max_workers=config.MAX_WORKERS) as executor:
        future_to_code = {
            executor.submit(processor.analyze_company_latest, code, api_key_to_use, master_df, doc_index_df): code
            for code in unique_target_codes
        }
        
        completed_count = 0
        for future in as_completed(future_to_code):
            code = future_to_code[future]
            try:
                data = future.result()
                if data:
                    database.upsert_record("companies", data["company"], ["edinet_code"])
                    
                    # 2. Filings
                    for f in data["filings"]:
                        database.upsert_record("filings", f, ["doc_id"])
                        
                    # 3. Financial Snapshots
                    for s in data["financial_snapshots"]:
                        database.upsert_record("financial_snapshots", s, ["doc_id"])
                        
                    # 4. Screening
                    database.upsert_record("company_screening", data["company_screening"], ["edinet_code"])
                    
                    print(f"  -> Successfully processed and saved data for {code}.")
                else:
                    print(f"  -> Skipped {code} (no new data or report).")
            except Exception as e:
                print(f"!!! Error processing {code}: {e} !!!")
            
            completed_count += 1
            if completed_count % 10 == 0:
                print(f"--- Progress: {completed_count}/{len(unique_target_codes)} companies analyzed ---")


    # --- 5. 処理済みファイルの移動 ---
    processed_dir = os.path.join(config.TARGET_LISTS_DIR, 'done')
    os.makedirs(processed_dir, exist_ok=True)
    for file_path in files_to_process:
        filename = os.path.basename(file_path)
        try:
            os.rename(file_path, os.path.join(processed_dir, filename))
            # print(f"Moved '{filename}' to '{processed_dir}'")
        except Exception as e:
            print(f"Error moving file '{filename}': {e}")
    print(f"Moved {len(files_to_process)} processed files to 'done' directory.")
    print("\nExtraction process finished.")

if __name__ == '__main__':
    main()
