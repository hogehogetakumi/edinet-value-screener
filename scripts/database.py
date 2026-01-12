# -*- coding: utf-8 -*-
"""
データベース関連の関数群
"""
import sqlite3
import os
from typing import Dict, Any, List

import config

def init_database():
    """
    既存テーブルが無ければ作成。既存があればそのまま（DROPしない）。
    新スキーマ（4テーブル）を作成。
    """
    os.makedirs(config.OUTPUT_DIR, exist_ok=True)
    conn = sqlite3.connect(config.DB_PATH)
    cursor = conn.cursor()

    # 1) companies
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS companies (
        edinet_code TEXT PRIMARY KEY,
        ticker TEXT,
        company_name TEXT NOT NULL
    )
    ''')

    # 2) filings
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS filings (
        doc_id TEXT PRIMARY KEY,
        edinet_code TEXT NOT NULL,
        parent_doc_id TEXT,
        doc_type_code TEXT NOT NULL,
        submit_datetime TEXT NOT NULL,
        period_start TEXT,
        period_end TEXT,
        csv_flag INTEGER,
        ordinance_code TEXT,
        form_code TEXT,
        accounting_standard TEXT,
        consolidated_flag TEXT,
        edinet_url TEXT,
        FOREIGN KEY(edinet_code) REFERENCES companies(edinet_code)
    )
    ''')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_filings_edinet_submit ON filings(edinet_code, submit_datetime DESC)')

    # 3) financial_snapshots
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS financial_snapshots (
        doc_id TEXT PRIMARY KEY,
        current_assets REAL,
        total_liabilities REAL,
        operating_cash_flow REAL,
        inventory REAL,
        accounts_receivable REAL,
        unit_multiplier INTEGER,
        source_quality_flags TEXT,
        extracted_at TEXT NOT NULL,
        total_shares REAL,
        net_income REAL,
        cash_and_equivalents REAL,
        FOREIGN KEY(doc_id) REFERENCES filings(doc_id)
    )
    ''')

    # 4) company_screening
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS company_screening (
        edinet_code TEXT PRIMARY KEY,
        latest_doc_id TEXT NOT NULL,
        latest_submit_datetime TEXT NOT NULL,
        latest_period_end TEXT,
        ncav REAL,
        is_cf_increasing INTEGER,
        is_inventory_warning INTEGER,
        is_receivable_warning INTEGER,
        alert_flags TEXT,
        checked_at TEXT NOT NULL,
        FOREIGN KEY(edinet_code) REFERENCES companies(edinet_code),
        FOREIGN KEY(latest_doc_id) REFERENCES filings(doc_id)
    )
    ''')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_company_screening_doc ON company_screening(latest_doc_id)')

    conn.commit()
    conn.close()
    print(f"Database initialized at: {config.DB_PATH}")

def upsert_record(table: str, record: Dict[str, Any], pk_cols: List[str]):
    """
    指定されたテーブルとPKカラムに基づいて UPSERT を実行する
    """
    conn = sqlite3.connect(config.DB_PATH)
    cursor = conn.cursor()
    columns = list(record.keys())
    placeholders = ", ".join([f":{c}" for c in columns])
    col_list = ", ".join(columns)

    pk_str = ", ".join(pk_cols)
    update_cols = [c for c in columns if c not in pk_cols]

    if update_cols:
        update_assign = ", ".join([f"{c}=excluded.{c}" for c in update_cols])
        sql = f"""
        INSERT INTO {table} ({col_list})
        VALUES ({placeholders})
        ON CONFLICT({pk_str}) DO UPDATE SET
            {update_assign}
        """
    else:
        # 更新対象カラムがない場合（PKのみ、または変更不要）は無視
        sql = f"""
        INSERT INTO {table} ({col_list})
        VALUES ({placeholders})
        ON CONFLICT({pk_str}) DO NOTHING
        """

    cursor.execute(sql, record)
    conn.commit()
    conn.close()
