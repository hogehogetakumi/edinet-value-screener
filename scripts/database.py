# -*- coding: utf-8 -*-
"""
データベース関連の関数群
"""
import sqlite3
import os
from typing import Dict, Any, List

import pandas as pd

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
        unit_multiplier INTEGER,
        extracted_at TEXT NOT NULL,
        y1_net_sales REAL,
        y1_operating_cash_flow REAL,
        y1_net_income REAL,
        y1_inventories REAL,
        y1_cash REAL,
        y1_debt REAL,
        y1_net_assets REAL,
        y0_net_sales REAL,
        y0_operating_cash_flow REAL,
        y0_net_income REAL,
        y0_inventories REAL,
        y0_cash REAL,
        y0_debt REAL,
        y0_net_assets REAL,
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
        sales_growth_rate REAL,
        inventory_growth_rate REAL,
        is_bankruptcy_risk INTEGER,
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


def export_company_financials_csv(output_path: str) -> None:
    """
    最新の財務データを集計し、比較CSVとして出力する
    """
    query = """
    WITH latest_filing AS (
        SELECT
            edinet_code,
            MAX(submit_datetime) AS max_submit_datetime
        FROM filings
        WHERE doc_type_code IN ('120', '130') /* 有報・訂正有報 */
        GROUP BY edinet_code
    )
    SELECT
        COALESCE(c.ticker, c.edinet_code) AS code,
        c.company_name AS name,
        f.period_end AS period,
        f.submit_datetime AS submit_date,
        fs.y1_net_sales AS net_sales,
        fs.y0_net_sales AS net_sales_prev,
        fs.y1_inventories AS inventories,
        fs.y0_inventories AS inventories_prev,
        fs.y1_net_income AS net_income,
        fs.y0_net_income AS net_income_prev,
        fs.y1_operating_cash_flow AS operating_cf,
        fs.y0_operating_cash_flow AS operating_cf_prev,
        fs.y1_cash AS cash,
        fs.y0_cash AS cash_prev,
        fs.y1_debt AS debt,
        fs.y0_debt AS debt_prev,
        fs.y1_net_assets AS net_assets,
        fs.y0_net_assets AS net_assets_prev
    FROM filings f
    JOIN latest_filing lf ON f.edinet_code = lf.edinet_code AND f.submit_datetime = lf.max_submit_datetime
    JOIN companies c ON f.edinet_code = c.edinet_code
    JOIN financial_snapshots fs ON f.doc_id = fs.doc_id
    ORDER BY c.edinet_code
    """
    conn = sqlite3.connect(config.DB_PATH)
    df = pd.read_sql_query(query, conn)
    conn.close()
    df.to_csv(output_path, index=False)
    print(f"Exported CSV to: {output_path}")
