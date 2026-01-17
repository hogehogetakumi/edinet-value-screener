# -*- coding: utf-8 -*-
"""
データ処理と分析の中核ロジック
- 日次インデックス構築
- 最新の年次報告書特定
- XBRLからの財務データ抽出
- 企業単位の分析実行
"""
import pandas as pd
from datetime import datetime, timedelta
from lxml import etree
from typing import Optional, Dict, Any, List

# プロジェクト内のモジュールをインポート
import config
import edinet_client
import xbrl_parser

# ==============================================================================
# --- Index Builder (daily doclists -> DataFrame)
# ==============================================================================
def build_daily_index(api_key: str, days_back: int) -> pd.DataFrame:
    """
    直近 `days_back` 日分の documents.json(type=2) をキャッシュしつつ集計・結合する
    """
    rows: List[dict] = []
    current_date = datetime.now()
    
    print(f"Building daily index for the last {days_back} days...")
    for i in range(days_back):
        date_str = (current_date - timedelta(days=i)).strftime('%Y-%m-%d')
        docs = edinet_client.list_documents(date_str, api_key)
        if docs:
            rows.extend(docs)
        if (i > 0 and i % 50 == 0) or i == days_back -1:
            print(f"  -> Indexed {i+1}/{days_back} days...")
            
    if not rows:
        print("  -> No documents found in the specified period.")
        return pd.DataFrame()
        
    df = pd.DataFrame(rows)
    
    # データ型を事前整理
    if 'submitDateTime' in df.columns:
        df['submitDateTime'] = pd.to_datetime(df['submitDateTime'], errors='coerce')
    for col in ['docTypeCode', 'edinetCode', 'xbrlFlag', 'periodEnd', 'docID',
                'withdrawalStatus', 'disclosureStatus', 'legalStatus']:
        if col in df.columns:
            df[col] = df[col].astype(str)
            
    return df

def pick_latest_annual_report(df_idx: pd.DataFrame, edinet_code: str) -> Optional[Dict[str, Any]]:
    """
    指定会社の最新の年次報告書を1件返す
    - 事前条件: XBRLあり、取下げ無し、法定公開、通常公開のみ
    - 同一期に複数提出がある場合は、訂正報告書 > 新しい提出日時のものを優先
    """
    if df_idx is None or df_idx.empty:
        return None
    required_cols = {'edinetCode', 'docTypeCode', 'xbrlFlag'}
    if not required_cols.issubset(df_idx.columns):
        print(f"  -> Index DataFrame is missing required columns: {required_cols - set(df_idx.columns)}")
        return None

    # フィルタリング
    sub = df_idx[
        (df_idx['edinetCode'] == edinet_code) &
        (df_idx['docTypeCode'].isin(['120', '130'])) & # 有報・訂正有報
        (df_idx['xbrlFlag'] == '1') &
        (df_idx['withdrawalStatus'] == '0') &
        (df_idx['disclosureStatus'] == '0')
    ].copy()

    if sub.empty or 'periodEnd' not in sub.columns or 'submitDateTime' not in sub.columns:
        return None

    # 日付キーの作成とソート
    sub['periodEnd'] = pd.to_datetime(sub['periodEnd'], errors='coerce')
    sub = sub.dropna(subset=['periodEnd']).copy()
    sub['is_corr'] = (sub['docTypeCode'] == '130').astype(int)
    
    # 最新の提出日時のものを最優先とする
    sub = sub.sort_values(['submitDateTime', 'is_corr'], ascending=[False, False])
    
    latest_report = sub.head(1).to_dict('records')
    if not latest_report:
        return None
        
    report = latest_report[0]
    report['periodEnd_str'] = report['periodEnd'].strftime('%Y-%m-%d')
    return report

# ==============================================================================
# --- Core Extraction Logic ---
# ==============================================================================
def extract_financials(report: Dict[str, Any], api_key: str) -> Dict[str, Any]:
    """
    指定された報告書から財務諸表科目を網羅的に抽出し、当期・前期の値を返す
    """
    base_details = {
        "docID": report.get('docID'), 
        "periodEnd": report.get('periodEnd_str'),
        "accounting_standard": "N/A",
    }
    
    xbrl_bytes = edinet_client.fetch_xbrl_instance(report['docID'], api_key)
    if not xbrl_bytes:
        return base_details

    try:
        root = etree.fromstring(xbrl_bytes)
        parsed_data = xbrl_parser.parse_financial_facts(root)
    except etree.XMLSyntaxError as e:
        print(f"  -> XML Syntax Error for docID {report['docID']}: {e}")
        return base_details
    
    base_details.update(parsed_data)
    return base_details

# ==============================================================================
# --- Company Analysis Runner ---
# ==============================================================================
def analyze_company_latest(code: str, api_key: str, master_df: pd.DataFrame, doc_index_df: pd.DataFrame) -> Optional[Dict[str, Any]]:
    """
    単一企業コードについて、最新の年次報告書から財務情報を抽出し、
    正規化されたDBテーブル群へ保存するためのデータ構造を返す
    """
    try:
        company_info = master_df.loc[code]
    except KeyError:
        print(f"[{code}] Not found in master list. Skipping.")
        return None
        
    report = pick_latest_annual_report(doc_index_df, code)
    if not report:
        return None

    print(f"[{code}] Processing latest docID: {report['docID']}")
    details = extract_financials(report, api_key)
    if not details:
        return None
        
    # 連結フラグの決定 (マスターの '連結の有無' を正とする)
    consolidated_str = str(company_info.get('連結の有無'))
    use_consolidated = (consolidated_str != '無') # '有' または不明/NaNなら連結を優先
    
    scope = "consolidated" if use_consolidated and "consolidated" in details else "non_consolidated"
    financials = details.get(scope, {})
    
    # 必要な財務データがなければ処理中断
    print('financials:',financials)
    if not financials or not financials.get("net_sales"):
        print(f"  -> [{code}] No usable financial data found in scope '{scope}'. Skipping.")
        return None

    # 結果格納用コンテナ
    result_container = {
        "company": {
            "edinet_code": code,
            "ticker": str(company_info.get('証券コード', ''))[:-1] if pd.notna(company_info.get('証券コード')) else None,
            "company_name": company_info.get('提出者名')
        },
        "filings": [],
        "financial_snapshots": [],
        "company_screening": {}
    }

    # --- 1. Filings Data ---
    submit_dt = report.get('submitDateTime')
    period_start_dt = financials.get("net_sales", {}).get("current", {}).get("context", {}).get("startDate")

    filing_rec = {
        "doc_id": report["docID"],
        "edinet_code": code,
        "parent_doc_id": report.get("parentDocID"),
        "doc_type_code": report.get("docTypeCode"),
        "submit_datetime": submit_dt.isoformat() if isinstance(submit_dt, (datetime, pd.Timestamp)) else str(submit_dt),
        "period_start": period_start_dt,
        "period_end": details.get("periodEnd"),
        "csv_flag": int(report.get("csvFlag", 0)) if report.get("csvFlag") else 0,
        "ordinance_code": report.get("ordinanceCode"),
        "form_code": report.get("formCode"),
        "accounting_standard": details.get("accounting_standard"),
        "consolidated_flag": "CONSOLIDATED" if scope == "consolidated" else "NON_CONSOLIDATED",
        "edinet_url": f"https://disclosure2.edinet-fsa.go.jp/WZEK0040.aspx?S100{report['docID'][4:]}" if report.get('docID') else None
    }
    result_container["filings"].append(filing_rec)

    # --- 2. Financial Snapshots Data (Current & Prior) ---
    # 当期
    y1_data = {
        "net_sales": financials.get("net_sales", {}).get("current", {}).get("value"),
        "operating_cash_flow": financials.get("operating_cf", {}).get("current", {}).get("value"),
        "net_income": financials.get("net_income", {}).get("current", {}).get("value"),
        "inventories": financials.get("inventories", {}).get("current", {}).get("value"),
        "cash": financials.get("cash", {}).get("current", {}).get("value"),
        "debt": financials.get("debt", {}).get("current", {}).get("value"),
        "net_assets": financials.get("net_assets", {}).get("current", {}).get("value"),
    }
    # 前期
    y0_data = {
        "net_sales": financials.get("net_sales", {}).get("prior", {}).get("value"),
        "operating_cash_flow": financials.get("operating_cf", {}).get("prior", {}).get("value"),
        "net_income": financials.get("net_income", {}).get("prior", {}).get("value"),
        "inventories": financials.get("inventories", {}).get("prior", {}).get("value"),
        "cash": financials.get("cash", {}).get("prior", {}).get("value"),
        "debt": financials.get("debt", {}).get("prior", {}).get("value"),
        "net_assets": financials.get("net_assets", {}).get("prior", {}).get("value"),
    }

    snapshot_rec = {
        "doc_id": report["docID"],
        "unit_multiplier": 1, # Default to 1 (Yen)
        "extracted_at": datetime.now().isoformat(),
        **{f"y1_{k}": v for k, v in y1_data.items()},
        **{f"y0_{k}": v for k, v in y0_data.items()},
    }
    result_container["financial_snapshots"].append(snapshot_rec)

    # --- 3. Company Screening Data (y1 vs y0) ---
    sales_growth = None
    if y1_data["net_sales"] is not None and y0_data["net_sales"] is not None and y0_data["net_sales"] > 0:
        sales_growth = (y1_data["net_sales"] / y0_data["net_sales"]) - 1

    inventory_growth = None
    if y1_data["inventories"] is not None and y0_data["inventories"] is not None and y0_data["inventories"] > 0:
        inventory_growth = (y1_data["inventories"] / y0_data["inventories"]) - 1
        
    is_bankruptcy_risk = None
    if y1_data["net_income"] is not None and y1_data["operating_cash_flow"] is not None:
        is_bankruptcy_risk = 1 if y1_data["net_income"] > 0 and y1_data["operating_cash_flow"] < 0 else 0

    alert_flags = []
    if is_bankruptcy_risk == 1:
        alert_flags.append("BANKRUPTCY_RISK")
    if inventory_growth is not None and inventory_growth > 0.2: # e.g. 20% increase
         alert_flags.append("HIGH_INVENTORY_GROWTH")

    screening_rec = {
        'edinet_code': code,
        'latest_doc_id': report['docID'],
        'latest_submit_datetime': filing_rec["submit_datetime"],
        'latest_period_end': details.get('periodEnd'),
        'sales_growth_rate': sales_growth,
        'inventory_growth_rate': inventory_growth,
        'is_bankruptcy_risk': is_bankruptcy_risk,
        'alert_flags': ",".join(alert_flags) if alert_flags else None,
        'checked_at': datetime.now().isoformat(),
    }
    result_container["company_screening"] = screening_rec

    return result_container