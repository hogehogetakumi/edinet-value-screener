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
            
    print(f"Daily index built successfully with {df} total documents.")
    return df

def pick_recent_annual_reports(df_idx: pd.DataFrame, edinet_code: str) -> List[dict]:
    """
    指定会社の直近2期分の年次報告書を返す（最新順）
    - 事前条件: XBRLあり、取下げ無し、法定公開、通常公開のみ
    """
    if df_idx is None or df_idx.empty:
        return []
    required_cols = {'edinetCode', 'docTypeCode', 'xbrlFlag'}
    if not required_cols.issubset(df_idx.columns):
        print(f"  -> Index DataFrame is missing required columns: {required_cols - set(df_idx.columns)}")
        return []

    # フィルタリング
    sub = df_idx[
        (df_idx['edinetCode'] == edinet_code) &
        (df_idx['docTypeCode'].isin(['120', '130'])) & # 有報・訂正 (四半期140を除外)
        (df_idx['xbrlFlag'] == '1') &
        (df_idx['withdrawalStatus'] == '0') & # 取下げなし
        (df_idx['disclosureStatus'] == '0') # 通常公開
    ].copy()

    if sub.empty or 'periodEnd' not in sub.columns:
        return []

    # 日付キーの作成とソート
    sub['periodEnd'] = pd.to_datetime(sub['periodEnd'], errors='coerce')
    sub = sub.dropna(subset=['periodEnd']).copy()
    sub['year_key'] = sub['periodEnd'].dt.strftime('%Y-%m-%d')
    sub['is_corr'] = (sub['docTypeCode'] == '130').astype(int)
    
    # 同一期内での優先順位付け: 訂正報告 > 新しい提出日時
    sub = sub.sort_values(['year_key', 'is_corr', 'submitDateTime'], ascending=[True, False, False])
    sub = sub.drop_duplicates('year_key', keep='first')

    # 最新順にソートして最大2件返す
    sub = sub.sort_values('submitDateTime', ascending=False)
    
    results = sub.head(2).to_dict('records')
    for r in results:
        r['periodEnd_str'] = r['year_key']
    return results

# ==============================================================================
# --- Core Extraction Logic ---
# ==============================================================================
def extract_financials(report: Dict[str, Any], api_key: str, consolidated_required: bool) -> Dict[str, Any]:
    """
    指定された報告書から財務諸表科目を抽出する
    """
    details: Dict[str, Any] = { 
        "docID": report.get('docID'), 
        "periodEnd": report.get('periodEnd_str'),
        "periodStart": report.get('periodStart')
    }
    
    xbrl_bytes = edinet_client.fetch_xbrl_instance(report['docID'], api_key)
    if not xbrl_bytes:
        return details

    try:
        root = etree.fromstring(xbrl_bytes)
        contexts, ns = xbrl_parser._build_contexts(root)
    except etree.XMLSyntaxError as e:
        print(f"  -> XML Syntax Error for docID {report['docID']}: {e}")
        return details

    period_end_str = report.get('periodEnd_str')
    
    # 会計基準の取得 (任意)
    try:
        standard_elem = root.xpath("//*[local-name()='AccountingStandardsDEI']")
        details['accountingStandard'] = standard_elem[0].text if standard_elem else "N/A"
    except Exception:
        details['accountingStandard'] = "Error"

    # --- 項目抽出 ---
    # PL (期間: duration)
    rev, rev_tag, rev_ctx = xbrl_parser._extract_single_fact(root, contexts, ns, config.PL_TAGS["revenue"], need_duration=True, period_end=period_end_str, consolidated_required=consolidated_required)
    op, op_tag, op_ctx = xbrl_parser._extract_single_fact(root, contexts, ns, config.PL_TAGS["operatingProfit"], need_duration=True, period_end=period_end_str, consolidated_required=consolidated_required)
    ocf, ocf_tag, ocf_ctx = xbrl_parser._extract_single_fact(root, contexts, ns, config.CF_TAGS["operatingCashFlow"], need_duration=True, period_end=period_end_str, consolidated_required=consolidated_required)

    # BS (時点: instant)
    ta, ta_tag, ta_ctx = xbrl_parser._extract_single_fact(root, contexts, ns, config.BS_TAGS["totalAssets"], need_duration=False, period_end=period_end_str, consolidated_required=consolidated_required)
    tl, tl_tag, tl_ctx = xbrl_parser._extract_single_fact(root, contexts, ns, config.BS_TAGS["totalLiabilities"], need_duration=False, period_end=period_end_str, consolidated_required=consolidated_required)
    eq, eq_tag, eq_ctx = xbrl_parser._extract_single_fact(root, contexts, ns, config.BS_TAGS["equity"], need_duration=False, period_end=period_end_str, consolidated_required=consolidated_required)
    ca, ca_tag, ca_ctx = xbrl_parser._extract_single_fact(root, contexts, ns, config.BS_TAGS["currentAssets"], need_duration=False, period_end=period_end_str, consolidated_required=consolidated_required)
    cl, cl_tag, cl_ctx = xbrl_parser._extract_single_fact(root, contexts, ns, config.BS_TAGS["currentLiabilities"], need_duration=False, period_end=period_end_str, consolidated_required=consolidated_required)
    inv, inv_tag, inv_ctx = xbrl_parser._extract_single_fact(root, contexts, ns, config.BS_TAGS["inventory"], need_duration=False, period_end=period_end_str, consolidated_required=consolidated_required)
    ar, ar_tag, ar_ctx = xbrl_parser._extract_single_fact(root, contexts, ns, config.BS_TAGS["accountsReceivable"], need_duration=False, period_end=period_end_str, consolidated_required=consolidated_required)

    # 株式数の取得（BS項目なので need_duration=False, 単位チェック無効）
    total_shares, _, _ = xbrl_parser._extract_single_fact(
        root, contexts, ns, 
        config.SHARES_TAGS["total_shares"], 
        need_duration=False,         # ← 重要: 株式数は「一時点(Instant)」のデータ
        period_end=period_end_str, 
        consolidated_required=False, # ※注記参照
        check_unit=False             # ← 重要: 単位チェックをスキップ
    )

    # # 自己株式数の取得
    # treasury_stock, _, _ = xbrl_parser._extract_single_fact(
    #     root, contexts, ns, 
    #     config.SHARES_TAGS["treasury_stock"], 
    #     need_duration=False, 
    #     period_end=period_end_str, 
    #     consolidated_required=False,
    #     check_unit=False             # ← 重要
    # )

    income, _, _ = xbrl_parser._extract_single_fact(
        root, contexts, ns, 
        config.NET_INCOME_TAGS["income_statement"],      # ← タグリストはもちろん変更する
        need_duration=True,          # ← 【変更】PLは「期間」のデータなので True
        period_end=period_end_str, 
        consolidated_required=True,  # ← 【変更】親会社株主に帰属〜は「連結」概念なので True
        check_unit=True              # ← 【変更】単位は「円」なので True (デフォルト)
    )

    cash_and_equiv, _, _ = xbrl_parser._extract_single_fact(
        root, contexts, ns, 
        config.CASH_TAGS,      # ← タグリストはもちろん変更する
        need_duration=False,          # ← 【変更】B/Sは「一時点(Instant)」のデータなので False
        period_end=period_end_str, 
        consolidated_required=True,  # ← 【変更】親会社株主に帰属〜は「連結」概念なので True
        check_unit=True              # ← 【変更】単位は「円」なので True (デフォルト)
    )
    print(f"  -> Extracted cash_and_equiv cash_and_equiv: {cash_and_equiv}.")



    # Noneハンドリング
    # if total_shares is None:
    #     # 株式数が取れない＝計算不能。ログを出してスキップか、エラー処理
    #     return None

    # if treasury_stock is None:
    #     treasury_stock = 0

    details.update({
        "revenue": rev, "revenueTag": rev_tag, "revenueCtx": rev_ctx,
        "operatingProfit": op, "operatingProfitTag": op_tag, "operatingProfitCtx": op_ctx,
        "totalAssets": ta, "totalAssetsTag": ta_tag, "totalAssetsCtx": ta_ctx,
        "totalLiabilities": tl, "totalLiabilitiesTag": tl_tag, "totalLiabilitiesCtx": tl_ctx,
        "equity": eq, "equityTag": eq_tag, "equityCtx": eq_ctx,
        "currentAssets": ca, "currentAssetsTag": ca_tag, "currentAssetsCtx": ca_ctx,
        "currentLiabilities": cl, "currentLiabilitiesTag": cl_tag, "currentLiabilitiesCtx": cl_ctx,
        "inventory": inv, "inventoryTag": inv_tag, "inventoryCtx": inv_ctx,
        "accountsReceivable": ar, "accountsReceivableTag": ar_tag, "accountsReceivableCtx": ar_ctx,
        "operatingCashFlow": ocf, "operatingCashFlowTag": ocf_tag, "operatingCashFlowCtx": ocf_ctx,
        "totalShares": total_shares,
        # "treasuryStock": treasury_stock,
        "netIncome": income,
        "cashAndEquivalents": cash_and_equiv,
    })
    return details

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
        
    reports = pick_recent_annual_reports(doc_index_df, code)
    if not reports:
        return None

    # 連結フラグの決定
    consolidated_str = str(company_info.get('連結の有無'))
    if consolidated_str == '有':
        consolidated_flag = 'CONSOLIDATED'
        consolidated_required = True
    elif consolidated_str == '無':
        consolidated_flag = 'NON_CONSOLIDATED'
        consolidated_required = False
    else:
        consolidated_flag = 'UNKNOWN'
        consolidated_required = True

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
    
    processed_details = []

    # 各レポートの処理
    for i, report in enumerate(reports):
        label = "Y1" if i == 0 else f"Y{i}"
        print(f"[{code}] Processing {label} docID: {report['docID']}")
        
        details = extract_financials(report, api_key, consolidated_required)
        
        if details is None:
            # print(f"  -> Failed to extract financial details for docID {report.get('docID')}. Skipping this report.")
            continue

        processed_details.append(details)

        # 1. Filings Data
        submit_dt = report.get('submitDateTime')
        filing_rec = {
            "doc_id": report["docID"],
            "edinet_code": code,
            "parent_doc_id": report.get("parentDocID"),
            "doc_type_code": report.get("docTypeCode"),
            "submit_datetime": submit_dt.isoformat() if isinstance(submit_dt, (datetime, pd.Timestamp)) else str(submit_dt),
            "period_start": details.get("periodStart"),
            "period_end": details.get("periodEnd"),
            "csv_flag": int(report.get("csvFlag", 0)) if report.get("csvFlag") else 0,
            "ordinance_code": report.get("ordinanceCode"),
            "form_code": report.get("formCode"),
            "accounting_standard": details.get("accountingStandard"),
            "consolidated_flag": consolidated_flag,
            "edinet_url": f"https://disclosure2.edinet-fsa.go.jp/WZEK0040.aspx?S100{report['docID'][4:]}" if report.get('docID') else None
        }
        result_container["filings"].append(filing_rec)

        # 2. Financial Snapshots Data
        # 欠損フラグの簡易生成
        quality_flags = []
        if details.get("currentAssets") is None: quality_flags.append("MISSING_CA")
        if details.get("totalLiabilities") is None: quality_flags.append("MISSING_TL")

        snapshot_rec = {
            "doc_id": report["docID"],
            "current_assets": details.get("currentAssets"),
            "total_liabilities": details.get("totalLiabilities"),
            "operating_cash_flow": details.get("operatingCashFlow"),
            "inventory": details.get("inventory"),
            "accounts_receivable": details.get("accountsReceivable"),
            "unit_multiplier": 1, # Default to 1 (Yen) as per current parser logic
            "source_quality_flags": ",".join(quality_flags) if quality_flags else None,
            "total_shares": details.get("totalShares"),
            # "treasury_stock": details.get("treasuryStock"),
            "net_income": details.get("netIncome"),
            "cash_and_equivalents": details.get("cashAndEquivalents"),
            "extracted_at": datetime.now().isoformat()
        }
        result_container["financial_snapshots"].append(snapshot_rec)

    # 3. Company Screening Data (Latest vs Previous)
    if not processed_details:
        return None

    y1_details = processed_details[0]
    y0_details = processed_details[1] if len(processed_details) > 1 else {}
    
    # NCAV計算: 流動資産 - 総負債
    ncav = None
    if y1_details.get('currentAssets') is not None and y1_details.get('totalLiabilities') is not None:
        ncav = y1_details['currentAssets'] - y1_details['totalLiabilities']
    
    # フラグ判定
    is_cf_increasing = None
    if y1_details.get('operatingCashFlow') is not None and y0_details.get('operatingCashFlow') is not None:
        is_cf_increasing = 1 if y1_details['operatingCashFlow'] > y0_details['operatingCashFlow'] else 0

    is_inventory_warning = None
    if y1_details.get('inventory') is not None and y0_details.get('inventory') is not None:
        is_inventory_warning = 1 if y1_details['inventory'] > y0_details['inventory'] * 1.2 else 0

    is_receivable_warning = None
    if y1_details.get('accountsReceivable') is not None and y0_details.get('accountsReceivable') is not None:
        is_receivable_warning = 1 if y1_details['accountsReceivable'] > y0_details['accountsReceivable'] * 1.2 else 0

    alert_flags = []
    if ncav is None: alert_flags.append("NCAV_CALC_FAILED")

    screening_rec = {
        'edinet_code': code,
        'latest_doc_id': y1_details['docID'],
        'latest_submit_datetime': result_container["filings"][0]["submit_datetime"],
        'latest_period_end': y1_details.get('periodEnd'),
        'ncav': ncav,
        'is_cf_increasing': is_cf_increasing,
        'is_inventory_warning': is_inventory_warning,
        'is_receivable_warning': is_receivable_warning,
        'alert_flags': ",".join(alert_flags) if alert_flags else None,
        'checked_at': datetime.now().isoformat(),
    }
    result_container["company_screening"] = screening_rec

    return result_container
