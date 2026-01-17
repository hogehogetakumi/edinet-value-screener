# -*- coding: utf-8 -*-
"""
設定ファイル
- パス、定数
- XBRLタグ定義
"""
import os

# --- 基本設定 ---
# 環境に合わせてパスを適宜変更してください
# 例: Google Colab の場合
# BASE_DIR = '/content/drive/MyDrive/edinet_api_project'
# ローカル環境の場合
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..')) # プロジェクトルートを想定

OUTPUT_DIR = os.path.join(BASE_DIR, 'public', 'data')
TEMP_DIR = os.path.join(BASE_DIR, 'temp')
DB_PATH = os.path.join(OUTPUT_DIR, 'edinet_analysis.db')
EDINET_CODE_LIST_PATH = os.path.join(BASE_DIR, 'EdinetcodeDlInfo.csv') # 事前にダウンロードが必要
TARGET_LISTS_DIR = os.path.join(BASE_DIR, 'before_screening_lists')
EDINET_BASE_URL = "https://disclosure.edinet-fsa.go.jp/api/v2"

# キャッシュディレクトリ
DOC_CACHE_DIR = os.path.join(TEMP_DIR, 'doclist_cache')

# --- API & 実行制御 ---
MAX_WORKERS = 4  # APIのレートリミットを考慮し、PCのコア数より少なめに
DAYS_BACK = 365 # 日次インデックスを構築する日数

# EDINET APIキー
try:
    from google.colab import userdata
    SUBSCRIPTION_KEY = userdata.get('EDINET_API_KEY')
except (ImportError, ModuleNotFoundError):
    SUBSCRIPTION_KEY = os.getenv("EDINET_API_KEY")

# APIレートリミット設定 (EDINET API仕様: 500ms/req)
MIN_INTERVAL_SEC = 0.6

# --- XBRLタグ定義 (Financial Items: Optimized) ---
# 優先度順。上位のタグが見つかればそれを採用する。
# IFRSと日本基準の両方に対応。
FINANCIAL_ITEM_TAGS = {
    # 1. 成長性: 売上高
    "net_sales": [
        "jpigp_cor:NetSalesIFRS",
        "SalesAndFinancialServicesRevenueIFRS",
        "jppfs_cor:NetSales",
        "NetSales",
        "OperatingRevenue1",
        "Revenue",
        "SalesRevenue"
    ],
    # 2. 効率性: 棚卸資産 (在庫)
    "inventories": [
        # --- 合計タグ (優先的に使用) ---
        "jpigp_cor:InventoriesCAIFRS", # IFRS 基準の棚卸資産
        "jppfs_cor:Inventories",       # 日本基準の棚卸資産
        "Inventories",                 # 一般的な棚卸資産 (合計)
        "InventoriesNet",              # IFRS (合計)
        # --- 内訳タグ (合計がない場合の計算用) ---
        "MerchandiseAndFinishedGoods",
        "Merchandise",
        "FinishedGoods",
        "WorkInProcess",
        "SemiFinishedGoods",
        "RawMaterialsAndSupplies",
        "RawMaterials",
        "Supplies"
    ],
    # 3. 収益性: 純利益
    "net_income": [
        "jpigp_cor:ProfitLossAttributableToOwnersOfParentIFRS",
        "jpigp_cor:ProfitLossIFRS",
        "jppfs_cor:ProfitLoss",
        "jppfs_cor:NetIncome",
        "ProfitLossAttributableToOwnersOfParent",
        "NetIncome",
        "ProfitLoss",
        "NetIncomeLoss"
    ],
    # 4. 現金: 営業キャッシュフロー
    "operating_cf": [
        "jpigp_cor:NetCashProvidedByUsedInOperatingActivitiesIFRS",
        "jppfs_cor:NetCashProvidedByUsedInOperatingActivities",
        "NetCashProvidedByUsedInOperatingActivities",
        "CashFlowsFromUsedInOperatingActivities"
    ],
    # --- 5. 安全性 (B/S項目) ---
    "cash": [
        "CashAndDeposits",
        "CashAndCashEquivalents"
    ],
    "debt": [
        "ShortTermLoansPayable",
        "LongTermLoansPayable",
        "CurrentPortionOfLongTermLoansPayable",
        "BondsPayable",
        "CurrentPortionOfBonds",
        "CommercialPapersLiabilities"
    ],
    "net_assets": [
        "NetAssets",
        "Equity"
    ]
}
