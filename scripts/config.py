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

# --- XBRLタグ定義 (PL/BS 最新期のみ) ---

# 損益計算書（PL）
PL_TAGS = {
    # 売上高（IFRS/日本基準の代表的バリエーション）
    "revenue": [
        "Revenue",
        "NetSales",
        "SalesRevenue",
        "OperatingRevenue"
    ],
    # 営業利益
    "operatingProfit": [
        "ProfitLossFromOperatingActivities",
        "OperatingIncome",
        "OperatingProfit"
    ]
}

# 貸借対照表（BS）
BS_TAGS = {
    "totalAssets":        ["TotalAssets", "Assets"],
    "totalLiabilities":   ["TotalLiabilities", "Liabilities"],
    # 純資産：IFRS/日本基準の双方をカバー
    "equity":             ["Equity", "NetAssets", "EquityAttributableToOwnersOfParent"],
    "currentAssets":      ["CurrentAssets", "AssetsCurrent"],
    "currentLiabilities": ["CurrentLiabilities", "LiabilitiesCurrent"],
    "inventory":          ["Inventories", "MerchandiseAndFinishedGoods", "InventoriesNet"],
    "accountsReceivable": ["NotesAndAccountsReceivableTrade", "AccountsReceivableTrade", "NotesAndAccountsReceivableTradeAndContractAssets"]
}

# キャッシュ・フロー計算書（CF）
CF_TAGS = {
    "operatingCashFlow": [
        "NetCashProvidedByUsedInOperatingActivities",
        "CashFlowsFromUsedInOperatingActivities"
    ]
}

# config.py に以下を追加・統合せよ

SHARES_TAGS = {
    # 発行済株式総数
    "total_shares": [
        "TotalNumberIssuedSharesSummaryOfBusinessResults", # 経営指標（サマリ）
        "TotalNumberOfIssuedSharesSummaryOfBusinessResults",
        "TotalNumberIssuedShares",                         # 株式の状況
        "CommonStockIssue",                                # IFRS/米国基準の一部
    ],
    # 自己株式数（金庫株）
    "treasury_stock": [
        "TreasuryStockSummaryOfBusinessResults",           # 経営指標（サマリ）
        "TreasuryStock",                                   # B/S上の自己株式
        "TreasuryShares",                                  # IFRS
    ]
}

# config.py

NET_INCOME_TAGS = {
    "income_statement": [
        # 優先順位1: 親会社の株主の取り分（連結の正解）
        "ProfitLossAttributableToOwnersOfParent",  # J-GAAP
        "ProfitLossAttributableToOwnersOfParentIFRS",
        "ProfitLossAttributableToOwnersOfParentSummaryOfBusinessResults",
        "ProfitAttributableToOwnersOfParent",      # IFRS

        # 米国基準 (US-GAAP) - たまに日本企業でも使う
        "NetIncomeLossAttributableToOwnersOfParent",
        "NetIncomeLoss",
        
        # 優先順位2: 単なる当期純利益（単体決算や、上記が見つからない場合の妥協案）
        "ProfitLoss",                              # J-GAAP/IFRS共通
        "NetIncome",                               # 米国基準など
    ]
}

# config.py

CASH_TAGS = [
    # --- 【最優先】 現金及び現金同等物 (Level 2: 真水) ---
    # IFRS企業はBSにこれを計上する。
    # 日本基準企業もCF計算書の「期末残高」としてこのタグを持つことが多い。
    "CashAndDeposits",
    "CashAndCashEquivalents",       
    
    # --- 【次点】 現金及び預金 (Level 1: ほぼ現金) ---
    # 日本基準のBSで最も一般的なタグ。
    # 上記が取れなければこれで妥協する。
    "CashAndDeposits",
    
    # --- 【予備】 その他考えられるタグ ---
    "Cash",                         # 米国基準や非常に古い形式
    "CashAndCashEquivalentsAtCarryingValue", # 米国基準など
    "CashAndCashEquivalentsIFRSSummaryOfBusinessResults"
]