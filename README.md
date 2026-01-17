
---

# Kessan X-Ray 開発仕様書

## 1. 要件定義 (Requirements)

### 1.1 プロダクトコンセプト

* **名称:** Kessan X-Ray（決算レントゲン）
* **Core Value:** 企業の「見せかけの好業績（PL）」の裏にある「財務の病巣（BS/CF）」を可視化し、投資家の損失を未然に防ぐ。
* **ターゲット:** 財務分析のスキルや時間がない兼業投資家。

### 1.2 解決する課題 (User Stories)

* **User Story A:** ユーザーは「売上は伸びているが、在庫が異常に積み上がっている（将来の損失リスク）」企業をひと目で見抜きたい。
* **User Story B:** ユーザーは「黒字だが現金が入ってきていない（粉飾・黒字倒産リスク）」企業を回避したい。
* **User Story C:** ユーザーは、スマホで移動中に「3秒」で保有株の健康状態をチェックしたい。

### 1.3 MVP機能スコープ

* EDINETからのデータ自動収集（日次バッチ）。
* 2つの異常検知ロジックの実装（在庫過多、利益/CF乖離）。
* スマホで見やすいWeb UIでの信号（青・黄・赤）表示。

---

## 2. 基本設計 (Architecture)

### 2.1 システム構成図

**「コストゼロ・運用ゼロ」ハイブリッド構成**

1. **Backend (Logic):** `GitHub Actions` (Python)
* トリガー: 毎日深夜（JST 03:00）
* 処理: EDINET API叩く → XBRL解析 → 計算


2. **Storage (Data Lake):** `Google Drive`
* 役割: 全データのバックアップ、CSV/SQLiteファイルの保管。
* 接続: Google Drive API経由。


3. **Database (App DB):** `Supabase` (PostgreSQL)
* 役割: Webアプリが表示するための「最新の判定結果（軽量データ）」のみを保持。


4. **Frontend (UI):** `Next.js` on `Vercel`
* 役割: ユーザーインターフェース。Supabaseからデータを取得して表示。



### 2.2 技術スタック

* **Language:** Python 3.x (Batch), TypeScript (Frontend)
* **Libraries:** `edinet-xbrl`, `pandas` (Python), `React`, `Tailwind CSS` (TS)
* **Infra:** Vercel (Web), Supabase (DB), GitHub Actions (Batch/CI)

---

## 3. 詳細設計 (Specifications)

### 3.1 データベース設計 (Supabase)

テーブル名: `financial_healths`

| Column | Type | Description |
| --- | --- | --- |
| `id` | uuid | Primary Key |
| `company_code` | varchar(5) | 証券コード (例: 7203) |
| `company_name` | varchar | 企業名 |
| `period` | date | 決算期（いつのデータか） |
| `signal_inventory` | varchar | 在庫判定 ('GREEN', 'YELLOW', 'RED') |
| `signal_cf` | varchar | CF判定 ('GREEN', 'YELLOW', 'RED') |
| `comment_inventory` | text | 在庫に関する一言コメント |
| `comment_cf` | text | CFに関する一言コメント |
| `updated_at` | timestamp | 更新日時 |

*※ユニーク制約: `company_code` + `period*`

### 3.2 判定ロジック仕様 (Logic Specs)

#### A. 在庫積み上がり検知 (Inventory Bloat)

* **Input:**
* 売上高成長率 () = (今期売上 - 前期売上) / 前期売上
* 在庫増加率 () = (今期在庫 - 前期在庫) / 前期在庫


* **Logic:**
* **RED (危険):** 
* 意味: 売上の伸び以上に在庫が爆増している。


* **YELLOW (注意):** 
* **GREEN (正常):** 上記以外



#### B. Accruals（利益/CF乖離）検知

* **Input:**
* 当期純利益 ()
* 営業キャッシュフロー ()


* **Logic:**
* **RED (危険):**  AND 
* 意味: 黒字なのに現金が減っている（最悪のパターン）。


* **YELLOW (注意):** 
* 意味: 利益の半分も現金が入っていない。


* **GREEN (正常):** 上記以外



---

## 4. 開発ロードマップ (Action Plan)

1. **Phase 1: 環境構築 (Today)**
* GitHubリポジトリ作成。
* Supabaseプロジェクト作成 & テーブル作成。
* Google Cloud Project作成 (Drive API有効化)。


2. **Phase 2: バックエンド実装 (Week 1)**
* PythonでEDINETからデータ取得 → Drive保存。
* XBRL解析ロジック実装。
* Supabaseへのデータ投入連携。


3. **Phase 3: フロントエンド実装 (Week 2)**
* Next.jsプロジェクト作成。
* Supabaseからデータ取得・表示。
* UIデザイン適用。



---