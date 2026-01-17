# -*- coding: utf-8 -*-
"""
XBRL解析のヘルパー関数群
"""
import pandas as pd
from lxml import etree
from typing import List, Optional, Tuple, Dict, Any

# プロジェクト内のモジュールをインポート
import config

def _parse_int_loose(txt: str) -> int:
    """カンマ区切り、(123)のような負数表現に対応した数値パース"""
    if txt is None:
        raise ValueError("Input cannot be None")
    s = str(txt).strip().replace(",", "")
    if s.startswith("(") and s.endswith(")"):  # (123) -> -123
        s = "-" + s[1:-1]
    try:
        return int(s)
    except (ValueError, TypeError):
        return int(float(s))

def _build_contexts(root: etree._Element) -> Tuple[Dict[str, Any], Dict[str, str]]:
    """
    XBRL内のコンテキスト(context)情報を解析して辞書を構築する
    - 期間(startDate, endDate)と時点(instant)を解析
    - 連結/非連結の次元(dimension)を解析
    """
    ns = root.nsmap
    ctx_map = {}
    for c in root.xpath("//xbrli:context", namespaces=ns):
        cid = c.get("id")
        if not cid:
            continue
        
        context_data = {
            "id": cid,
            "startDate": None,
            "endDate": None,
            "is_duration": False,
            "scope": "consolidated"  # Default to consolidated
        }

        # --- Period (instant or duration) ---
        period = c.find("xbrli:period", namespaces=ns)
        if period is None:
            continue
            
        instant_el = period.find("xbrli:instant", namespaces=ns)
        if instant_el is not None:
            context_data["endDate"] = instant_el.text
        else:
            context_data["is_duration"] = True
            start_date_el = period.find("xbrli:startDate", ns)
            end_date_el = period.find("xbrli:endDate", ns)
            context_data["startDate"] = start_date_el.text if start_date_el is not None else None
            context_data["endDate"] = end_date_el.text if end_date_el is not None else None

        # --- Scenario (for dimensions like consolidated/non-consolidated) ---
        scenario = c.find("xbrli:scenario", namespaces=ns)
        if scenario is not None:
            # Find explicit members in the scenario
            for member in scenario.xpath(".//xbrldi:explicitMember", namespaces=ns):
                dimension = member.get("dimension")
                if dimension and dimension.endswith("ConsolidatedOrNonConsolidatedAxis"):
                    if member.text and member.text.endswith("NonConsolidatedMember"):
                        context_data["scope"] = "non_consolidated"
                    # If it's ConsolidatedMember, it matches our default, so no need to change
                    break # Assume only one such dimension per context
        
        ctx_map[cid] = context_data
    return ctx_map, ns

def _unit_is_jpy(root: etree._Element, unit_ref: str) -> bool:
    """指定されたunitRefが 'jpy' (日本円) であるかを判定する"""
    if not unit_ref:
        return False
    ns = root.nsmap
    
    if unit_ref.lower() == 'jpy':
        return True
        
    u = root.xpath(f"//xbrli:unit[@id='{unit_ref}']", namespaces=ns)
    if not u:
        return False
        
    measures = u[0].xpath(".//xbrli:measure/text()", namespaces=ns)
    return any(str(m).lower().endswith(":jpy") or str(m).lower() == "jpy" for m in measures)

def _find_fact_in_contexts(root: etree._Element, contexts: Dict[str, Any], ns: Dict[str, str],
                           tag_list: List[str], allowed_context_ids: List[str],
                           *, need_duration: bool, check_unit: bool = True
                           ) -> Tuple[Optional[int], Optional[str], Optional[str]]:
    """
    指定されたcontext IDのリストに合致する最初の有効なファクトを検索する
    """
    # E03588 specific debugging
    SHOULD_DEBUG = "E03588" in root.find('.//jpdei_cor:EDINETCodeDEI', root.nsmap).text
    
    if SHOULD_DEBUG:
        print(f"\n[DEBUG] Searching for tags: {tag_list}")
        print(f"[DEBUG] Allowed contexts: {allowed_context_ids}")
        print(f"[DEBUG] Duration needed: {need_duration}")

    for tag in tag_list:
        tag_local = tag.split(":")[-1]
        # 名前空間を無視して、ローカル名だけで検索
        elems = root.xpath(f"//*[local-name()='{tag_local}']")

        if SHOULD_DEBUG and elems:
            print(f"[DEBUG] Found {len(elems)} potential elements for tag '{tag}'")

        for el in elems:
            ctx_id = el.get("contextRef")
            
            # 提出者（EDINETコード）の特定
            edinet_code_elem = root.find('.//jpdei_cor:EDINETCodeDEI', root.nsmap)
            edinet_code = edinet_code_elem.text if edinet_code_elem is not None else "Unknown"

            # E03588の時だけデバッグ情報を表示
            if "E03588" in edinet_code:
                print(f"[DEBUG]  - Checking element <{el.tag}> with context '{ctx_id}' and value '{el.text}'")


            if not ctx_id or ctx_id not in allowed_context_ids:
                if "E03588" in edinet_code:
                    print(f"[DEBUG]    - REJECTED: Context '{ctx_id}' not in allowed list.")
                continue
            
            ctx = contexts.get(ctx_id)
            if not ctx:
                if "E03588" in edinet_code:
                    print(f"[DEBUG]    - REJECTED: Context ID '{ctx_id}' not found in context map.")
                continue

            if ctx["is_duration"] != need_duration:
                if "E03588" in edinet_code:
                    print(f"[DEBUG]    - REJECTED: Duration mismatch. Have: {ctx['is_duration']}, Need: {need_duration}")
                continue

            if check_unit:
                unit_ref = el.get("unitRef")
                if not _unit_is_jpy(root, unit_ref):
                    if "E03588" in edinet_code:
                        print(f"[DEBUG]    - REJECTED: Unit '{unit_ref}' is not JPY.")
                    continue
            
            original_qname = etree.QName(el.tag)
            tag_prefix = tag.split(":")[0] if ":" in tag else None

            if tag_prefix and ns.get(tag_prefix) and ns.get(tag_prefix) != original_qname.namespace:
                if "E03588" in edinet_code:
                     print(f"[DEBUG]    - REJECTED: Namespace mismatch. Expected prefix '{tag_prefix}' (ns: {ns.get(tag_prefix)}), but got ns: '{original_qname.namespace}'")
                continue

            try:
                val = _parse_int_loose(el.text)
                if "E03588" in edinet_code:
                    print(f"[DEBUG]    - SUCCESS: Found valid fact. Value: {val}")
                return val, tag, ctx_id
            except (ValueError, TypeError, AttributeError):
                if "E03588" in edinet_code:
                    print(f"[DEBUG]    - REJECTED: Failed to parse value '{el.text}'.")
                continue
    
    if SHOULD_DEBUG:
        print(f"[DEBUG] No matching fact found for tags: {tag_list}")
                
    return None, None, None

def _calculate_inventories_value(root: etree._Element, contexts: Dict[str, Any], ns: Dict[str, str],
                               allowed_context_ids: List[str], need_duration: bool
                               ) -> Tuple[Optional[int], Optional[str], Optional[str]]:
    """
    在庫の合計値を計算する。まず合計タグを探し、なければ構成要素から合算する。
    """
    # 1. 合計タグを優先的に検索
    total_tags = [
        "jpigp_cor:InventoriesCAIFRS",
        "jppfs_cor:Inventories",
        "Inventories",
        "InventoriesNet"
    ]
    val, tag, ctx_id = _find_fact_in_contexts(root, contexts, ns, total_tags, allowed_context_ids, need_duration=need_duration)
    if val is not None:
        return val, tag, ctx_id

    # 2. 合計タグがなければ、構成要素から計算
    total_from_components = 0
    found_any_component = False
    found_ctx_id = None  # 最初に見つかったコンポーネントのコンテキストを保持

    def get_component(tag_name: str) -> int:
        nonlocal found_any_component, found_ctx_id
        comp_val, _, comp_ctx_id = _find_fact_in_contexts(root, contexts, ns, [tag_name], allowed_context_ids, need_duration=need_duration)
        if comp_val is not None:
            if not found_any_component:
                found_ctx_id = comp_ctx_id
            found_any_component = True
            return comp_val
        return 0

    # --- ブロック1: 販売用 (商品・製品) ---
    combined_val, _, combined_ctx_id = _find_fact_in_contexts(root, contexts, ns, ["MerchandiseAndFinishedGoods"], allowed_context_ids, need_duration=need_duration)
    if combined_val is not None:
        total_from_components += combined_val
        found_any_component = True
        found_ctx_id = combined_ctx_id
    else:
        total_from_components += get_component("Merchandise")
        total_from_components += get_component("FinishedGoods")

    # --- ブロック2: 製造途中 (仕掛品・半製品) ---
    total_from_components += get_component("WorkInProcess")
    total_from_components += get_component("SemiFinishedGoods")

    # --- ブロック3: 材料 (原材料・貯蔵品) ---
    raw_combined_val, _, raw_combined_ctx_id = _find_fact_in_contexts(root, contexts, ns, ["RawMaterialsAndSupplies"], allowed_context_ids, need_duration=need_duration)
    if raw_combined_val is not None:
        total_from_components += raw_combined_val
        if not found_any_component:
            found_ctx_id = raw_combined_ctx_id
        found_any_component = True
    else:
        total_from_components += get_component("RawMaterials")
        total_from_components += get_component("Supplies")

    if found_any_component:
        return total_from_components, "Inventories(Calculated)", found_ctx_id

    return None, None, None

def get_latest_dates(contexts: Dict[str, Any]) -> Tuple[Optional[str], Optional[str]]:
    """Get the most recent duration end date and instant date from all contexts."""
    latest_duration_end_date = None
    latest_instant_date = None
    for ctx in contexts.values():
        if ctx['is_duration'] and ctx['endDate']:
            if latest_duration_end_date is None or ctx['endDate'] > latest_duration_end_date:
                latest_duration_end_date = ctx['endDate']
        elif not ctx['is_duration'] and ctx['endDate']:
            if latest_instant_date is None or ctx['endDate'] > latest_instant_date:
                latest_instant_date = ctx['endDate']
    return latest_duration_end_date, latest_instant_date


def get_target_context_ids(contexts: Dict[str, Any], scope: str, is_duration: bool, date: str) -> List[str]:
    """Find context IDs matching scope, type (duration/instant), and date."""
    ids = []
    for cid, ctx in contexts.items():
        if ctx['scope'] == scope and ctx['is_duration'] == is_duration and ctx['endDate'] == date:
            ids.append(cid)
    return ids


def parse_financial_facts(root: etree._Element) -> Dict[str, Any]:
    """
    XBRLインスタンスから主要な財務数値を網羅的に抽出する (動的コンテキスト検索版)
    """
    contexts, ns = _build_contexts(root)
    results: Dict[str, Any] = {}

    if not contexts:
        return results

    # Determine the latest reporting dates dynamically
    latest_duration_end, latest_instant_end = get_latest_dates(contexts)

    # Determine prior dates (approx. 1 year ago)
    # This is a simplification; a more robust solution would parse dates properly.
    if latest_duration_end:
        prior_duration_end = str(int(latest_duration_end[:4]) - 1) + latest_duration_end[4:]
    else:
        prior_duration_end = None

    if latest_instant_end:
        prior_instant_end = str(int(latest_instant_end[:4]) - 1) + latest_instant_end[4:]
    else:
        prior_instant_end = None

    for scope in ["consolidated", "non_consolidated"]:
        scope_results: Dict[str, Any] = {}
        for item, tags in config.FINANCIAL_ITEM_TAGS.items():
            item_results: Dict[str, Any] = {}
            # B/S項目 (時点) か P/L, C/F項目 (期間) かを判定
            is_bs_item = item in ["inventories", "cash", "debt", "net_assets"]
            need_duration = not is_bs_item

            # --- Current Period ---
            current_date = latest_instant_end if is_bs_item else latest_duration_end
            if current_date:
                allowed_ctx_ids = get_target_context_ids(contexts, scope, need_duration, current_date)
                if allowed_ctx_ids:
                    if item == "inventories":
                        val, tag, found_ctx_id = _calculate_inventories_value(root, contexts, ns, allowed_ctx_ids, need_duration=need_duration)
                    else:
                        val, tag, found_ctx_id = _find_fact_in_contexts(root, contexts, ns, tags, allowed_ctx_ids, need_duration=need_duration)
                    
                    if val is not None:
                        item_results["current"] = {"value": val, "tag": tag, "context": contexts.get(found_ctx_id)}

            # --- Prior Period ---
            prior_date = prior_instant_end if is_bs_item else prior_duration_end
            if prior_date:
                allowed_ctx_ids = get_target_context_ids(contexts, scope, need_duration, prior_date)
                # Also check for dates that are off by a day, which can happen.
                try:
                    from datetime import datetime, timedelta
                    d = datetime.strptime(prior_date, "%Y-%m-%d")
                    prior_date_minus_1 = (d - timedelta(days=1)).strftime("%Y-%m-%d")
                    allowed_ctx_ids.extend(get_target_context_ids(contexts, scope, need_duration, prior_date_minus_1))
                except (ValueError, ImportError):
                    pass # ignore if date parsing fails

                if allowed_ctx_ids:
                    if item == "inventories":
                        val, tag, found_ctx_id = _calculate_inventories_value(root, contexts, ns, allowed_ctx_ids, need_duration=need_duration)
                    else:
                        val, tag, found_ctx_id = _find_fact_in_contexts(root, contexts, ns, tags, allowed_ctx_ids, need_duration=need_duration)

                    if val is not None:
                        item_results["prior"] = {"value": val, "tag": tag, "context": contexts.get(found_ctx_id)}

            if item_results:
                scope_results[item] = item_results
        
        if scope_results:
            results[scope] = scope_results

    # Accounting standard (remains the same)
    try:
        standard_elem = root.xpath("//*[local-name()='AccountingStandardsDEI']")
        results['accounting_standard'] = standard_elem[0].text if standard_elem else "N/A"
    except Exception:
        results['accounting_standard'] = "Error"

    return results