# -*- coding: utf-8 -*-
"""
XBRL解析のヘルパー関数群
"""
import pandas as pd
from lxml import etree
from typing import List, Optional, Tuple, Dict, Any

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
    - duration は endDate, instant は instant を期末日として格納
    - 連結/個別は context id に 'nonconsolidated' を含むかで近似判定
    """
    ns = root.nsmap
    ctx = {}
    for c in root.xpath("//xbrli:context", namespaces=ns):
        cid = c.get("id")
        if not cid:
            continue
        
        period = c.find("xbrli:period", namespaces=ns)
        if period is None:
            continue

        is_duration = period.find("xbrli:instant", namespaces=ns) is None
        
        if is_duration:
            end_el = period.find("xbrli:endDate", namespaces=ns)
            end_text = end_el.text if end_el is not None else None
        else:
            inst_el = period.find("xbrli:instant", namespaces=ns)
            end_text = inst_el.text if inst_el is not None else None
            
        is_consolidated = 'nonconsolidated' not in (cid or '').lower()
        ctx[cid] = {
            "is_duration": is_duration, 
            "endDate": end_text, 
            "is_consolidated": is_consolidated
        }
    return ctx, ns

def _unit_is_jpy(root: etree._Element, unit_ref: str) -> bool:
    """指定されたunitRefが 'jpy' (日本円) であるかを判定する"""
    if not unit_ref:
        return False
    ns = root.nsmap
    
    # unitRefが 'jpy' そのものである簡易ケース
    if unit_ref.lower() == 'jpy':
        return True
        
    u = root.xpath(f"//xbrli:unit[@id='{unit_ref}']", namespaces=ns)
    if not u:
        return False
        
    measures = u[0].xpath(".//xbrli:measure/text()", namespaces=ns)
    return any(str(m).lower().endswith(":jpy") or str(m).lower() == "jpy" for m in measures)

def _extract_single_fact(root: etree._Element, contexts: Dict[str, Any], ns: Dict[str, str], 
                         tag_list: List[str], *, need_duration: bool,
                         period_end: str, consolidated_required: bool) -> Tuple[Optional[int], Optional[str], Optional[str]]:
    """
    指定タグ群から、context/通貨/期間が合う最初の単一のファクト(数値)を抽出する
    """
    pe = pd.to_datetime(period_end, errors="coerce")
    pe_str = pe.strftime("%Y-%m-%d") if pd.notna(pe) else None
    if not pe_str:
        return None, None, None

    for tag in tag_list:
        # 名前空間を問わずローカル名で検索
        elems = root.xpath(f"//*[local-name()='{tag}']")
        if not elems:
            continue
            
        for el in elems:
            ctx_id = el.get("contextRef")
            if not ctx_id or ctx_id not in contexts:
                continue
            
            ctx = contexts[ctx_id]

            # 期間タイプ(duration/instant)と期末日が一致するか
            if ctx["is_duration"] != need_duration:
                continue
            if ctx["endDate"] != pe_str:
                continue
            
            # 連結/個別の要件が一致するか
            if ctx["is_consolidated"] != consolidated_required:
                continue

            # 単位が日本円か
            unit_ref = el.get("unitRef")
            if not _unit_is_jpy(root, unit_ref):
                continue
            
            # 数値のパース試行
            try:
                val = _parse_int_loose(el.text)
                return val, tag, ctx_id
            except (ValueError, TypeError, AttributeError):
                continue
                
    return None, None, None
