import json
from typing import Any, Dict, Optional


def _to_int(value: Any) -> Optional[int]:
    if value is None:
        return None
    text = str(value).strip()
    if text == "":
        return None
    return int(float(text))


def _to_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    text = str(value).strip()
    if text == "":
        return None
    return float(text)


def _to_str(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _json_bytes(payload: Dict[str, Any]) -> bytes:
    return json.dumps(payload, ensure_ascii=False).encode("utf-8")


def member_payload(row: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "msno": _to_str(row.get("msno")),
        "city": _to_int(row.get("city")),
        "bd": _to_int(row.get("bd")),
        "gender": _to_str(row.get("gender")) or "unknown",
        "registered_via": _to_int(row.get("registered_via")),
        "registration_init_time": _to_str(row.get("registration_init_time")),
    }


def transaction_payload(row: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "msno": _to_str(row.get("msno")),
        "payment_method_id": _to_int(row.get("payment_method_id")),
        "payment_plan_days": _to_int(row.get("payment_plan_days")),
        "plan_list_price": _to_float(row.get("plan_list_price")),
        "actual_amount_paid": _to_float(row.get("actual_amount_paid")),
        "is_auto_renew": _to_int(row.get("is_auto_renew")),
        "transaction_date": _to_str(row.get("transaction_date")),
        "membership_expire_date": _to_str(row.get("membership_expire_date")),
        "is_cancel": _to_int(row.get("is_cancel")),
    }


def user_log_payload(row: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "msno": _to_str(row.get("msno")),
        "date": _to_str(row.get("date")),
        "num_25": _to_int(row.get("num_25")),
        "num_50": _to_int(row.get("num_50")),
        "num_75": _to_int(row.get("num_75")),
        "num_985": _to_int(row.get("num_985")),
        "num_100": _to_int(row.get("num_100")),
        "num_unq": _to_int(row.get("num_unq")),
        "total_secs": _to_float(row.get("total_secs")),
    }


def to_value_bytes(payload: Dict[str, Any]) -> bytes:
    return _json_bytes(payload)
