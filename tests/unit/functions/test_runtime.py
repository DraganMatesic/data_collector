from enum import Enum
from types import SimpleNamespace

from data_collector.utilities.functions.runtime import (
    bulk_hash,
    is_module_available,
    list_enum_values,
    make_hash,
    obj_diff,
)


class SampleEnum(Enum):
    ONE = 1
    TWO = 2


def test_is_module_available_for_known_and_unknown_module() -> None:
    assert is_module_available("sys") is True
    assert is_module_available("module_that_should_not_exist_for_tests") is False


def test_list_enum_values_returns_member_values_in_declaration_order() -> None:
    assert list_enum_values(SampleEnum) == [1, 2]


def test_make_hash_is_deterministic_with_case_and_spacing_normalization() -> None:
    row_a = {"name": "Acme   Corp", "city": "Zagreb"}
    row_b = {"name": " acme corp ", "city": "zagreb"}

    assert make_hash(row_a) == make_hash(row_b)


def test_make_hash_supports_on_keys_and_exclude_keys() -> None:
    source = {"name": "ACME", "city": "Zagreb", "country": "Croatia"}

    only_name = make_hash(source, on_keys=["name"])
    only_name_lower = make_hash({"name": "acme"}, on_keys=["name"])
    assert only_name == only_name_lower

    no_country = make_hash(source, exclude_keys=["country"])
    no_country_reference = make_hash({"name": "acme", "city": "zagreb"})
    assert no_country == no_country_reference


def test_bulk_hash_updates_dicts_inplace_by_default() -> None:
    rows = [
        {"id": 1, "name": "Alpha"},
        {"id": 2, "name": "Beta"},
    ]

    result = bulk_hash(rows)

    assert isinstance(result, list)
    assert len(result) == len(rows)
    assert result[0] is rows[0]
    assert result[1] is rows[1]
    assert all("sha" in row for row in rows)
    assert rows[0]["sha"] != rows[1]["sha"]


def test_obj_diff_with_single_and_composite_keys() -> None:
    existing = [
        SimpleNamespace(sha="1", company_id=10, record_id=100),
        SimpleNamespace(sha="2", company_id=10, record_id=200),
    ]
    incoming = [
        SimpleNamespace(sha="2", company_id=10, record_id=200),
        SimpleNamespace(sha="3", company_id=11, record_id=300),
    ]

    to_insert_single, to_remove_single = obj_diff(incoming, existing, compare_key="sha")
    assert [obj.sha for obj in to_insert_single] == ["3"]
    assert [obj.sha for obj in to_remove_single] == ["1"]

    to_insert_composite, to_remove_composite = obj_diff(
        incoming,
        existing,
        compare_key=("company_id", "record_id"),
    )
    assert [(obj.company_id, obj.record_id) for obj in to_insert_composite] == [(11, 300)]
    assert [(obj.company_id, obj.record_id) for obj in to_remove_composite] == [(10, 100)]
