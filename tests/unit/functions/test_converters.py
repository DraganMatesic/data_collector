from data_collector.utilities.functions.converters import object_to_dict, to_none


class ObjWithDict:
    def __init__(self) -> None:
        self.visible = "value"
        self._private = "hidden"
        self.callable_attr = lambda: "ignore"


class ObjWithSlots:
    __slots__ = ("name", "_secret", "counter")

    def __init__(self) -> None:
        self.name = "slot"
        self._secret = "hidden"
        self.counter = 3


def test_to_none_handles_known_null_like_values() -> None:
    assert to_none(None) is None
    assert to_none("None") is None
    assert to_none("NaN") is None
    assert to_none("nat") is None
    assert to_none("value") == "value"


def test_object_to_dict_for_mapping() -> None:
    payload = {"valid": 1, "_private": 2}
    assert object_to_dict(payload) == {"valid": 1}


def test_object_to_dict_for_object_with___dict__() -> None:
    obj = ObjWithDict()
    assert object_to_dict(obj) == {"visible": "value"}


def test_object_to_dict_for_object_with___slots__() -> None:
    obj = ObjWithSlots()
    assert object_to_dict(obj) == {"name": "slot", "counter": 3}
