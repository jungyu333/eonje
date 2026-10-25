import json
from collections.abc import Callable

import pytest

from eonje.client.errors import ApiError, UnexpectedResponseError
from eonje.client.response import parse_response

Load = Callable[[str], str]


def _body(**fields: object) -> str:
    return json.dumps({"resultCode": "00", "resultMsg": "정상", **fields})


# --- normal pages -----------------------------------------------------------


def test_normal_page_returns_result_code_total_count_and_items(fixture_text: Load) -> None:
    # GIVEN a normal getChargerStatus page with four items
    text = fixture_text("getChargerStatus_page.json")

    # WHEN it is parsed
    parsed = parse_response(text)

    # THEN code, totalCount and all items are returned
    assert parsed.result_code == "00"
    assert parsed.total_count == 4
    assert len(parsed.items) == 4


def test_duplicate_observations_are_kept_as_received(fixture_text: Load) -> None:
    # GIVEN a page containing the same (statId, chgerId, statUpdDt) twice
    text = fixture_text("getChargerStatus_page.json")

    # WHEN it is parsed
    items = parse_response(text).items

    # THEN both rows survive; dedupe is a transform concern, not the client's
    keys = [(i["statId"], i["chgerId"], i["statUpdDt"]) for i in items]
    assert keys.count(("ME174013", "01", "20261009140512")) == 2


def test_unknown_status_codes_are_kept_unchanged(fixture_text: Load) -> None:
    # GIVEN a page with status "7" (undocumented) and "" (blank)
    text = fixture_text("getChargerStatus_page.json")

    # WHEN it is parsed
    stats = [i["stat"] for i in parse_response(text).items]

    # THEN the unknown codes are present as received strings
    assert "7" in stats
    assert "" in stats


def test_empty_optional_fields_are_kept_as_empty_strings(fixture_text: Load) -> None:
    # GIVEN an item whose lastTedt and nowTsdt are empty strings
    text = fixture_text("getChargerStatus_page.json")

    # WHEN it is parsed
    item = next(i for i in parse_response(text).items if i["statId"] == "PI795111")

    # THEN the fields are still present and still empty strings
    assert item["lastTedt"] == ""
    assert item["nowTsdt"] == ""


def test_undocumented_fields_are_kept(fixture_text: Load) -> None:
    # GIVEN an item carrying the undocumented "maker" field
    text = fixture_text("getChargerStatus_page.json")

    # WHEN it is parsed
    item = next(i for i in parse_response(text).items if i["statId"] == "PI795111")

    # THEN the field is passed through
    assert item["maker"] == "XX"


def test_single_item_object_is_wrapped_in_a_list(fixture_text: Load) -> None:
    # GIVEN a page where items.item is a single object instead of an array
    text = fixture_text("getChargerStatus_single_item.json")

    # WHEN it is parsed
    parsed = parse_response(text)

    # THEN items is a one-element list holding that object
    assert parsed.total_count == 1
    assert [i["statId"] for i in parsed.items] == ["ME174013"]


def test_parsing_the_same_text_twice_yields_equal_results(fixture_text: Load) -> None:
    # GIVEN a normal page
    text = fixture_text("getChargerStatus_page.json")

    # WHEN it is parsed twice
    first, second = parse_response(text), parse_response(text)

    # THEN the results are equal
    assert first == second


def test_numeric_string_total_count_is_converted_to_int() -> None:
    # GIVEN totalCount sent as a string
    text = _body(totalCount="13668", items="")

    # WHEN it is parsed
    parsed = parse_response(text)

    # THEN totalCount is an int
    assert parsed.total_count == 13668


# --- empty pages / NO_DATA --------------------------------------------------


def test_page_past_the_end_has_total_count_but_no_items(fixture_text: Load) -> None:
    # GIVEN a normal response whose items is "" (page beyond the last)
    text = fixture_text("getChargerStatus_empty_page.json")

    # WHEN it is parsed
    parsed = parse_response(text)

    # THEN items is empty but totalCount is preserved
    assert parsed.items == []
    assert parsed.total_count == 13668


@pytest.mark.parametrize("items", [None, {}, {"item": None}, {"item": []}])
def test_empty_items_shapes_yield_no_items(items: object) -> None:
    # GIVEN a normal response with an empty items container in some shape
    text = _body(totalCount=0, items=items)

    # WHEN it is parsed
    parsed = parse_response(text)

    # THEN no items are returned
    assert parsed.items == []


def test_missing_items_key_yields_no_items() -> None:
    # GIVEN a normal response without an items key
    text = _body(totalCount=0)

    # WHEN it is parsed
    parsed = parse_response(text)

    # THEN no items are returned
    assert parsed.items == []


def test_no_data_result_code_is_an_empty_page_not_an_error(fixture_text: Load) -> None:
    # GIVEN resultCode 03 (NODATA) without totalCount
    # (documented code; the live API answered an empty query with 00 + empty list instead,
    #  see real_no_data.json — this case guards the documented shape)
    text = fixture_text("getChargerStatus_no_data.json")

    # WHEN it is parsed
    parsed = parse_response(text)

    # THEN it is a zero-count, zero-item page with the code preserved
    assert parsed.result_code == "03"
    assert parsed.total_count == 0
    assert parsed.items == []


# --- API errors -------------------------------------------------------------


def test_non_normal_result_code_raises_api_error_with_code(fixture_text: Load) -> None:
    # GIVEN resultCode 22 (request limit exceeded)
    text = fixture_text("getChargerStatus_api_error.json")

    # WHEN it is parsed
    with pytest.raises(ApiError) as exc:
        parse_response(text)

    # THEN the error carries the result code and message
    assert exc.value.result_code == "22"
    assert "LIMITED_NUMBER_OF_SERVICE_REQUESTS_EXCEEDS_ERROR" in str(exc.value)


def test_missing_result_code_raises_api_error_with_empty_code() -> None:
    # GIVEN a JSON object without resultCode
    text = json.dumps({"totalCount": 1, "items": {"item": []}})

    # WHEN it is parsed
    with pytest.raises(ApiError) as exc:
        parse_response(text)

    # THEN it is an API error with an empty code
    assert exc.value.result_code == ""


# --- unexpected responses ---------------------------------------------------


def test_gateway_xml_error_raises_unexpected_response(fixture_text: Load) -> None:
    # GIVEN the data.go.kr gateway XML error body
    text = fixture_text("gateway_error.xml")

    # WHEN it is parsed
    # THEN it is reported as an unexpected response
    with pytest.raises(UnexpectedResponseError):
        parse_response(text)


def test_empty_body_raises_unexpected_response() -> None:
    # GIVEN an empty body
    # WHEN it is parsed
    # THEN it is reported as an unexpected response
    with pytest.raises(UnexpectedResponseError):
        parse_response("")


@pytest.mark.parametrize("text", ["[]", "null", '"00"', "3"])
def test_non_object_json_root_raises_unexpected_response(text: str) -> None:
    # GIVEN valid JSON whose root is not an object
    # WHEN it is parsed
    # THEN it is reported as an unexpected response
    with pytest.raises(UnexpectedResponseError):
        parse_response(text)


@pytest.mark.parametrize("total", [None, "abc", [1]])
def test_invalid_total_count_raises_unexpected_response(total: object) -> None:
    # GIVEN a normal response with an unusable totalCount
    text = _body(totalCount=total, items="")

    # WHEN it is parsed
    # THEN it is reported as an unexpected response
    with pytest.raises(UnexpectedResponseError):
        parse_response(text)


def test_missing_total_count_on_normal_response_raises_unexpected_response() -> None:
    # GIVEN a normal response without totalCount
    text = _body(items={"item": []})

    # WHEN it is parsed
    # THEN it is reported as an unexpected response
    with pytest.raises(UnexpectedResponseError):
        parse_response(text)


@pytest.mark.parametrize("items", ["x", [1, 2], 5])
def test_non_object_items_container_raises_unexpected_response(items: object) -> None:
    # GIVEN a normal response whose items is a non-empty non-object
    text = _body(totalCount=1, items=items)

    # WHEN it is parsed
    # THEN it is reported as an unexpected response
    with pytest.raises(UnexpectedResponseError):
        parse_response(text)


@pytest.mark.parametrize("item", ["x", 5, True])
def test_scalar_item_raises_unexpected_response(item: object) -> None:
    # GIVEN a normal response whose items.item is a scalar
    text = _body(totalCount=1, items={"item": item})

    # WHEN it is parsed
    # THEN it is reported as an unexpected response
    with pytest.raises(UnexpectedResponseError):
        parse_response(text)


# --- real API responses (tests/fixtures/evcharger/real_*.json) ----------------


def test_real_gateway_error_keeps_reason_code_and_message(fixture_text: Load) -> None:
    # GIVEN the real response for an unregistered service key (dataType=JSON)
    text = fixture_text("real_gateway_error.json")

    # WHEN it is parsed
    with pytest.raises(ApiError) as exc:
        parse_response(text)

    # THEN the gateway reason code and message survive for the call log
    assert exc.value.result_code == "30"
    assert "SERVICE_KEY_IS_NOT_REGISTERED_ERROR" in str(exc.value)


def test_real_status_page_returns_page_items_and_full_total_count(fixture_text: Load) -> None:
    # GIVEN a real getChargerStatus first page (numOfRows=10 of 16239)
    text = fixture_text("real_status_page.json")

    # WHEN it is parsed
    parsed = parse_response(text)

    # THEN the code is normal, totalCount is the full count and only this page's items return
    assert parsed.result_code == "00"
    assert parsed.total_count == 16239
    assert len(parsed.items) == 10


def test_real_status_items_are_passed_through_unchanged(fixture_text: Load) -> None:
    # GIVEN a real getChargerStatus page
    text = fixture_text("real_status_page.json")
    expected = json.loads(text)["items"]["item"]

    # WHEN it is parsed
    items = parse_response(text).items

    # THEN every item equals the received object: same keys, same string values
    assert items == expected


def test_real_status_items_keep_empty_now_tsdt_as_empty_string(fixture_text: Load) -> None:
    # GIVEN a real getChargerStatus page where no charger is currently charging
    text = fixture_text("real_status_page.json")

    # WHEN it is parsed
    items = parse_response(text).items

    # THEN nowTsdt is present on every item as an empty string, not dropped or None
    assert [i["nowTsdt"] for i in items] == [""] * 10


def test_real_status_items_keep_status_codes_as_strings(fixture_text: Load) -> None:
    # GIVEN a real getChargerStatus page with stat "2" and "5"
    text = fixture_text("real_status_page.json")

    # WHEN it is parsed
    stats = {i["stat"] for i in parse_response(text).items}

    # THEN status codes stay as received strings; mapping happens at aggregation time
    assert stats == {"2", "5"}


def test_real_status_page_keeps_each_charger_of_a_station(fixture_text: Load) -> None:
    # GIVEN a real page where station ME174050 has chargers 01..05
    text = fixture_text("real_status_page.json")

    # WHEN it is parsed
    items = parse_response(text).items

    # THEN all five chargers are present (same statId is not a duplicate)
    chargers = sorted(i["chgerId"] for i in items if i["statId"] == "ME174050")
    assert chargers == ["01", "02", "03", "04", "05"]


def test_parsing_real_status_page_twice_yields_equal_results(fixture_text: Load) -> None:
    # GIVEN a real getChargerStatus page
    text = fixture_text("real_status_page.json")

    # WHEN it is parsed twice
    first, second = parse_response(text), parse_response(text)

    # THEN the results are equal
    assert first == second


def test_real_info_page_returns_page_items_and_full_total_count(fixture_text: Load) -> None:
    # GIVEN a real getChargerInfo first page (numOfRows=10 of 76183)
    text = fixture_text("real_info_page.json")

    # WHEN it is parsed
    parsed = parse_response(text)

    # THEN the code is normal, totalCount is the full count and only this page's items return
    assert parsed.result_code == "00"
    assert parsed.total_count == 76183
    assert len(parsed.items) == 10


def test_real_info_items_keep_undocumented_maker_field(fixture_text: Load) -> None:
    # GIVEN a real getChargerInfo page carrying "maker", which the API docs do not list
    text = fixture_text("real_info_page.json")

    # WHEN it is parsed
    items = parse_response(text).items

    # THEN maker is present on every item with the received value
    assert {i["maker"] for i in items} == {"시그넷", "채비"}


def test_real_info_items_keep_every_received_field(fixture_text: Load) -> None:
    # GIVEN a real getChargerInfo page whose items carry 37 fields
    text = fixture_text("real_info_page.json")
    expected = json.loads(text)["items"]["item"]

    # WHEN it is parsed
    items = parse_response(text).items

    # THEN every item equals the received object, field for field
    assert items == expected
    assert all(len(i) == 37 for i in items)


def test_real_info_items_are_not_type_converted(fixture_text: Load) -> None:
    # GIVEN a real getChargerInfo item with numeric-looking fields
    text = fixture_text("real_info_page.json")

    # WHEN it is parsed
    item = next(i for i in parse_response(text).items if i["statId"] == "ME174013")

    # THEN lat/lng/output/year stay strings; conversion belongs to transform/
    assert item["lat"] == "37.476296"
    assert item["lng"] == "126.9583876"
    assert item["output"] == "50"
    assert item["year"] == "2017"


def test_real_info_items_keep_blank_and_whitespace_values_as_received(fixture_text: Load) -> None:
    # GIVEN real getChargerInfo items with empty powerType and a space-prefixed addrDetail
    text = fixture_text("real_info_page.json")

    # WHEN it is parsed
    items = {(i["statId"], i["chgerId"]): i for i in parse_response(text).items}

    # THEN empty strings and leading whitespace are not normalized
    assert items[("ME174013", "01")]["powerType"] == ""
    assert items[("ME174013", "01")]["nowTsdt"] == ""
    assert items[("ME174027", "01")]["addrDetail"] == " 1층 입구"


def test_parsing_real_info_page_twice_yields_equal_results(fixture_text: Load) -> None:
    # GIVEN a real getChargerInfo page
    text = fixture_text("real_info_page.json")

    # WHEN it is parsed twice
    first, second = parse_response(text), parse_response(text)

    # THEN the results are equal
    assert first == second


def test_real_no_data_is_a_normal_code_with_zero_count_and_no_items(fixture_text: Load) -> None:
    # GIVEN the real empty result: resultCode "00", totalCount 0, items.item []
    text = fixture_text("real_no_data.json")

    # WHEN it is parsed
    parsed = parse_response(text)

    # THEN it is a normal, empty page (the real API does not send "03" here)
    assert parsed.result_code == "00"
    assert parsed.total_count == 0
    assert parsed.items == []
