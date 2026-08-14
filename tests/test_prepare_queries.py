from spark.prepare_queries import build_queries


def test_exactly_two_queries():
    result = build_queries(
        "Jane",
        "Doe",
        "Example Inc",
        "Director",
        "New York",
    )

    assert len(result) == 2
    assert result[0]["query"] != result[1]["query"]


def test_missing_name_returns_no_queries():
    result = build_queries("", "", "Example Inc", "Director", "New York")
    assert result == []
