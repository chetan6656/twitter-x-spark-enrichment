import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parents[1] / "spark"))

from prepare_queries import build_queries
from scoring import candidates_from_payload, final_fields
from sqs_producer import QueryJournal, publish_rows, stable_query_id


def test_two_physical_queries_for_valid_contact():
    queries = build_queries("Jane", "Doe", "Example Inc", "Director", "New York")
    assert len(queries) == 2
    assert {q["query_number"] for q in queries} == {1, 2}
    assert len({q["query"] for q in queries}) == 2


def test_missing_name_is_not_a_valid_contact():
    assert build_queries("", "", "Example Inc", "Director", "New York") == []


def test_query_id_is_stable():
    assert stable_query_id("c1", 1, "query") == stable_query_id("c1", 1, "query")
    assert stable_query_id("c1", 1, "query") != stable_query_id("c1", 2, "query")


def test_company_profile_is_not_trusted():
    row = {"ic_fname": "Jane", "ic_lname": "Doe", "ic_company": "Example Inc", "Location": "New York"}
    payload = {"organic": [{
        "link": "https://x.com/exampleinc",
        "title": "Example Inc on X",
        "snippet": "Example Inc company account",
    }]}
    candidates = candidates_from_payload(row, payload, "company query")
    assert candidates[0]["confidence"] == "low"
    assert final_fields(candidates[0])["twitter_handle_final"] == ""


def test_sqs_journal_prevents_duplicate_publish(tmp_path):
    class FakeSQS:
        def __init__(self):
            self.calls = []

        def send_message_batch(self, **kwargs):
            self.calls.append(kwargs)
            return {"Successful": kwargs["Entries"], "Failed": []}

    rows = [{"contact_id": "c1", "query_number": "1", "query": "q1"}]
    sqs = FakeSQS()
    journal = QueryJournal(str(tmp_path / "journal.sqlite3"))
    assert publish_rows(rows, sqs, "queue", journal) == (1, 0)
    assert publish_rows(rows, sqs, "queue", journal) == (0, 1)
    assert len(sqs.calls) == 1


def test_final_output_schema():
    fields = final_fields({"username": "JaneDoe", "confidence": "high", "evidence": "e"})
    assert set(fields) == {
        "twitter_username_final", "twitter_handle_final", "twitter_profile_url_final",
        "twitter_confidence", "twitter_verified", "twitter_match_evidence",
        "twitter_matched_query", "twitter_display_name", "twitter_followers",
    }
