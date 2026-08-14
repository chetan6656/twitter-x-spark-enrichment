# Twitter/X Contact Enrichment

Production-oriented contact enrichment for discovering trusted Twitter/X handles using Zyte Search.

## Overview

This project searches for contact-level Twitter/X profiles using contact name, company, title, location, and available LinkedIn context. Results are scored, cached, verified where configured, and exported with confidence and evidence fields.

## Capabilities

- Exactly 2 search queries per contact
- Zyte Search API integration
- Persistent response cache
- Retry and timeout handling
- Checkpoint and resume support
- Candidate confidence scoring
- Twitter/X profile extraction
- High-confidence deliverable generation
- Shard processing and output merging

## Processing Flow

1. Load contact records.
2. Build two Twitter/X queries per contact.
3. Submit searches through Zyte.
4. Cache successful responses.
5. Extract candidate profile URLs.
6. Score candidates using contact and search-result evidence.
7. Verify candidates where configured.
8. Export trusted handles and QA fields.

## Local Usage

py -3 src\run_pipeline.py --input data\contacts.csv --limit 100 --no-excel --queries-per-contact 2 --workers 4

## AWS and Spark Direction

For large-scale processing, Spark prepares and scores the data while a centralized worker service controls Zyte requests through one global rate limiter. The rate limit is cluster-wide, not per Spark executor, and must be approved by the Zyte account.

Workflow: Contacts from RDS or S3 -> Spark query preparation -> global limiter -> Zyte Search API -> S3 cache and checkpoints -> Spark scoring -> final deliverable

## Security

- Store ZYTE_API_KEY in a local .env file or AWS Secrets Manager.
- Never commit API keys, contact data, response caches, or generated deliverables.
- Use .env.example as the configuration template.

## Repository Structure

- src: enrichment, verification, filtering, and merge scripts
- spark: distributed processing jobs
- config: runtime configuration
- tests: validation tests
- docs: technical documentation

## Status

The local Twitter/X enrichment pipeline is implemented with caching, checkpointing, scoring, and resume support. Spark execution, centralized rate limiting, and cloud storage integration require a controlled test before production-scale processing.
