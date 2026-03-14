import os
import time
import requests
from dotenv import load_dotenv
from elastic_search import es_client as es, INDEX_NAME
from embeddings import get_embedding

load_dotenv()


# ── Result parser ─────────────────────────────────────────────────────────────

def parse_results(result: dict) -> list[dict]:
    return [
        {
            "score": hit.get("_score"),
            "title": hit["_source"].get("title"),
            "url": hit["_source"].get("url"),
            "excerpt": hit["_source"].get("excerpt"),
            "site_name": hit["_source"].get("site_name"),
            "language": hit["_source"].get("language"),
            # highlight snippets if present, fallback to excerpt
            "snippet": hit.get("highlight", {}).get("content") 
                       or hit.get("highlight", {}).get("excerpt")
                       or [hit["_source"].get("excerpt", "")]
        }
        for hit in result["hits"]["hits"]
    ]


# ── Search functions ──────────────────────────────────────────────────────────

def fulltext_search(
    query: str,
    *,
    language: str | None = None,
    site_name: str | None = None,
    size: int = 10,
) -> list[dict]:
    """BM25 full-text search across title, excerpt, and content."""

    must = [
        {
            "multi_match": {
                "query": query,
                "fields": ["title^3", "excerpt^2", "content"],
            }
        }
    ]

    filters = _build_filters(language=language, site_name=site_name)

    body: dict = {
        "size": size,
        "query": {
            "bool": {
                "must": must,
                **({"filter": filters} if filters else {}),
            }
        },
    }

    return parse_results(es.search(index=INDEX_NAME, body=body).body)


def semantic_search(
    query: str,
    *,
    k: int = 10,
    num_candidates: int = 100,
) -> list[dict]:
    """Pure vector / semantic search using cosine similarity."""
    query_vector = get_embedding(query)

    body = {
        "knn": {
            "field": "semantic_text_embedding",
            "query_vector": query_vector,
            "k": k,
            "num_candidates": num_candidates,
        }
    }

    return parse_results(es.search(index=INDEX_NAME, body=body).body)


def hybrid_search(
    query: str,
    *,
    language: str | None = None,
    site_name: str | None = None,
    k: int = 10,
    num_candidates: int = 100,
    rank_window_size: int = 50,
    rank_constant: int = 60,
) -> list[dict]:
    """Hybrid search: BM25 + vector via Reciprocal Rank Fusion (RRF)."""
    query_vector = get_embedding(query)

    filters = _build_filters(language=language, site_name=site_name)

    bm25_query: dict = {
        "multi_match": {
            "query": query,
            "fields": ["title^3", "excerpt^2", "content"],
        }
    }

    # wrap in bool+filter if filters are present
    if filters:
        bm25_query = {
            "bool": {
                "must": [bm25_query],
                "filter": filters,
            }
        }

    body = {
        "retriever": {
            "rrf": {
                "retrievers": [
                    {"standard": {"query": bm25_query}},
                    {
                        "knn": {
                            "field": "semantic_text_embedding",
                            "query_vector": query_vector,
                            "k": k,
                            "num_candidates": num_candidates,
                        }
                    },
                ],
                "rank_window_size": rank_window_size,
                "rank_constant": rank_constant,
            }
        }
    }

    return parse_results(es.search(index=INDEX_NAME, body=body).body)


# ── Internal helpers ──────────────────────────────────────────────────────────

def _build_filters(
    language: str | None,
    site_name: str | None,
) -> list[dict]:
    filters = []
    if language:
        filters.append({"term": {"language": language}})
    if site_name:
        filters.append({"term": {"site_name": site_name}})
    return filters


# ── Quick manual test ─────────────────────────────────────────────────────────

if __name__ == "__main__":
    import json

    query = "dog or cat what should i get as a pet"

    # print("\n── Full-text search ──────────────────────────────")
    # results = fulltext_search(query, language="en")
    # print(json.dumps(results, indent=2))

    # print("\n── Semantic search ───────────────────────────────")
    # results = semantic_search(query)
    # print(json.dumps(results, indent=2))

    print("\n── Hybrid search ─────────────────────────────────")
    results = hybrid_search(query, language="en")
    print(json.dumps(results, indent=2))