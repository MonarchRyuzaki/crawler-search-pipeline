from elasticsearch import Elasticsearch, helpers
from dotenv import load_dotenv
import os

load_dotenv()

INDEX_NAME = "crawl_documents"


class ElasticsearchClient:
    _instance: "ElasticsearchClient | None" = None
    _client: Elasticsearch

    def __new__(cls) -> "ElasticsearchClient":
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._client = Elasticsearch(
                api_key=os.environ.get("ELASTICSEARCH_API_KEY"),
                hosts="http://localhost:9200",
            )
        return cls._instance

    @property
    def client(self) -> Elasticsearch:
        return self._client

    def ping(self) -> bool:
        return self._client.ping()

    def get_index_mappings(self) -> dict:
        return {
            "properties": {
                "title": {
                    "type": "text",
                    "fields": {"keyword": {"type": "keyword"}},
                },
                "excerpt": {
                    "type": "text",
                    "fields": {"keyword": {"type": "keyword"}},
                },
                "site_name": {"type": "keyword"},
                "language": {"type": "keyword"},
                "byline": {
                    "type": "text",
                    "fields": {"keyword": {"type": "keyword"}},
                },
                "url": {"type": "keyword"},
                "crawled_at": {"type": "date"},
                "content_hash": {"type": "keyword"},
                "content": {"type": "text"},
                "semantic_text_embedding": {
                    "type": "dense_vector",
                    "dims": 384,
                    "index": True,
                    "similarity": "cosine",
                },
            }
        }

    def create_index_if_not_exists(self, index_name: str) -> None:
        if not self._client.indices.exists(index=index_name):
            self._client.indices.create(
                index=index_name,
                mappings=self.get_index_mappings()
            )
            print(f"Index '{index_name}' created.")
        else:
            print(f"Index '{index_name}' already exists.")

    def bulk_insert(
        self,
        index_name: str,
        documents: list[dict],
        refresh: bool | str | None = None,
    ) -> None:
        if not documents:
            return

        actions = []
        for doc in documents:
            action: dict = {"_index": index_name, "_source": doc}
            if "_id" in doc:
                action["_id"] = doc["_id"]
                action["_source"] = {k: v for k, v in doc.items() if k != "_id"}
            actions.append(action)

        try:
            helpers.bulk(self._client, actions, refresh=refresh)
            print(f"Inserted {len(actions)} documents into '{index_name}'.")
        except Exception as e:
            print(f"Bulk insert error: {e}")

    def ensure_index(self) -> None:
        if not self.ping():
            raise ConnectionError("Could not connect to Elasticsearch.")
        self.create_index_if_not_exists(INDEX_NAME)

    def search(self, **kwargs):
        return self._client.search(**kwargs)


es_client = ElasticsearchClient()
es_client.ensure_index()