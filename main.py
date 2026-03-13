from data import documents
from text_processor import process_text
from embeddings import get_embedding


def main():
    entries = documents
    cleaned_entries = []

    for d in entries:
        title = d.get("title")
        excerpt = d.get("excerpt")
        content = d.get("content")

        semantic_text = process_text(title, excerpt, content)
        semantic_text_embedding = get_embedding(semantic_text)

        new_entry = {
            **d,
            "semantic_text_embedding": semantic_text_embedding,
        }

        cleaned_entries.append(new_entry)

    return cleaned_entries

main()