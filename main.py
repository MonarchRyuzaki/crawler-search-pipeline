from data import documents
from text_processor import process_text


def main():
    entries = documents
    cleaned_entries = []

    for d in entries:
        title = d.get("title")
        excerpt = d.get("excerpt")
        content = d.get("content")

        semantic_text = process_text(title, excerpt, content)

        print(semantic_text)
        print()

        new_entry = {
            **d,
            "semantic_text": semantic_text,
        }

        cleaned_entries.append(new_entry)

    return cleaned_entries

main()