from dotenv import load_dotenv
load_dotenv()
import os, requests

url = "https://router.huggingface.co/hf-inference/models/sentence-transformers/all-MiniLM-L6-v2/pipeline/feature-extraction"
headers = {
    "Authorization": f"Bearer {os.environ.get('HF_TOKEN', '')}",
    "Content-Type": "application/json",
}


def get_embedding(text: str):
    """Return embedding vector for the given text using Hugging Face Inference API."""
    payload = {"inputs": text}
    resp = requests.post(url, headers=headers, json=payload)

    if resp.status_code != 200:
        print("Request failed:", resp.status_code)
        print("Response body:", resp.text)
        raise SystemExit(1)

    return resp.json()


if __name__ == "__main__":
    # Example usage for manual testing
    example_text = "Your text here for embedding generation"
    embedding = get_embedding(example_text)
    print(embedding)  # should be a vector of size 384