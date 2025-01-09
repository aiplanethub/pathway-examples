from pathway.xpacks.llm.vector_store import VectorStoreClient
from pprint import pprint

PATHWAY_PORT = 8765
PATHWAY_HOST = "localhost"

client = VectorStoreClient(
    host=PATHWAY_HOST,
    port=PATHWAY_PORT,
)

query = input("Enter your question:\n").strip()

print("Asking question:", query)
docs = client(query)
pprint(docs)
