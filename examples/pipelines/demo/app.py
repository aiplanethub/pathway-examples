import os
from pprint import pprint
from typing import Any

import dotenv
import pathway as pw
from pathway.xpacks.llm.embedders import GeminiEmbedder
from pathway.xpacks.llm.parsers import PypdfParser
from pathway.xpacks.llm.splitters import TokenCountSplitter
from pathway.xpacks.llm.vector_store import VectorStoreServer

dotenv.load_dotenv()

print(os.path.join(os.getcwd(), "credentials.json"))

PATHWAY_PORT = 8765
PATHWAY_HOST = "localhost"

data_sources: list[pw.Table[Any]] = []

table = pw.io.gdrive.read(
    mode="streaming",
    object_id="1rlL3jHAAxyhninhQG9CkZvgBd5HsbU1j",
    # object_id = "1fTeI450eEPwhFmInrGnlNFA4g9GNpMOE",
    service_user_credentials_file=os.path.join(os.getcwd(), "credentials.json"),
    with_metadata=True,
)


def on_change(key: str, row: dict[str, str], time: int, is_addition: bool):
    print(key, time, is_addition)
    print(type(row["data"]), row.keys())
    pprint(row["_metadata"])


pw.io.subscribe(
    table,
    on_change=on_change,
)

data_sources.append(table)

# Choose document transformers
text_splitter = TokenCountSplitter()
embedder = GeminiEmbedder(api_key=os.environ["GEMINI_API_KEY"])

vector_server = VectorStoreServer(
    *data_sources,
    embedder=embedder,
    splitter=text_splitter,
    parser=PypdfParser(),
)

# this server runs on a separate thread
server = vector_server.run_server(
    host=PATHWAY_HOST, port=PATHWAY_PORT, threaded=False, with_cache=False
)
