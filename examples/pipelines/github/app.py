import dotenv
import os
import sys

import pathway as pw
from pathway.io.http import PathwayWebserver

if not dotenv.load_dotenv():
    print("Couldn't load .env")
    sys.exit(1)

host = os.environ.get("HOST", "localhost")
port = int(os.environ.get("PORT", "8000"))
webserver = PathwayWebserver(host=host, port=port)

commits_table = pw.io.airbyte.read(
    config_file_path="config.yaml",
    streams=["commits"],
    enforce_method="pypi",
    mode="static",
)

pw.io.csv.write(commits_table, "commits.csv")

persistence_config = pw.persistence.Config(
    backend=pw.persistence.Backend.filesystem("./Cache")
)

pw.run(
    monitoring_level=pw.MonitoringLevel.NONE,
    persistence_config=persistence_config,
)
