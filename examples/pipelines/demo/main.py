
import pathway as pw
import os

print(os.path.join(os.getcwd(), "credentials.json"))

files_table = pw.io.gdrive.read(
  # "1rlL3jHAAxyhninhQG9CkZvgBd5HsbU1j",
  "1fTeI450eEPwhFmInrGnlNFA4g9GNpMOE",
  mode="static",
  service_user_credentials_file=os.path.join(os.getcwd(), "credentials.json"),
  with_metadata=True,
)

pw.io.jsonlines.write(files_table, "output.jsonl")
pw.run()