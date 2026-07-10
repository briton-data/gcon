from pathlib import Path

FILES = [
    "agent.py",
    "receipt.py",
    "verifier.py",
]

for filename in FILES:
    path = Path(filename)

    # Read as bytes and decode safely
    raw = path.read_bytes()
    text = raw.decode("utf-8", errors="replace")

    print(f"Processing {filename}...")

    # Replace common escaped sequences
    repaired = (
        text
        .replace("\\r\\n", "\n")
        .replace("\\n", "\n")
        .replace("\\t", "\t")
        .replace("\\\"", "\"")
        .replace("\\'", "'")
    )

    # Remove surrounding quotes only if the entire file is quoted
    if repaired.startswith('"') and repaired.endswith('"'):
        repaired = repaired[1:-1]

    path.write_text(repaired, encoding="utf-8", newline="\n")

    print(f"Repaired {filename}: {repaired.count(chr(10)) + 1} lines")