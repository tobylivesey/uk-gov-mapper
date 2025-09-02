from pathlib import Path
from bs4 import BeautifulSoup
import json

def html_to_text(html: str | None) -> str:
    soup = BeautifulSoup(html or "", "html.parser")
    return soup.get_text("\n").strip()

def write_ndjson(row: dict, fname: Path) -> None:
    fname.parent.mkdir(parents=True, exist_ok=True)
    with fname.open("a", encoding="utf-8") as f:
        f.write(json.dumps(row, ensure_ascii=False) + "\n")