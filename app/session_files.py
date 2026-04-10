import re
from datetime import datetime
from pathlib import Path


SESSION_FILE_RE = re.compile(r"^[A-Za-z0-9_-]+\.json$")
RESERVED_PUBLIC_SLUGS = {"admin", "api", "manager", "static", "viewer"}


def slugify_session_name(name: str) -> str:
    slug = re.sub(r"[^A-Za-z0-9_-]+", "-", name.strip()).strip("-_").lower()
    if not slug:
        slug = datetime.utcnow().strftime("session-%Y%m%d-%H%M%S")
    return slug


def resolve_named_directory(app, config_key: str, default_dir: str) -> Path:
    configured = Path(app.config.get(config_key, default_dir))
    if configured.is_absolute():
        base = configured
    else:
        base = Path(app.root_path).parent / configured
    base.mkdir(parents=True, exist_ok=True)
    return base


def resolve_session_file(base_dir: Path, filename: str) -> Path:
    if not SESSION_FILE_RE.fullmatch(filename):
        raise ValueError("Invalid session filename")
    return base_dir / filename