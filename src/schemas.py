"""Message models and Avro schema loading from .avsc files."""

from dataclasses import dataclass
from pathlib import Path


def _schemas_dir() -> Path:
    """Directory with .avsc files (hw1/schemas/)."""
    return Path(__file__).resolve().parent.parent / "schemas"


def load_schema_from_avsc(name: str) -> str:
    """Load an Avro schema from the file schemas/<name>.avsc.

    :param name: Schema name without extension (for example, "user").
    :returns: Contents of the .avsc file as a string.
    """
    path = _schemas_dir() / f"{name}.avsc"
    return path.read_text().strip()


# User schema — loaded from user.avsc
USER_SCHEMA_STR = load_schema_from_avsc("user")


@dataclass(slots=True)
class User:
    """User message model, corresponds to the user.avsc schema."""

    name: str

    def to_dict(self) -> dict:
        """Dictionary for Avro serialization (matches schema fields)."""
        return {"name": self.name}
