"""Shared database configuration helpers.

All scripts should use the same environment variables:
- DB_HOST, DB_PORT, DB_NAME (or DB_DATABASE), DB_USER, DB_PASSWORD
Optionally:
- DATABASE_URL (overrides derived URL if set)

This module also optionally loads a local .env file when running outside Docker.

Notes for Azure Database for PostgreSQL:
- Azure typically *requires SSL*.
- If you see `no pg_hba.conf entry ... no encryption`, add `sslmode=require`.
"""

import os
from dataclasses import dataclass
from urllib.parse import quote_plus, urlparse, urlunparse, parse_qsl, urlencode


def _try_load_dotenv():
    """Best-effort load of .env for local runs.

    In Docker Compose, env vars are already injected, so this is a no-op.
    """

    try:
        from dotenv import load_dotenv  # type: ignore

        load_dotenv(override=False)
    except Exception:
        # Don't hard-fail if python-dotenv isn't installed.
        return


def _with_sslmode_require(url: str) -> str:
    """Ensure the SQLAlchemy/psycopg2 URL contains sslmode=require.

    Azure Database for PostgreSQL commonly rejects non-SSL connections with:
    `no pg_hba.conf entry ... no encryption`.
    """

    try:
        parsed = urlparse(url)
        # Only handle postgres urls
        if parsed.scheme not in {"postgresql", "postgres"}:
            return url

        q = dict(parse_qsl(parsed.query, keep_blank_values=True))
        q.setdefault("sslmode", os.getenv("DB_SSLMODE", "require"))
        new_query = urlencode(q)
        return urlunparse(parsed._replace(query=new_query))
    except Exception:
        # If parsing fails, don't break startup.
        return url


@dataclass(frozen=True)
class DbSettings:
    host: str
    port: int
    name: str
    user: str
    password: str
    sslmode: str = "require"

    @property
    def sqlalchemy_url(self) -> str:
        # quote_plus to keep special chars in credentials safe inside URLs
        user = quote_plus(self.user)
        password = quote_plus(self.password)
        return _with_sslmode_require(
            f"postgresql://{user}:{password}@{self.host}:{self.port}/{self.name}"
        )


def get_db_settings() -> DbSettings:
    _try_load_dotenv()

    host = os.getenv("DB_HOST", "localhost")
    port = int(os.getenv("DB_PORT", "5432"))

    # Support both DB_NAME (preferred) and DB_DATABASE (seen in some envs)
    name = os.getenv("DB_NAME") or os.getenv("DB_DATABASE") or "weather_db"

    user = os.getenv("DB_USER", "admin")
    password = os.getenv("DB_PASSWORD", "password")
    sslmode = os.getenv("DB_SSLMODE", "require")

    return DbSettings(host=host, port=port, name=name, user=user, password=password, sslmode=sslmode)


def get_database_url() -> str:
    _try_load_dotenv()

    url = os.getenv("DATABASE_URL")
    if url:
        # Make sure user-provided URLs are also forced to SSL unless explicitly set
        return _with_sslmode_require(url)

    return get_db_settings().sqlalchemy_url


def get_psycopg2_connect_kwargs() -> dict:
    s = get_db_settings()
    return {
        "host": s.host,
        "port": str(s.port),
        "database": s.name,
        "user": s.user,
        "password": s.password,
        "sslmode": s.sslmode,
    }
