"""Create tables on every configured shard (idempotent). Run once before first start:

    python -m scripts.init_shards
"""
from app.db.shard_router import get_shard_registry


def main() -> None:
    registry = get_shard_registry()
    registry.create_all()
    print(f"Initialised {registry.shard_count} shard(s).")


if __name__ == "__main__":
    main()
