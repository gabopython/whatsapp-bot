#!/usr/bin/env python3
import argparse
import asyncio
from typing import Final

from sqlalchemy import delete, select

from app import Conversation, SessionLocal, init_db

DEFAULT_PHONE: Final[str] = "593962052098"


async def delete_conversation_by_phone(phone: str, *, dry_run: bool = False) -> int:
    await init_db()

    async with SessionLocal() as session:
        existing = await session.scalar(select(Conversation).where(Conversation.phone == phone))
        if existing is None:
            print(f"No conversation found for phone: {phone}")
            return 0

        if dry_run:
            print(f"Dry run: would delete conversation for phone: {phone}")
            return 1

        await session.execute(delete(Conversation).where(Conversation.phone == phone))
        await session.commit()
        print(f"Deleted conversation for phone: {phone}")
        return 1


def main() -> None:
    parser = argparse.ArgumentParser(description="Delete a conversation from the database by phone number")
    parser.add_argument("--phone", default=DEFAULT_PHONE, help=f"Phone number to remove (default: {DEFAULT_PHONE})")
    parser.add_argument("--dry-run", action="store_true", help="Only show what would be deleted")
    args = parser.parse_args()

    phone = args.phone.strip()
    if not phone:
        raise SystemExit("Phone number cannot be empty")

    result = asyncio.run(delete_conversation_by_phone(phone, dry_run=args.dry_run))
    if result == 0:
        raise SystemExit(0)


if __name__ == "__main__":
    main()
