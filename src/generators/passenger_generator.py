"""
passenger_generator.py — генератор пассажиров.

Создаёт реалистичных пассажиров с русскими именами через Faker.
~3% без email, ~30% с картой лояльности.
"""
from __future__ import annotations

import random
import uuid
from datetime import datetime, timezone

from faker import Faker

from src.generators.config import MISSING_EMAIL_RATE

faker_ru = Faker("ru_RU")
faker_ru.seed_instance(42)


def generate_passenger(created_at: datetime | None = None) -> dict:
    if created_at is None:
        created_at = datetime.now(timezone.utc)

    passenger_id = str(uuid.uuid4())

    # ~3% — пустой email (намеренная неидеальность)
    if random.random() < MISSING_EMAIL_RATE:
        email = None
    else:
        email = faker_ru.email()

    # ~30% — есть карта лояльности
    frequent_flyer_id = (
        f"FF{random.randint(1_000_000, 9_999_999)}"
        if random.random() < 0.30
        else None
    )

    return {
        "passenger_id": passenger_id,
        "first_name": faker_ru.first_name(),
        "last_name": faker_ru.last_name(),
        "email": email,
        "phone": faker_ru.phone_number(),
        "date_of_birth": faker_ru.date_of_birth(minimum_age=18, maximum_age=80),
        "frequent_flyer_id": frequent_flyer_id,
        "created_at": created_at,
    }


def generate_passengers(count: int, created_at: datetime | None = None) -> list[dict]:
    return [generate_passenger(created_at) for _ in range(count)]
