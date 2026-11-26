from __future__ import annotations

from decimal import Decimal

from pydantic import BaseModel, Field, field_validator

from app.money import to_minor


def _check_two_dp(v: Decimal) -> Decimal:
    to_minor(v)  # raises ValueError if more than 2 decimal places
    return v


class CreateAccountRequest(BaseModel):
    owner_name: str = Field(min_length=1, max_length=120)
    initial_balance: Decimal = Field(default=Decimal("0.00"), ge=0)
    currency: str = Field(default="INR", pattern=r"^[A-Z]{3}$")

    _dp = field_validator("initial_balance")(_check_two_dp)


class AccountResponse(BaseModel):
    id: str
    owner_name: str
    balance: str
    currency: str


class CreatePaymentRequest(BaseModel):
    debit_account_id: str
    credit_account_id: str
    amount: Decimal = Field(gt=0, description="Positive, at most 2 decimal places")
    currency: str = Field(default="INR", pattern=r"^[A-Z]{3}$")

    _dp = field_validator("amount")(_check_two_dp)


class PaymentResponse(BaseModel):
    id: str
    debit_account_id: str
    credit_account_id: str
    amount: str
    currency: str
    state: str
    failure_reason: str | None = None
    replayed: bool = False
