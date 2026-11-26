from __future__ import annotations

import uuid

from fastapi import APIRouter, Header, HTTPException, Request, Response
from sqlalchemy import select

from app.core.idempotency import IdempotencyKeyReuseError
from app.db.models import Account, Transaction
from app.money import from_minor, to_minor
from app.schemas.api import (
    AccountResponse,
    CreateAccountRequest,
    CreatePaymentRequest,
    PaymentResponse,
)
from app.services.payments import (
    PaymentRejectedError,
    TransactionNotFoundError,
    create_payment,
    get_payment,
)

router = APIRouter()


def _registry(request: Request):
    return request.app.state.registry


def _account_view(a: Account) -> AccountResponse:
    return AccountResponse(
        id=a.id, owner_name=a.owner_name, balance=str(from_minor(a.balance_minor)), currency=a.currency
    )


def _payment_view(t: Transaction, replayed: bool = False) -> PaymentResponse:
    return PaymentResponse(
        id=t.id,
        debit_account_id=t.debit_account_id,
        credit_account_id=t.credit_account_id,
        amount=str(from_minor(t.amount_minor)),
        currency=t.currency,
        state=t.state.value,
        failure_reason=t.failure_reason,
        replayed=replayed,
    )


@router.post("/accounts", response_model=AccountResponse, status_code=201, tags=["accounts"])
def create_account(body: CreateAccountRequest, request: Request):
    registry = _registry(request)
    # The id IS the shard key, so it must exist before we pick a shard to insert into.
    account = Account(
        id=str(uuid.uuid4()),
        owner_name=body.owner_name,
        balance_minor=to_minor(body.initial_balance),
        currency=body.currency,
    )
    with registry.session_for_account(account.id) as session:
        session.add(account)
    return _account_view(account)


@router.get("/accounts/{account_id}", response_model=AccountResponse, tags=["accounts"])
def read_account(account_id: str, request: Request):
    registry = _registry(request)
    with registry.session_for_account(account_id) as session:
        account = session.execute(select(Account).where(Account.id == account_id)).scalar_one_or_none()
    if account is None:
        raise HTTPException(404, "account not found")
    return _account_view(account)


@router.post("/payments", response_model=PaymentResponse, status_code=201, tags=["payments"])
def submit_payment(
    body: CreatePaymentRequest,
    request: Request,
    response: Response,
    idempotency_key: str = Header(..., alias="Idempotency-Key", min_length=1, max_length=128),
):
    if body.debit_account_id == body.credit_account_id:
        raise HTTPException(422, "debit and credit accounts must differ")
    try:
        txn, replayed = create_payment(
            _registry(request),
            idempotency_key=idempotency_key,
            debit_account_id=body.debit_account_id,
            credit_account_id=body.credit_account_id,
            amount_minor=to_minor(body.amount),
            currency=body.currency,
        )
    except PaymentRejectedError as exc:
        raise HTTPException(422, str(exc)) from exc
    except IdempotencyKeyReuseError as exc:
        raise HTTPException(409, str(exc)) from exc
    if replayed:
        response.status_code = 200  # same result as the first call, but not "created"
        response.headers["Idempotent-Replay"] = "true"
    return _payment_view(txn, replayed)


@router.get("/payments/{transaction_id}", response_model=PaymentResponse, tags=["payments"])
def read_payment(transaction_id: str, request: Request):
    try:
        return _payment_view(get_payment(_registry(request), transaction_id))
    except TransactionNotFoundError as exc:
        raise HTTPException(404, "transaction not found") from exc


@router.get("/healthz", tags=["ops"])
def healthz(request: Request):
    return {"status": "ok", "shards": _registry(request).shard_count}
