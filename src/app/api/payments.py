from uuid import UUID

from fastapi import APIRouter, HTTPException, status

from app.api.deps import IdempotencyKey, SessionDep
from app.api.schemas import ErrorResponse, PaymentAccepted, PaymentCreate, PaymentDetail
from app.services import payments as service

router = APIRouter(prefix="/payments", tags=["payments"])


@router.post(
    "",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=PaymentAccepted,
    summary="Создать платёж",
    description="Принимает платёж в обработку и сразу отвечает 202. Итог платежа приходит "
    "webhook'ом на `webhook_url`; его же можно узнать через `GET /payments/{payment_id}`.",
    responses={
        status.HTTP_202_ACCEPTED: {"description": "Платёж принят или уже был создан с этим ключом и телом."},
        status.HTTP_409_CONFLICT: {
            "model": ErrorResponse,
            "description": "Idempotency-Key уже использован с другим телом запроса.",
        },
    },
)
async def create_payment(body: PaymentCreate, idempotency_key: IdempotencyKey, session: SessionDep) -> PaymentAccepted:
    try:
        payment = await service.create_payment(
            session,
            service.NewPayment(
                amount=body.amount,
                currency=body.currency,
                webhook_url=str(body.webhook_url),
                description=body.description,
                metadata=body.metadata,
            ),
            idempotency_key,
        )
    except service.IdempotencyConflict as exc:
        raise HTTPException(status.HTTP_409_CONFLICT, detail=str(exc)) from exc
    return PaymentAccepted.from_payment(payment)


@router.get(
    "/{payment_id}",
    response_model=PaymentDetail,
    summary="Получить платёж",
    description="Текущее состояние платежа: статус, время обработки и доставки webhook.",
    response_description="Платёж найден.",
    responses={status.HTTP_404_NOT_FOUND: {"model": ErrorResponse, "description": "Платёж не найден."}},
)
async def get_payment(payment_id: UUID, session: SessionDep) -> PaymentDetail:
    payment = await service.get_payment(session, payment_id)
    if payment is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, detail="Платёж не найден")
    return PaymentDetail.from_payment(payment)
