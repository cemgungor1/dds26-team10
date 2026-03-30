import uuid
from collections import defaultdict
from typing import Any


class CheckoutWorkflow:
    name = "checkout"

    stock_command_topic = "stock-commands"
    payment_command_topic = "payment-commands"

    def normalize_items(self, raw_items: list[list | tuple]) -> list[dict[str, Any]]:
        counts: dict[str, int] = defaultdict(int)
        for item_id, quantity in raw_items:
            counts[str(item_id)] += int(quantity)
        return [{"item_id": item_id, "quantity": qty} for item_id, qty in counts.items()]

    def build_initial_tx(self, tx_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        return {
            "transaction_id": tx_id,
            "protocol": "saga",   # later: request-driven
            "workflow": self.name,
            "status": "started",
            "step": "reserve_stock",
            "payload": payload,
            "state": {
                "attempt_id": str(uuid.uuid4()),
                "lock_token": str(uuid.uuid4()),
                "stock_idem_key": str(uuid.uuid4()),
                "payment_idem_key": str(uuid.uuid4()),
            },
            "reason": "",
            "last_updated": 0.0,
        }

    def reserve_stock_command(self, tx: dict[str, Any]) -> dict[str, Any]:
        return {
            "message_id": str(uuid.uuid4()),
            "type": "ReserveStock",
            "transaction_id": tx["transaction_id"],
            "order_id": tx["payload"]["order_id"],
            "attempt_id": tx["state"]["attempt_id"],
            "items": tx["payload"]["items"],
            "idempotency_key": tx["state"]["stock_idem_key"],
        }

    def charge_payment_command(self, tx: dict[str, Any]) -> dict[str, Any]:
        return {
            "message_id": str(uuid.uuid4()),
            "type": "ChargePayment",
            "transaction_id": tx["transaction_id"],
            "order_id": tx["payload"]["order_id"],
            "attempt_id": tx["state"]["attempt_id"],
            "user_id": tx["payload"]["user_id"],
            "amount": tx["payload"]["total_cost"],
            "idempotency_key": tx["state"]["payment_idem_key"],
        }

    def rollback_payment_command(self, tx: dict[str, Any]) -> dict[str, Any]:
        return {
            "message_id": str(uuid.uuid4()),
            "type": "RollbackPayment",
            "transaction_id": tx["transaction_id"],
            "order_id": tx["payload"]["order_id"],
            "attempt_id": tx["state"]["attempt_id"],
            "user_id": tx["payload"]["user_id"],
            "amount": tx["payload"]["total_cost"],
            "idempotency_key": tx["state"]["payment_idem_key"],
        }

    def rollback_stock_command(self, tx: dict[str, Any]) -> dict[str, Any]:
        return {
            "message_id": str(uuid.uuid4()),
            "type": "RollbackStock",
            "transaction_id": tx["transaction_id"],
            "order_id": tx["payload"]["order_id"],
            "attempt_id": tx["state"]["attempt_id"],
            "items": tx["payload"]["items"],
            "idempotency_key": tx["state"]["stock_idem_key"],
        }