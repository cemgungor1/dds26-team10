import time
from typing import Any


class SagaEngine:
    def __init__(self, storage, dispatcher, workflow, *, timeout_s: float = 10.0) -> None:
        self.storage = storage
        self.dispatcher = dispatcher
        self.workflow = workflow
        self.timeout_s = timeout_s

    def start_transaction(self, tx_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        tx = self.workflow.build_initial_tx(tx_id, payload)

        if not self.storage.acquire_lock(tx_id, tx["state"]["lock_token"]):
            existing = self.storage.get_tx(tx_id)
            if existing and existing.get("status") not in ("completed", "failed"):
                raise ValueError("Transaction already in progress")

        self.storage.set_tx(tx)
        self.storage.append_log(tx_id, {"action": "START_TRANSACTION", "payload": payload})

        command = self.workflow.reserve_stock_command(tx)
        self.storage.append_log(tx_id, {"action": "SEND_RESERVE_STOCK", "command": command})

        if not self.dispatcher.send_with_retry(self.workflow.stock_command_topic, command):
            self._finish_failed(tx, "Failed to start transaction")
            raise ValueError("Failed to start transaction")

        return tx

    def handle_event(self, event: dict[str, Any]) -> None:
        tx_id = event.get("transaction_id") or event.get("saga_id") or event.get("order_id")
        if not tx_id:
            return

        tx = self.storage.get_tx(tx_id)
        if not tx:
            return
        if tx.get("status") in ("completed", "failed"):
            return

        event_attempt = event.get("attempt_id")
        tx_attempt = tx["state"].get("attempt_id")
        if event_attempt and tx_attempt and event_attempt != tx_attempt:
            return

        event_type = event.get("type")
        if event_type == "StockReserved":
            self._handle_stock_reserved(tx, event)
        elif event_type == "PaymentCharged":
            self._handle_payment_charged(tx, event)
        elif event_type == "PaymentRolledBack":
            self._handle_payment_rolled_back(tx, event)
        elif event_type == "StockRolledBack":
            self._handle_stock_rolled_back(tx, event)

    def cancel_transaction(self, tx_id: str, reason: str) -> None:
        tx = self.storage.get_tx(tx_id)
        if not tx or tx.get("status") in ("completed", "failed"):
            return

        step = tx.get("step")
        if step == "reserve_stock":
            self._transition_to_rollback_stock(tx, reason)
        elif step == "charge_payment":
            self._transition_to_rollback_payment(tx, reason)
        elif step in ("rollback_payment", "rollback_stock"):
            return
        else:
            self._finish_failed(tx, reason)

    def recover_transaction(self, tx: dict[str, Any]) -> None:
        step = tx.get("step")
        self.storage.refresh_lock(tx["transaction_id"], tx["state"].get("lock_token"))

        if step == "reserve_stock":
            cmd = self.workflow.reserve_stock_command(tx)
            self.dispatcher.send_with_retry(self.workflow.stock_command_topic, cmd)
        elif step == "charge_payment":
            cmd = self.workflow.charge_payment_command(tx)
            self.dispatcher.send_with_retry(self.workflow.payment_command_topic, cmd)
        elif step == "rollback_payment":
            cmd = self.workflow.rollback_payment_command(tx)
            self.dispatcher.send_with_retry(self.workflow.payment_command_topic, cmd)
        elif step == "rollback_stock":
            cmd = self.workflow.rollback_stock_command(tx)
            self.dispatcher.send_with_retry(self.workflow.stock_command_topic, cmd)

        tx["last_updated"] = time.time()
        self.storage.set_tx(tx)

    def _handle_stock_reserved(self, tx: dict[str, Any], event: dict[str, Any]) -> None:
        if tx.get("step") != "reserve_stock":
            return

        self.storage.append_log(tx["transaction_id"], {"action": "RECV_STOCK_RESERVED", "event": event})

        if not event.get("success", False):
            self._finish_failed(tx, event.get("reason", "Stock reservation failed"))
            return

        tx["step"] = "charge_payment"
        self.storage.set_tx(tx)

        cmd = self.workflow.charge_payment_command(tx)
        self.storage.append_log(tx["transaction_id"], {"action": "SEND_CHARGE_PAYMENT", "command": cmd})
        if not self.dispatcher.send_with_retry(self.workflow.payment_command_topic, cmd):
            self._transition_to_rollback_stock(tx, "Failed to send payment command")

    def _handle_payment_charged(self, tx: dict[str, Any], event: dict[str, Any]) -> None:
        if tx.get("step") != "charge_payment":
            return

        self.storage.append_log(tx["transaction_id"], {"action": "RECV_PAYMENT_CHARGED", "event": event})

        if not event.get("success", False):
            self._transition_to_rollback_stock(tx, event.get("reason", "Payment failed"))
            return

        self._finish_completed(tx)

    def _handle_payment_rolled_back(self, tx: dict[str, Any], event: dict[str, Any]) -> None:
        if tx.get("step") != "rollback_payment":
            return

        self.storage.append_log(tx["transaction_id"], {"action": "RECV_PAYMENT_ROLLED_BACK", "event": event})

        if not event.get("success", False):
            cmd = self.workflow.rollback_payment_command(tx)
            self.dispatcher.send_with_retry(self.workflow.payment_command_topic, cmd)
            return

        self._transition_to_rollback_stock(tx, tx.get("reason", "Rollback payment succeeded"))

    def _handle_stock_rolled_back(self, tx: dict[str, Any], event: dict[str, Any]) -> None:
        if tx.get("step") != "rollback_stock":
            return

        self.storage.append_log(tx["transaction_id"], {"action": "RECV_STOCK_ROLLED_BACK", "event": event})

        if not event.get("success", False):
            cmd = self.workflow.rollback_stock_command(tx)
            self.dispatcher.send_with_retry(self.workflow.stock_command_topic, cmd)
            return

        self._finish_failed(tx, tx.get("reason", "Compensated and failed"))

    def _transition_to_rollback_payment(self, tx: dict[str, Any], reason: str) -> None:
        tx["status"] = "compensating"
        tx["step"] = "rollback_payment"
        tx["reason"] = reason
        self.storage.set_tx(tx)

        cmd = self.workflow.rollback_payment_command(tx)
        self.storage.append_log(tx["transaction_id"], {"action": "SEND_ROLLBACK_PAYMENT", "command": cmd})
        self.dispatcher.send_with_retry(self.workflow.payment_command_topic, cmd)

    def _transition_to_rollback_stock(self, tx: dict[str, Any], reason: str) -> None:
        tx["status"] = "compensating"
        tx["step"] = "rollback_stock"
        tx["reason"] = reason
        self.storage.set_tx(tx)

        cmd = self.workflow.rollback_stock_command(tx)
        self.storage.append_log(tx["transaction_id"], {"action": "SEND_ROLLBACK_STOCK", "command": cmd})
        self.dispatcher.send_with_retry(self.workflow.stock_command_topic, cmd)

    def _finish_completed(self, tx: dict[str, Any]) -> None:
        tx["status"] = "completed"
        tx["step"] = "done"
        self.storage.set_tx(tx)
        self.storage.append_log(tx["transaction_id"], {"action": "COMPLETE_TRANSACTION"})
        self.storage.release_lock(tx["transaction_id"], tx["state"].get("lock_token"))
        self.storage.notify_done(tx["transaction_id"], "completed")
        self.storage.expire_tx(tx["transaction_id"])

    def _finish_failed(self, tx: dict[str, Any], reason: str) -> None:
        tx["status"] = "failed"
        tx["step"] = "done"
        tx["reason"] = reason
        self.storage.set_tx(tx)
        self.storage.append_log(tx["transaction_id"], {"action": "FAIL_TRANSACTION", "reason": reason})
        self.storage.release_lock(tx["transaction_id"], tx["state"].get("lock_token"))
        self.storage.notify_done(tx["transaction_id"], f"failed:{reason}")
        self.storage.expire_tx(tx["transaction_id"])