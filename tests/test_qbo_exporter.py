from datetime import date
from src.accounting.models import Deposit, DepositLine, Expense, ExpenseLine
from src.integrations.qbo_gateway.exporter import QBOGatewayExporter


class _FakeClient:
    def __init__(self):
        self.calls = []

    def post(self, path, payload, headers=None, params=None):
        self.calls.append({"path": path, "payload": payload, "headers": headers or {}, "params": params or {}})
        return {"path": path, "ok": True}


def _dep(txn: str, amount: float) -> Deposit:
    return Deposit(
        txn_id=txn,
        date=date(2025, 3, 3),
        deposit_to_account="Bank 9999",
        doc_number=txn,
        lines=[DepositLine(description="deposit", amount=amount, account_or_item="Income")],
    )


def _exp(txn: str, amount: float) -> Expense:
    return Expense(
        txn_id=txn,
        date=date(2025, 4, 4),
        vendor="Vendor A",
        bank_account="BA",
        doc_number=txn,
        lines=[ExpenseLine(description="expense", amount=amount, expense_account="Expenses")],
    )


def test_exporter_builds_headers_and_params():
    fake = _FakeClient()
    exporter = QBOGatewayExporter(client=fake)
    d = _dep("d1", 10.0)
    exporter.send_deposits("client-1", [d], environment="sandbox", auto_create=True)
    call = fake.calls[0]
    assert call["path"] == "/qbo/client-1/deposits"
    assert "Idempotency-Key" in call["headers"]
    assert call["headers"]["Idempotency-Key"] == d.txn_id
    assert call["params"]["environment"] == "sandbox"
    assert call["params"]["auto_create"] == "true"


def test_exporter_expenses_path_and_headers():
    fake = _FakeClient()
    exporter = QBOGatewayExporter(client=fake)
    e = _exp("e1", 5.0)
    exporter.send_expenses("client-2", [e], environment="production", auto_create=False)
    call = fake.calls[0]
    assert call["path"] == "/qbo/client-2/expenses"
    assert call["headers"]["Idempotency-Key"] == e.txn_id
    assert call["params"]["environment"] == "production"
