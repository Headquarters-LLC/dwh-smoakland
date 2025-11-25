from datetime import date
from src.accounting.models import Deposit, DepositLine, Expense, ExpenseLine
from src.accounting.services import export_deposits, export_expenses


class _FakeExporter:
    def __init__(self):
        self.calls = []

    def send_deposits(self, client_id, deposits, **kwargs):
        self.calls.append({"type": "deposit", "client_id": client_id, "deposits": list(deposits), **kwargs})
        return [{"ok": True, "count": len(deposits)}]

    def send_expenses(self, client_id, expenses, **kwargs):
        self.calls.append({"type": "expense", "client_id": client_id, "expenses": list(expenses), **kwargs})
        return [{"ok": True, "count": len(expenses)}]


def _mk_deposit(txn_id: str, amount: float) -> Deposit:
    return Deposit(
        txn_id=txn_id,
        date=date(2025, 1, 1),
        deposit_to_account="Bank",
        doc_number=txn_id,
        lines=[DepositLine(description="d", amount=amount, account_or_item="Income")],
    )


def _mk_expense(txn_id: str, amount: float) -> Expense:
    return Expense(
        txn_id=txn_id,
        date=date(2025, 1, 1),
        vendor="Vendor",
        bank_account="Bank",
        doc_number=txn_id,
        lines=[ExpenseLine(description="e", amount=amount, expense_account="Expense:Ops")],
    )


def test_export_deposits_chunks_and_keys():
    fake = _FakeExporter()
    deposits = [_mk_deposit(f"d{i}", 10.0 + i) for i in range(3)]
    res = export_deposits(deposits, fake, client_id="realm", batch_size=2)
    assert len(fake.calls) == 2  # chunked 2 + 1
    assert all(call["environment"] == "sandbox" for call in fake.calls)
    assert fake.calls[0]["idempotency_keys"][0] == deposits[0].txn_id
    assert len(res) == 2


def test_export_expenses_chunks_and_keys():
    fake = _FakeExporter()
    expenses = [_mk_expense(f"e{i}", 5.0 + i) for i in range(4)]
    res = export_expenses(expenses, fake, client_id="realm", batch_size=3)
    assert len(fake.calls) == 2  # chunked 3 + 1
    assert fake.calls[0]["idempotency_keys"][1] == expenses[1].txn_id
    assert len(res) == 2
