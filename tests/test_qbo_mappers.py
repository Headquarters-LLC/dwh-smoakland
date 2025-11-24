from datetime import date
from src.accounting.models import Deposit, DepositLine, Expense, ExpenseLine
from src.integrations.qbo_gateway.mappers import deposit_to_payload, expense_to_payload


def test_deposit_mapper_payload():
    deposit = Deposit(
        txn_id="tx1",
        date=date(2025, 1, 10),
        deposit_to_account="Bank 1234",
        doc_number="tx1",
        lines=[
            DepositLine(
                description="Main desc - extra",
                amount=125.55,
                account_or_item="Revenue:Sub",
                entity_name="Customer A",
                entity_type="customer",
            )
        ],
        private_note="note-here",
    )
    payload = deposit_to_payload(deposit)
    assert payload["deposit_to_account"] == "Bank 1234"
    assert payload["doc_number"] == "tx1"
    assert payload["private_note"] == "note-here"
    assert payload["lines"][0]["description"] == "Main desc - extra"
    assert payload["lines"][0]["account_or_item"] == "Revenue:Sub"
    assert payload["lines"][0]["entity_name"] == "Customer A"
    assert payload["lines"][0]["entity_type"] == "customer"
    assert "class" not in payload["lines"][0]
    assert "linked_doc" not in payload["lines"][0]
    assert "txn_id" not in payload


def test_expense_mapper_payload():
    expense = Expense(
        txn_id="ex1",
        date=date(2025, 2, 2),
        vendor="Vendor X",
        bank_account="TPH 5597",
        doc_number="ex1",
        lines=[
            ExpenseLine(
                expense_account="Expenses:Office",
                description="Desk purchase",
                amount=250.0,
            )
        ],
        private_note="desk on floor 2",
    )
    payload = expense_to_payload(expense)
    assert payload["vendor"] == "Vendor X"
    assert payload["bank_account"] == "TPH 5597"
    assert payload["doc_number"] == "ex1"
    assert payload["lines"][0]["expense_account"] == "Expenses:Office"
    assert payload["lines"][0]["amount"] == 250.0
    assert "class" not in payload["lines"][0]
    assert "txn_id" not in payload


def test_expense_mapper_with_class():
    expense = Expense(
        txn_id="ex2",
        date=date(2025, 2, 3),
        vendor="Vendor Y",
        bank_account="TPH 5597",
        doc_number="ex2",
        lines=[
            ExpenseLine(
                expense_account="Expenses:Ops",
                description="Desk chair",
                amount=300.0,
                class_name="Ops",
            )
        ],
        private_note="chair",
    )
    payload = expense_to_payload(expense)
    assert payload["lines"][0]["class"] == "Ops"
