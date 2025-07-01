import csv
import re

import beangulp

from beancount.core import amount, data
from beancount.core.number import D
from beangulp import Importer
from dateutil.parser import parse


class SwisscardImporter(Importer):
    """An importer for Swisscard's cashback CSV files."""

    def __init__(self, filepattern: str, account: data.Account):
        self._filepattern = filepattern
        self._account = account

    def name(self) -> str:
        return super().name() + self._account

    def identify(self, filepath: str) -> bool:
        return re.search(self._filepattern, filepath) is not None

    def account(self, filepath: str) -> data.Account:
        return self._account

    def extract(self, filepath: str, existing: data.Entries) -> data.Entries:
        entries = []
        with open(filepath) as csvfile:
            reader = csv.DictReader(
                csvfile,
                delimiter=",",
                skipinitialspace=True,
            )
            for row in reader:
                book_date = parse(row["Transaction date"].strip(), dayfirst=True).date()
                currency = row["Currency"]
                value = D(row["Amount"])
                asset_amount = amount.Amount(-value, currency)   # Negative (assets decrease)
                expense_amount = amount.Amount(value, currency)   # Positive (expenses increase)
                metakv = {
                    "merchant": row["Merchant Category"],
                    "category": row["Registered Category"],
                }
                meta = data.new_metadata(filepath, 0, metakv)
                description = row["Description"].strip()
                entry = data.Transaction(
                    meta,
                    book_date,
                    "*",
                    "",
                    description,
                    data.EMPTY_SET,
                    data.EMPTY_SET,
                    [
                        data.Posting(self._account, asset_amount, None, None, None, None),
                        data.Posting("Expenses:FIXME", expense_amount, None, None, None, None),
                    ],
                )
                entries.append(entry)

        return entries
    
def get_importer(account):
    return SwisscardImporter("swisscard/.*\.csv", account)


if __name__ == "__main__":
    ingest = beangulp.Ingest([get_importer("Assets:CashBackCard:Cash")], [])
    ingest()