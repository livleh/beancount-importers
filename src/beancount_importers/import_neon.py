import csv
from datetime import datetime

# Beancount core components
from beancount.core import data, amount, flags
from beancount.core.number import Decimal

# Beangulp base importer
import beangulp

# Your specific categorization logic
from beancount_importers.bank_classifier import payee_to_account_mapping

UNCATEGORIZED_EXPENSES_ACCOUNT = "Expenses:FIXME"


class NeonImporter(beangulp.Importer):
    """
    Importer for Neon bank CSV files.
    It manually parses the semicolon-delimited CSV format.
    """
    
    def __init__(self, account, currency="CHF"):
        # FIX: Use self._account to avoid overwriting the account() method.
        self._account = account
        self.currency = currency

    def identify(self, filepath):
        """A simple but effective identification strategy."""
        return 'neon' in filepath.lower() and filepath.lower().endswith('.csv')

    def account(self, filepath):
        """Return the account associated with this importer."""
        # FIX: Return the stored internal variable.
        return self._account

    def extract(self, filepath, existing_entries=None):
        """
        Extract transactions from the given file. This method contains the
        core logic for parsing the CSV and creating Beancount entries.
        """
        entries = []
        
        with open(filepath, 'r', encoding='utf-8-sig', newline='') as f:
            reader = csv.reader(f, delimiter=';', quotechar='"')

            try:
                header = next(reader)
                h = {name: i for i, name in enumerate(header)}
                
                required_cols = ["Date", "Amount", "Description", "Subject"]
                if not all(col in h for col in required_cols):
                    print(f"Warning: File {filepath} is missing one of the required columns: {required_cols}")
                    return []
                    
            except StopIteration:
                return []

            for i, row in enumerate(reader):
                if not row or not row[h["Date"]]:
                    continue

                post_date = datetime.strptime(row[h["Date"]], "%Y-%m-%d").date()
                amt = Decimal(row[h["Amount"]])
                payee = row[h["Description"]]
                narration = row[h["Subject"]]
                
                meta = data.new_metadata(filepath, i + 2)
                
                txn = data.Transaction(
                    meta=meta,
                    date=post_date,
                    flag=flags.FLAG_OKAY,
                    payee=payee,
                    narration=narration,
                    tags=data.EMPTY_SET,
                    links=data.EMPTY_SET,
                    postings=[
                        # Use self._account here as well for consistency
                        data.Posting(self._account, amount.Amount(amt, self.currency), None, None, None, None)
                    ],
                )

                comment = narration
                posting_account = None

                if amt < 0:
                    posting_account = payee_to_account_mapping.get(payee)
                    if not posting_account:
                        posting_account = UNCATEGORIZED_EXPENSES_ACCOUNT
                else:
                    if "Withdrawing savings" in comment:
                        posting_account = "Assets:Neon:Savings"
                    elif "Metal Cashback" in comment:
                        posting_account = "Income:Neon:Cashback"
                    elif "Referral reward" in comment:
                        posting_account = "Income:Neon:Referrals"
                    else:
                        posting_account = "Income:Uncategorized:Neon:Cash"
                        txn.meta["skip_transaction"] = True
                
                txn.postings.append(
                    data.Posting(posting_account, -txn.postings[0].units, None, None, None, None)
                )

                entries.append(txn)
                
        return entries


# This part of your file remains the same
def get_importer(account, currency):
    return NeonImporter(account, currency)


if __name__ == "__main__":
    ingest = beangulp.Ingest([get_importer("Assets:Neon:Cash", "CHF")], [])
    ingest()