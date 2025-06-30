from beancount.core import data
import beangulp
from beancount_importers.bank_classifier import payee_to_account_mapping
from beangulp.importers import csv

import tempfile
import os
import csv as csv_module
import chardet  # For encoding detection
from decimal import Decimal


Col = csv.Col

UNCATEGORIZED_EXPENSES_ACCOUNT = "Expenses:FIXME"

# Define constants from cache module
HEAD_DETECT_MAX_BYTES = 128 * 1024  # Same as in cache.py


class FilteringCSVImporter(beangulp.Importer):
    """Custom CSV importer that filters rows based on a specified column"""
    
    def __init__(self, config, account, currency, filter_column=None, filter_value=None, **kwargs):
        self.csv_importer = csv.CSVImporter(config, account, currency, **kwargs)
        self.filter_column = filter_column
        self.filter_value = filter_value
        
    def identify(self, filepath):
        return self.csv_importer.identify(filepath)
    
    def account(self, filepath):
        return self.csv_importer.account(filepath)
    
    def date(self, filepath):
        return self.csv_importer.date(filepath)
    
    def filename(self, filepath):
        return self.csv_importer.filename(filepath)
    
    def extract(self, filepath, existing):
        # Detect file encoding
        with open(filepath, 'rb') as f:
            rawdata = f.read(HEAD_DETECT_MAX_BYTES)
        detected = chardet.detect(rawdata)
        encoding = detected['encoding'] or 'utf-8'  # Default to UTF-8
        
        # Read the entire file with detected encoding
        with open(filepath, 'r', encoding=encoding, errors='replace') as f:
            rows = f.readlines()
        
        if not rows:
            return []
        
        # Split header into columns and find filter column position
        header = rows[0].strip().split(',')
        col_idx = None
        if self.filter_column:
            try:
                col_idx = header.index(self.filter_column)
            except ValueError:
                pass
        
        filtered_rows = [rows[0]]  # Keep header as-is
        
        # Process each row
        for row in rows[1:]:
            # Only process non-empty rows
            stripped_row = row.strip()
            if not stripped_row:
                continue
                
            # Split row into columns
            cols = stripped_row.split(',')
            
            # Skip if the row doesn't have enough columns
            if col_idx is not None and len(cols) <= col_idx:
                filtered_rows.append(row)
                continue
                
            # Check filter column value and skip if it matches
            if cols[col_idx].strip() == self.filter_value:
                continue
            
            # --- Subtract fees from the amount if applicable ---
            # Only process if we have at least 7 columns (index 0-6)
            if len(cols) > 6:
                try:
                    # Convert amount and fee to Decimal for accuracy
                    amount = Decimal(cols[5].strip())
                    fee = Decimal(cols[6].strip())
                    
                    # Subtract fee from amount
                    net_amount = amount - fee
                    
                    # Replace old amount with net_amount
                    cols[5] = str(net_amount)
                    
                    # Reconstruct row with new amount
                    row = ','.join(cols) + '\n'
                except (ValueError, TypeError, IndexError):
                    # Keep original row if conversion fails
                    pass
            #---
            filtered_rows.append(row)
        
        # Create a temporary file with filtered contents
        with tempfile.NamedTemporaryFile(mode='w', encoding=encoding, delete=False, suffix='.csv') as tf:
            tf.writelines(filtered_rows)
            temp_filepath = tf.name
        
        try:
            # Process the temporary file
            return self.csv_importer.extract(temp_filepath, existing)
        finally:
            # Clean up temporary file
            try:
                os.unlink(temp_filepath)
            except OSError:
                pass


def categorizer(txn, row):
    payee = row[4]
    comment = row[4]
    if comment.startswith("To "):
        payee = comment[3:]

    posting_account = None
    if txn.postings[0].units.number < 0:
        posting_account = payee_to_account_mapping.get(payee)

        if not posting_account:
            posting_account = UNCATEGORIZED_EXPENSES_ACCOUNT
    else:
        if "Withdrawing savings" in comment:
            posting_account = "Assets:Revolut:Savings"
        elif "Metal Cashback" in comment:
            posting_account = "Income:Revolut:Cashback"
        elif "Referral reward" in comment:
            posting_account = "Income:Revolut:Referrals"
        else:
            posting_account = "Income:Uncategorized:Revolut"
        txn.meta["skip_transaction"] = True

    txn.postings.append(
        data.Posting(posting_account, -txn.postings[0].units, None, None, None, None)
    )
    return txn


def get_importer(account, currency):
    config = {
        Col.DATE: "Started Date",
        Col.NARRATION: "Description",
        Col.AMOUNT: "Amount",
        Col.PAYEE: "Description",
        Col.CURRENCY: "Currency",
        Col.BALANCE: "Balance",
    }
    
    return FilteringCSVImporter(
        config,
        account,
        currency,
        filter_column="State",  # Name of the header for the filter column
        filter_value="REVERTED",   # Value to filter out
        categorizer=categorizer,
    )


if __name__ == "__main__":
    ingest = beangulp.Ingest([get_importer("Assets:Revolut:Cash", "GBP")], [])
    ingest()