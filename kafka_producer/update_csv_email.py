import csv
from pathlib import Path

files = [
    Path('data/customers.csv'),
    Path('data/merchants.csv'),
]

for path in files:
    with path.open('r', newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames + ['email']
        rows = list(reader)

    for row in rows:
        row['email'] = 'databeli14@gmail.com'

    with path.open('w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    print(f'Updated {path} with {len(rows)} rows; header={fieldnames}')
