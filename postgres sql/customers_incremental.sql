BEGIN;

-- =====================================================
-- Insert 2 new customers
-- =====================================================

INSERT INTO customers (
    customer_id,
    first_name,
    last_name,
    gender,
    age,
    city,
    state,
    country,
    annual_income,
    customer_segment,
    account_open_date,
    risk_score,
    preferred_spending_min,
    preferred_spending_max,
    preferred_city,
    preferred_country,
    trusted_device_id,
    card_number,
    card_type,
    email,
    transaction_limit,
    update_timestamp
)
VALUES
(
    'CUST001001',
    'Rahul',
    'Sharma',
    'Male',
    31,
    'Gurugram',
    'Haryana',
    'India',
    1450000.00,
    'Gold',
    '2026-07-02',
    12,
    5000.00,
    25000.00,
    'Delhi',
    'India',
    'DEV-1001',
    '4532123412345678',
    'Visa',
    'databeli14@gmail.com',
    100000,
    CURRENT_TIMESTAMP
),
(
    'CUST001002',
    'Priya',
    'Mehta',
    'Female',
    28,
    'Jaipur',
    'Rajasthan',
    'India',
    980000.00,
    'Silver',
    '2026-07-02',
    8,
    3000.00,
    15000.00,
    'Jaipur',
    'India',
    'DEV-1002',
    '5212345678901234',
    'MasterCard',
    'databeli14@gmail.com',
    100000,
    CURRENT_TIMESTAMP
);

-- =====================================================
-- Update an existing customer
-- =====================================================

UPDATE customers
SET
    annual_income = 1850000.00,
    risk_score = 5,
    city = 'Bengaluru',
    state = 'Karnataka',
    update_timestamp = CURRENT_TIMESTAMP
WHERE customer_id = 'CUST000001';

COMMIT;