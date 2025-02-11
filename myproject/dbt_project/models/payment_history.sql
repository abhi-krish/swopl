{{ config(
    materialized='incremental',
    unique_key='loan_id'
) }}

WITH borrower_data AS (
    SELECT
        loan_id,
        borrower_id,
        first_name,
        last_name,
        ssn,
        toDate(dob) AS dob,
        email,
        phone_number,
        address,
        toDecimal64(income, 2) AS income,
        toInt32(credit_score) AS credit_score
    FROM
        default.borrowers_data
),

co_borrower_data AS (
    SELECT
        co_borrower_id,
        loan_id,
        first_name,
        last_name,
        relationship_to_borrower
    FROM
        default.co_borrowers_data
),

collateral_data AS (
    SELECT
        loan_id,
        collateral_type,
        toDecimal64(collateral_value, 2) AS collateral_value,
        vin_or_property_id
    FROM
        default.collateral_details
),

delinquency_data AS (
    SELECT
        loan_id,
        delinquency_status,
        toDate(last_reported_date) AS last_reported_date
    FROM
        default.delinquency_status
),

loan_master_data AS (
    SELECT
        loan_id,
        loan_type,
        toDecimal64(loan_amount, 2) AS loan_amount,
        toDecimal64(remaining_balance, 2) AS remaining_balance,
        toFloat64(interest_rate) AS interest_rate,
        toDate(origination_date) AS origination_date,
        toDate(maturity_date) AS maturity_date,
        payment_frequency,
        loan_status
    FROM
        default.loan_master_data
),

payment_history_raw AS (
    SELECT
        loan_id,
        toDate(payment_date) AS payment_date,
        toDecimal64(payment_amount, 2) AS payment_amount
    FROM
        default.payment_history
)

SELECT
    p.loan_id,
    p.payment_date,
    p.payment_amount
FROM
    payment_history_raw p
JOIN
    loan_master_data lm ON p.loan_id = lm.loan_id
JOIN
    borrower_data b ON b.loan_id = p.loan_id
LEFT JOIN
    co_borrower_data cb ON cb.loan_id = p.loan_id
LEFT JOIN
    collateral_data cd ON cd.loan_id = p.loan_id
LEFT JOIN
    delinquency_data d ON d.loan_id = p.loan_id

{% if is_incremental() %}
WHERE
    p.loan_id IS NOT NULL
{% endif %}