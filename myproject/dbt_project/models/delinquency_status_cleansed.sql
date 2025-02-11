{{ config(
    materialized='incremental',
    unique_key='loan_id',
    database='internal_loan_data'
) }}

WITH borrowers AS (
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
    FROM default.borrowers_data
),

co_borrowers AS (
    SELECT
        loan_id,
        toUUID(co_borrower_id) AS co_borrower_id,
        first_name,
        last_name,
        relationship_to_borrower
    FROM default.co_borrowers_data
),

collateral AS (
    SELECT
        loan_id,
        collateral_type,
        toDecimal64(collateral_value, 2) AS collateral_value,
        vin_or_property_id
    FROM default.collateral_details
),

delinquency AS (
    SELECT
        loan_id,
        toInt32(delinquency_status) AS delinquency_status,
        toDate(last_reported_date) AS last_reported_date
    FROM default.delinquency_status
),

loan_master AS (
    SELECT
        loan_id,
        toInt32(loan_amount) AS loan_amount,
        toDecimal64(remaining_balance, 2) AS remaining_balance,
        toFloat64(interest_rate) AS interest_rate,
        toDate(origination_date) AS origination_date,
        toDate(maturity_date) AS maturity_date,
        payment_frequency,
        loan_status
    FROM default.loan_master_data
)

SELECT
    toInt32(d.loan_id) AS loan_id,
    d.delinquency_status,
    d.last_reported_date
FROM delinquency d
JOIN loan_master lm ON d.loan_id = lm.loan_id
JOIN borrowers b ON b.loan_id = d.loan_id
LEFT JOIN co_borrowers cb ON cb.loan_id = d.loan_id
LEFT JOIN collateral c ON c.loan_id = d.loan_id

{% if is_incremental() %}
WHERE d.loan_id IN (SELECT loan_id FROM {{ this }})
{% endif %}