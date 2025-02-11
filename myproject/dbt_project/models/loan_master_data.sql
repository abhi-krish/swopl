{% set unique_key = 'loan_id' %}

WITH borrowers AS (
    SELECT 
        borrower_id,
        loan_id,
        first_name,
        last_name,
        ssn,
        toDate(dob) AS dob,
        email,
        phone_number,
        address,
        toDecimal(income, 18, 2) AS income,
        toInt32(credit_score) AS credit_score
    FROM default.borrowers_data
),
co_borrowers AS (
    SELECT 
        co_borrower_id,
        loan_id,
        first_name,
        last_name,
        relationship_to_borrower
    FROM default.co_borrowers_data
),
collaterals AS (
    SELECT 
        toInt32(loan_id) AS loan_id,
        collateral_type,
        toDecimal(collateral_value, 10, 2) AS collateral_value,
        vin_or_property_id
    FROM default.collateral_details_cleansed
),
delinquency AS (
    SELECT 
        toInt32(loan_id) AS loan_id,
        delinquency_status,
        toDate(last_reported_date) AS last_reported_date
    FROM default.delinquency_status
),
payment AS (
    SELECT 
        toInt32(loan_id) AS loan_id,
        toDate(payment_date) AS payment_date,
        toDecimal(payment_amount, 18, 2) AS payment_amount
    FROM default.payment_history
),
documents AS (
    SELECT 
        toInt32(loan_id) AS loan_id,
        document_type,
        document_link
    FROM default.loan_documents
),
master_data AS (
    SELECT 
        loan_id,
        loan_type,
        toDecimal(loan_amount, 18, 2) AS loan_amount,
        toDecimal(remaining_balance, 18, 2) AS remaining_balance,
        toFloat64(interest_rate) AS interest_rate,
        toDate(origination_date) AS origination_date,
        toDate(maturity_date) AS maturity_date,
        payment_frequency,
        loan_status
    FROM default.loan_master_data
)

SELECT 
    md.loan_id,
    md.loan_type,
    md.loan_amount,
    md.remaining_balance,
    md.interest_rate,
    md.origination_date,
    md.maturity_date,
    md.payment_frequency,
    md.loan_status
FROM master_data md
LEFT JOIN borrowers b ON md.loan_id = toInt32(b.loan_id)
LEFT JOIN co_borrowers cb ON md.loan_id = toInt32(cb.loan_id)
LEFT JOIN collaterals c ON md.loan_id = c.loan_id
LEFT JOIN delinquency d ON md.loan_id = d.loan_id
LEFT JOIN payment p ON md.loan_id = p.loan_id
LEFT JOIN documents doc ON md.loan_id = doc.loan_id

{% if is_incremental() %}
    WHERE md.loan_id NOT IN (SELECT loan_id FROM {{ this }})
{% endif %}