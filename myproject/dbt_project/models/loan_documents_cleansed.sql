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
        toDecimal64(income, 2) AS income,
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

collateral AS (
    SELECT
        toInt32(loan_id) AS loan_id,
        collateral_type,
        toDecimal64(collateral_value, 2) AS collateral_value,
        vin_or_property_id
    FROM default.collateral_details
),

delinquency AS (
    SELECT
        toInt32(loan_id) AS loan_id,
        delinquency_status,
        last_reported_date
    FROM default.delinquency_status
),

loan_master AS (
    SELECT
        toInt32(loan_id) AS loan_id,
        loan_type,
        toDecimal64(loan_amount, 2) AS loan_amount,
        remaining_balance,
        toDecimal64(interest_rate, 2) AS interest_rate,
        toDate(origination_date) AS origination_date,
        toDate(maturity_date) AS maturity_date,
        payment_frequency,
        loan_status
    FROM default.loan_master_data
),

loan_documents AS (
    SELECT
        toInt32(loan_id) AS loan_id,
        document_type,
        document_link
    FROM default.loan_documents
)

SELECT
    CAST(ld.loan_id AS UUID) AS loan_id,
    ld.document_type,
    ld.document_link
FROM loan_documents AS ld
LEFT JOIN borrowers AS b ON b.loan_id = ld.loan_id
LEFT JOIN co_borrowers AS cb ON cb.loan_id = ld.loan_id
LEFT JOIN collateral AS c ON c.loan_id = ld.loan_id
LEFT JOIN delinquency AS d ON d.loan_id = ld.loan_id
LEFT JOIN loan_master AS lm ON lm.loan_id = ld.loan_id

{% if is_incremental() %}
WHERE ld.loan_id NOT IN (SELECT loan_id FROM {{ this }})
{% endif %}