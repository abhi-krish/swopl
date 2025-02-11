{% materialization incremental, unique_key='loan_id' %}
WITH 
    borrowers AS (
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
            CAST(income AS Decimal(18, 2)) AS income,
            CAST(credit_score AS Int32) AS credit_score
        FROM default.borrowers_data
    ),
    co_borrowers AS (
        SELECT 
            CAST(co_borrower_id AS UUID) AS co_borrower_id,
            CAST(loan_id AS Int32) AS loan_id,
            first_name,
            last_name,
            relationship_to_borrower
        FROM default.co_borrowers_data
    ),
    collateral AS (
        SELECT 
            CAST(loan_id AS Int32) AS loan_id,
            collateral_type,
            CAST(collateral_value AS Decimal(10, 2)) AS collateral_value,
            vin_or_property_id
        FROM default.collateral_details
    ),
    loan_master AS (
        SELECT 
            CAST(loan_id AS Int32) AS loan_id,
            loan_type,
            CAST(loan_amount AS Decimal(18, 2)) AS loan_amount,
            CAST(remaining_balance AS Decimal(18, 2)) AS remaining_balance,
            CAST(interest_rate AS Float64) AS interest_rate,
            toDate(origination_date) AS origination_date,
            toDate(maturity_date) AS maturity_date,
            payment_frequency,
            loan_status
        FROM default.loan_master_data
    )
SELECT 
    c.loan_id,
    c.collateral_type,
    c.collateral_value,
    c.vin_or_property_id
FROM collateral AS c
LEFT JOIN loan_master AS lm ON c.loan_id = lm.loan_id
LEFT JOIN borrowers AS b ON lm.loan_id = CAST(b.loan_id AS Int32)
LEFT JOIN co_borrowers AS cb ON b.loan_id = cb.loan_id

{% if is_incremental() %}
WHERE c.loan_id > (SELECT MAX(loan_id) FROM {{ this }})
{% endif %}