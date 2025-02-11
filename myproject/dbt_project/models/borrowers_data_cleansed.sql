{% if is_incremental() %}
    WITH new_data AS (
        SELECT 
            borrower_id,
            toUUID(co_borrower_id) AS co_borrower_id,
            toInt32(loan_id) AS loan_id,
            first_name,
            last_name,
            ssn,
            toDate(dob) AS dob,
            email,
            phone_number,
            address,
            toDecimal32(income, 2) AS income,
            toInt32(credit_score) AS credit_score,
            relationship_to_borrower
        FROM default.borrowers_data
    ),
    co_borrower_data AS (
        SELECT 
            toUUID(co_borrower_id) AS co_borrower_id,
            toInt32(loan_id) AS loan_id,
            first_name AS co_first_name,
            last_name AS co_last_name,
            relationship_to_borrower
        FROM default.co_borrowers_data
    )
    SELECT 
        b.borrower_id,
        b.co_borrower_id,
        b.loan_id,
        b.first_name,
        b.last_name,
        b.ssn,
        b.dob,
        b.email,
        b.phone_number,
        b.address,
        b.income,
        b.credit_score,
        co.relationship_to_borrower 
    FROM new_data b
    LEFT JOIN co_borrower_data co ON b.co_borrower_id = co.co_borrower_id
    WHERE b.loan_id IS NOT NULL

{% else %}
    WITH borrowers AS (
        SELECT 
            toUUID(b.borrower_id) AS borrower_id,
            toUUID(b.co_borrower_id) AS co_borrower_id,
            toInt32(b.loan_id) AS loan_id,
            b.first_name,
            b.last_name,
            b.ssn,
            toDate(b.dob) AS dob,
            b.email,
            b.phone_number,
            b.address,
            toDecimal32(b.income, 2) AS income,
            toInt32(b.credit_score) AS credit_score,
            co.relationship_to_borrower
        FROM default.borrowers_data b
        LEFT JOIN (
            SELECT 
                toUUID(co_borrower_id) AS co_borrower_id,
                toInt32(loan_id) AS loan_id,
                first_name,
                last_name,
                relationship_to_borrower
            FROM default.co_borrowers_data
        ) co ON b.co_borrower_id = co.co_borrower_id
    )
    SELECT * 
    FROM borrowers
{% endif %}