CREATE TABLE Ledger 
(
    id SERIAL PRIMARY KEY,
    coin_name VARCHAR(60) NOT NULL,
    price NUMERIC(20,10) NOT NULL,
    aodate TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);



CREATE OR REPLACE FUNCTION insert_coins_from_csv_into_ledger(
    p_coin_name VARCHAR(60),
    p_price NUMERIC(20,10),
	p_aodate timestamp
)
RETURNS INTEGER AS $$
DECLARE
    v_id INTEGER;
BEGIN
    INSERT INTO Ledger (coin_name, price,aodate)
    VALUES (UPPER(p_coin_name), p_price, p_aodate)
    RETURNING id INTO v_id;

    RETURN v_id;
END;
$$ LANGUAGE plpgsql;
