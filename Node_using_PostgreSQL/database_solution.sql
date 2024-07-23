CREATE TABLE Ledger 
(
    id SERIAL PRIMARY KEY,
    coin_name VARCHAR(60) NOT NULL,
    price NUMERIC(20,10) NOT NULL,
    aodate TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);



CREATE OR REPLACE FUNCTION insert_crypto_into_ledger(
    p_coin_name VARCHAR(60),
    p_price NUMERIC(20,10)
)
RETURNS INTEGER 
AS 
$$
DECLARE
    v_id INTEGER;
BEGIN
    INSERT INTO Ledger (coin_name, price)
    VALUES (UPPER(p_coin_name), p_price)
    RETURNING id INTO v_id;

    RETURN v_id;
END;
$$ LANGUAGE plpgsql;



CREATE OR REPLACE FUNCTION getCurrentPrice(p_coin_name VARCHAR(60))
RETURNS NUMERIC(20,10) 
AS 
$$
DECLARE
    v_price NUMERIC(20,10);
BEGIN
    SELECT price
    INTO v_price
    FROM Ledger
    WHERE coin_name = UPPER(p_coin_name)
    ORDER BY aodate DESC
    LIMIT 1;

    IF v_price IS NULL THEN
        RAISE EXCEPTION 'No price found for coin %', UPPER(p_coin_name);
    END IF;

    RETURN v_price;
END;
$$ LANGUAGE plpgsql;



CREATE OR REPLACE FUNCTION get_all_prices(p_coin_name VARCHAR(60))
RETURNS TABLE (
    id integer,
    coin_name VARCHAR(60),
    price NUMERIC(20,10),
    aodate TIMESTAMP
)
AS 
$$
BEGIN
    -- Check if p_coin_name parameter is NULL or empty
    IF p_coin_name IS NULL OR length(trim(p_coin_name)) = 0 THEN
        RAISE EXCEPTION 'Parameter p_coin_name cannot be NULL or empty';
    END IF;

    -- Return query with filter on p_coin_name and ordered by aodate descending
    RETURN QUERY
    SELECT cp.id, cp.coin_name, cp.price, cp.aodate
    FROM Ledger cp
    WHERE cp.coin_name = UPPER(p_coin_name)
    ORDER BY cp.aodate DESC;
END;
$$ LANGUAGE plpgsql;


