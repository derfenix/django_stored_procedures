CREATE TABLE IF NOT EXISTS test_table(
  id SERIAL NOT NULL PRIMARY KEY,
  name VARCHAR(255),
  amount INT
);

CREATE OR REPLACE VIEW test_view AS (
    SELECT id, name, amount * 2 as amount FROM test_table
);

CREATE OR REPLACE FUNCTION test_function(num INTEGER) RETURNS INT AS
$$
BEGIN
  RETURN num * 4;
END
$$ LANGUAGE plpgsql;
