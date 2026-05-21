CREATE DATABASE regression_manual;
USE regression_manual;

CREATE TABLE users (id INT INDEXED, name STRING, age INT, city STRING);
INSERT INTO users (id, name, age, city) VALUES
  (1, "Ann", 20, "Moscow"),
  (2, "Bob", 30, "Kazan"),
  (3, "Kate", 22, "Moscow");

SELECT id AS user_id, name AS user_name FROM users WHERE id == 2;
SELECT name AS user_name, age AS user_age FROM users WHERE id >= 2 ORDER BY id;

UPDATE users SET age = 31, name = "Robert" WHERE id == 2;
SELECT id, name, age FROM users WHERE id == 2;

SELECT COUNT(*) AS total FROM users;
SELECT SUM(age) AS age_sum FROM users;
SELECT AVG(age) AS avg_age FROM users WHERE id == 1;
SELECT city, COUNT(*) AS users_count FROM users GROUP BY city ORDER BY city;

SELECT id AS matched_id FROM users WHERE age >= 20 AND city LIKE "Mos%";
DELETE FROM users WHERE id == 1;
SELECT * FROM users ORDER BY id;
REVERT users "2000.01.01-00:00:00.000";
SELECT * FROM users;

