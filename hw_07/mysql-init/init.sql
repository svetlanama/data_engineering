-- Локальна заміна навчальної БД: схема студента та тестова вибірка
-- olympic_dataset.athlete_event_results (структура як в оригіналі, дані згенеровані).

CREATE DATABASE IF NOT EXISTS svitlana_moiseyenko;
CREATE DATABASE IF NOT EXISTS olympic_dataset;

GRANT ALL PRIVILEGES ON svitlana_moiseyenko.* TO 'airflow'@'%';
GRANT SELECT ON olympic_dataset.* TO 'airflow'@'%';

CREATE TABLE olympic_dataset.athlete_event_results (
    edition VARCHAR(50),
    edition_id INT,
    country_noc VARCHAR(3),
    sport VARCHAR(50),
    event VARCHAR(100),
    result_id INT,
    athlete VARCHAR(100),
    athlete_id INT,
    pos VARCHAR(10),
    medal VARCHAR(10),
    isTeamSport BOOLEAN
);

SET SESSION cte_max_recursion_depth = 10000;

INSERT INTO olympic_dataset.athlete_event_results
WITH RECURSIVE seq (n) AS (
    SELECT 1 UNION ALL SELECT n + 1 FROM seq WHERE n < 5000
)
SELECT
    CONCAT(1896 + 4 * (n % 32), ' Summer Olympics'),
    n % 32 + 1,
    ELT(n % 6 + 1, 'UKR', 'USA', 'GBR', 'FRA', 'GER', 'JPN'),
    ELT(n % 5 + 1, 'Athletics', 'Swimming', 'Fencing', 'Boxing', 'Rowing'),
    CONCAT('Event ', n % 40 + 1),
    100000 + n,
    CONCAT('Athlete ', n),
    n,
    CAST(n % 20 + 1 AS CHAR),
    CASE
        WHEN n % 20 = 0 THEN 'Gold'
        WHEN n % 20 = 1 OR n % 45 = 7 THEN 'Silver'
        WHEN n % 20 = 2 OR n % 30 = 13 THEN 'Bronze'
    END,
    n % 5 = 4
FROM seq;
