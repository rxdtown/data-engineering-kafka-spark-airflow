CREATE DATABASE analytics;

\connect analytics

CREATE TABLE IF NOT EXISTS reporting_commandes (
    ville VARCHAR(50),
    jour DATE,
    volume_total NUMERIC(12,2),
    nb_commandes INTEGER,
    PRIMARY KEY (ville, jour)
);