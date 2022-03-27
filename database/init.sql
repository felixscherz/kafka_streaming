CREATE TABLE IF NOT EXISTS regions (
  region_id SERIAL PRIMARY KEY,
  region VARCHAR(255) NOT NULL
);

INSERT INTO regions (region_id, region) VALUES
    (DEFAULT, 'global'),
    (DEFAULT, 'germany');
