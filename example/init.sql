CREATE TABLE IF NOT EXISTS tbl_example (
    ID BIGINT NOT NULL UNIQUE,
    UPDATED TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    CREATED TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    TEXT VARCHAR(255),
    PRIMARY KEY (ID)
);