create schema if not exists shmr;

CREATE TABLE shmr.progress
(
  id varchar(20) NOT NULL,
  latest_posting bigint NOT NULL,
  CONSTRAINT account_pkey PRIMARY KEY (id)
)
WITH (
OIDS=FALSE
);