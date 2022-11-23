CREATE SCHEMA icyb AUTHORIZATION postgres;

CREATE TABLE IF NOT EXISTS icyb.transactions (
    id serial NOT NULL,
    user_id integer NOT NULL,
    date timestamp without time zone NOT NULL,
    account_id integer NOT NULL DEFAULT 0,
    amount numeric(8, 2) NOT NULL,
    category character varying(17) COLLATE pg_catalog."default" NOT NULL,
    description character varying(85) COLLATE pg_catalog."default" NOT NULL,
    balance numeric(9, 2) NOT NULL,
    is_del boolean NOT NULL DEFAULT false,
    PRIMARY KEY (id)
) TABLESPACE pg_default;

ALTER TABLE
    icyb.transactions OWNER to postgres;

CREATE TABLE IF NOT EXISTS icyb.regular (
    id serial NOT NULL,
    user_id integer NOT NULL,
    description character varying(25) COLLATE pg_catalog."default",
    search_f character varying(25) COLLATE pg_catalog."default" NOT NULL,
    arg_sf character varying(61) COLLATE pg_catalog."default",
    amount numeric(8, 2) NOT NULL,
    start_date date NOT NULL,
    end_date date,
    d_years integer NOT NULL,
    d_months integer NOT NULL,
    d_days integer NOT NULL,
    adjust_price boolean NOT NULL DEFAULT false,
    adjust_date boolean NOT NULL DEFAULT false,
    follow_overdue boolean NOT NULL DEFAULT false;

PRIMARY KEY (id)
) TABLESPACE pg_default;

ALTER TABLE
    icyb.regular OWNER to postgres;

CREATE TABLE IF NOT EXISTS icyb.dictionary_categories (
    id serial NOT NULL,
    user_id integer NOT NULL,
    key character varying(61) COLLATE pg_catalog."default" NOT NULL,
    value character varying(61) COLLATE pg_catalog."default" NOT NULL,
    PRIMARY KEY (id)
) TABLESPACE pg_default;

ALTER TABLE
    icyb.dictionary_categories OWNER to postgres;

CREATE TABLE IF NOT EXISTS icyb.onetime (
    id serial NOT NULL,
    user_id integer NOT NULL,
    description character varying(25),
    amount numeric(8, 2) NOT NULL,
    date date NOT NULL,
    PRIMARY KEY (id)
) TABLESPACE pg_default;

ALTER TABLE
    IF EXISTS icyb.onetime OWNER to postgres;

CREATE TABLE IF NOT EXISTS icyb.sbs_models (
    id serial NOT NULL,
    user_id integer NOT NULL,
    dump bytea NOT NULL
);

ALTER TABLE
    IF EXISTS icyb.sbs_models OWNER to postgres;


CREATE TABLE IF NOT EXISTS icyb.accounts (
    id serial NOT NULL,
    user_id integer NOT NULL,
    type smallint NOT NULL,
    description character varying(25) COLLATE,
    credit_limit money,
    discharge_date date,
    PRIMARY KEY (id)
) TABLESPACE pg_default;

ALTER TABLE
    IF EXISTS icyb.accounts OWNER to postgres;