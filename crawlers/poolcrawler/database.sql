--
--
--

BEGIN;

--
--
--

CREATE TABLE public.transactions3
(
  id serial,
  hash character varying(200),
  block_number integer,
  joined_pool timestamp without time zone,
  joined_chain timestamp without time zone,
  user_from character varying(70),
  user_to character varying(70),
  receipt_status character varying(200),
  value character varying(30),
  gas character varying(30),
  gas_price character varying(30),
  input text,
  gas_offered integer,
  CONSTRAINT transactions3_pkey PRIMARY KEY (id),
  CONSTRAINT transactions3_unique UNIQUE (hash)
);

--
--
--

COMMIT;
