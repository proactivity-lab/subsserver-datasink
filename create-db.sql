CREATE TABLE data
(
  id serial NOT NULL,
  parent integer,
  guid character varying(16),
  inserted timestamp without time zone DEFAULT now(),
  arrival timestamp without time zone,
  production_start timestamp without time zone,
  production_end timestamp without time zone,
  type character varying,
  value double precision,
  CONSTRAINT data_id PRIMARY KEY (id)
)
WITH (
  OIDS=FALSE
);

CREATE TABLE devices
(
  id serial NOT NULL,
  guid character varying(16),
  start_timestamp timestamp without time zone NOT NULL,
  end_timestamp timestamp without time zone,
  name character varying,
  type character varying,
  group_tag character varying,
  location_tag character varying,
  coordsys character varying,
  latitude double precision,
  longitude double precision,
  elevation double precision,
  CONSTRAINT device_id PRIMARY KEY (id)
)
WITH (
  OIDS=FALSE
);

CREATE INDEX IF NOT EXISTS data_production_start_idx ON data production_start;
CREATE INDEX IF NOT EXISTS data_production_end_idx ON data production_end;

