CREATE TEMPORARY TABLE temp_bano (      -- create temporary table (contains raw BANO dataset)
    banoid varchar(200),
    numero varchar(20),
    voie varchar(200),
    code_post varchar(10),
    nom_comm varchar(200),
    source varchar(10),
    lat double precision,
    lon double precision
);

COPY temp_bano FROM '{{ path }}' WITH (FORMAT CSV, DELIMITER ',', NULL '');

DROP TABLE IF EXISTS bano;

CREATE TEMPORARY TABLE bano (     -- create definite table (does not contain duplicate banoid values)
    banoid varchar(200) PRIMARY KEY,
    voie varchar(200),
    nom_comm varchar(200),
    lat double precision,
    lon double precision
);

INSERT INTO bano (banoid, voie, nom_comm, lat, lon)
SELECT DISTINCT ON (dedupe.voie, dedupe.nom_comm) dedupe.banoid, dedupe.voie, dedupe.nom_comm, dedupe.lat, dedupe.lon
FROM (SELECT DISTINCT ON(banoid) banoid, voie, nom_comm, lat, lon FROM temp_bano WHERE voie is not NULL) AS dedupe ORDER BY dedupe.voie, dedupe.nom_comm, dedupe.banoid;

CREATE INDEX bano_voie_idx ON bano(voie);
CREATE INDEX bano_nom_comm_idx ON bano(nom_comm);

DROP TABLE IF EXISTS bano_whitelisted;

CREATE TABLE bano_whitelisted (         -- create table used to align to BANO dataset
    banoid varchar(200) PRIMARY KEY,
    voie varchar(200),
    nom_comm varchar(200),
    lat double precision,
    lon double precision
);

WITH comm AS (
    SELECT nom_comm FROM (
        SELECT cw_eid, (regexp_split_to_array(cw_label, E'[,.]|(?:\\s+[(-])'))[1] AS nom_comm
        FROM cw_locationauthority WHERE cw_label LIKE '% -- %'
    ) AS tmp GROUP BY(tmp.nom_comm) HAVING COUNT(tmp.cw_eid) > 10
)
INSERT INTO bano_whitelisted (banoid, voie, nom_comm, lat, lon)         -- insert municipalities referred to in > 10 LocationAuthority labels
SELECT bano.banoid, bano.voie, bano.nom_comm, bano.lat, bano.lon FROM bano JOIN comm ON bano.nom_comm = comm.nom_comm;
INSERT INTO bano_whitelisted (banoid, voie, nom_comm, lat, lon)         -- insert Paris cf. https://extranet.logilab.fr/ticket/64545415
SELECT bano.banoid, bano.voie, bano.nom_comm, bano.lat, bano.lon FROM bano WHERE nom_comm = 'Paris' ON CONFLICT(banoid) DO NOTHING;

CREATE INDEX bano_whitelisted_voie_idx ON bano_whitelisted(voie);
CREATE INDEX bano_whitelisted_nom_comm_idx ON bano_whitelisted(nom_comm);

{% if owner %}
ALTER TABLE bano_whitelisted OWNER TO {{ owner }};
{% endif %}
