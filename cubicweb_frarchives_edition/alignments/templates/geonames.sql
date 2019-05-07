--
-- Copyright LOGILAB S.A. (Paris, FRANCE) 2016-2019
-- Contact http://www.logilab.fr -- mailto:contact@logilab.fr
--
-- This software is governed by the CeCILL-C license under French law and
-- abiding by the rules of distribution of free software. You can use,
-- modify and/ or redistribute the software under the terms of the CeCILL-C
-- license as circulated by CEA, CNRS and INRIA at the following URL
-- "http://www.cecill.info".
--
-- As a counterpart to the access to the source code and rights to copy,
-- modify and redistribute granted by the license, users are provided only
-- with a limited warranty and the software's author, the holder of the
-- economic rights, and the successive licensors have only limited liability.
--
-- In this respect, the user's attention is drawn to the risks associated
-- with loading, using, modifying and/or developing or reproducing the
-- software by the user in light of its specific status of free software,
-- that may mean that it is complicated to manipulate, and that also
-- therefore means that it is reserved for developers and experienced
-- professionals having in-depth computer knowledge. Users are therefore
-- encouraged to load and test the software's suitability as regards their
-- requirements in conditions enabling the security of their systemsand/or
-- data to be ensured and, more generally, to use and operate it in the
-- same conditions as regards security.
--
-- The fact that you are presently reading this means that you have had
-- knowledge of the CeCILL-C license and that you accept its terms.
--

CREATE EXTENSION IF NOT EXISTS pg_trgm;
CREATE EXTENSION IF NOT EXISTS unaccent;

-- ALTER FUNCTION unaccent(text) IMMUTABLE;

CREATE OR REPLACE FUNCTION f_unaccent(text)
RETURNS text AS
$func$
SELECT public.unaccent('public.unaccent', $1)  -- schema-qualify function and dictionary
$func$  LANGUAGE sql IMMUTABLE;

drop table if exists geonames;

create table geonames (
  geonameid integer PRIMARY KEY,
  name varchar(200),
  asciiname varchar(200),
  alternatenames varchar(10000),
  latitude double precision,
  longitude double precision,
  fclass char(1),
  fcode varchar(10),
  country_code varchar(2),
  cc2 varchar(200),
  admin1_code varchar(20),
  admin2_code varchar(80),
  admin3_code varchar(20),
  admin4_code varchar(20),
  population bigint,
  elevation int,
  dem int,
  timezone varchar(40),
  moddate date
);


COPY geonames from '{{ allcountries_path }}' null as '';

CREATE INDEX geonames_fcode_idx ON geonames(fcode);
CREATE INDEX geonames_admin2code_idx ON geonames(admin2_code);
CREATE INDEX geonames_country_code_idx ON geonames(country_code);
CREATE INDEX geonames_fclass_idx ON geonames(fclass);

DROP TABLE IF EXISTS geonames_altnames;

CREATE TEMPORARY TABLE tmp_geonames_altnames (
    alternateNameId integer  PRIMARY KEY,
    geonameid integer not null,
    isolanguage varchar(7),
    alternate_name varchar(400),
    isPreferredName boolean,
    isShortName boolean,
    isColloquial boolean,
    isHistoric boolean
);

COPY tmp_geonames_altnames from '{{ altnames_path }}' null as '';

CREATE INDEX tmp_geonames_altnames_isolanguage_idx ON tmp_geonames_altnames(isolanguage);


CREATE TABLE geonames_altnames (
    alternateNameId integer  PRIMARY KEY,
    geonameid integer not null,
    isolanguage varchar(7),
    alternate_name varchar(400),
    isPreferredName boolean,
    isShortName boolean,
    isColloquial boolean,
    isHistoric boolean,
    rank integer
);

INSERT INTO geonames_altnames
SELECT tmp.alternateNameId,
       tmp.geonameid,
       tmp.isolanguage,
       tmp.alternate_name,
       tmp.isPreferredName,
       tmp.isShortName,
       tmp.isColloquial,
       tmp.isHistoric,
       tmp.rank
FROM (SELECT
        alternateNameId,
        geonameid,
        isolanguage,
        alternate_name,
        isPreferredName,
        isShortName,
        isColloquial,
        isHistoric,
        -- add the rank column to get the preferred label easely
        ROW_NUMBER() OVER (PARTITION BY geonameid, isolanguage
        ORDER BY isPreferredName DESC NULLS LAST, isShortName) rank
      FROM tmp_geonames_altnames) as tmp
JOIN geonames AS geo
ON geo.geonameid = tmp.geonameid
AND geo.fclass IN ('A', 'P')
AND tmp.isolanguage = 'fr';

CREATE INDEX geonames_altnames_geonameid_idx ON geonames_altnames USING btree(geonameid);
CREATE INDEX geonames_altnames_isolanguage_idx ON geonames_altnames(isolanguage);
CREATE INDEX geonames_altnames_rank_idx ON geonames_altnames(rank);
CREATE INDEX geonames_altnames_name_gin_idx ON geonames_altnames USING gin (alternate_name gin_trgm_ops);
CREATE INDEX geonames_altnames_name_lower_unaccent_gin_idx ON geonames_altnames USING gin (lower(f_unaccent((alternate_name)::text)) gin_trgm_ops);
