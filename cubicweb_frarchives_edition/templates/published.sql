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

{% for etype in etypes -%}
create or replace function {{schema}}.wf_related_update_for_{{etype|lower}}(_eid int)
  returns void
  security definer
  language plpgsql
as $$
  begin
-- we MUST have a published entity here, not checked here!
-- Note: we always do 'DELETE + INSERT' to not have to check
-- wether we must INSERT or UPDATE (and an UPDATE is basically
-- a DELETE+INSERT, so)

-- First ensure the {{etype}} object is inserted in the {{schema}} namespace
     execute format('delete from {{schema}}.cw_{{etype|lower}} as c where c.cw_eid=%s', _eid);
     execute format('insert into {{schema}}.cw_{{etype|lower}} (select * from cw_{{etype|lower}} as c where c.cw_eid=%s)', _eid);

{% if etype == 'FindingAid' %}
     execute format('delete from {{schema}}.cw_facomponent as fac where fac.cw_finding_aid = %s', _eid);
     execute format('insert into {{schema}}.cw_facomponent (select * from cw_facomponent as c where c.cw_finding_aid=%s)', _eid);
{% endif %}

-- Handle relations.
{%- for rdef, rdirs in rtypes[etype].items() %}
{%- for rdir in rdirs %}
{%- set col='eid_from' if rdir == 'subject' else 'eid_to' %}
-- {{rtype}}
     delete from {{schema}}.{{rdef}}_relation as c
       where c.{{col}}=_eid;
     insert into {{schema}}.{{rdef}}_relation
       (select * from {{rdef}}_relation as c
        where c.{{col}}=_eid);
{%- endfor %}
{%- endfor %}
{% if etype == 'FindingAid' %}
     delete from {{schema}}.index_relation as c
       where c.eid_to in (
         select cw_eid from cw_facomponent
           where cw_finding_aid=_eid);

     insert into {{schema}}.index_relation
        (select c.* from index_relation c
          join cw_facomponent f ON c.eid_to=f.cw_eid
          where f.cw_finding_aid=_eid);
{% endif %}

  end;
$$;
{% endfor %}

{% for etype in etypes -%}
create or replace function {{schema}}.wf_related_delete_for_{{etype|lower}}(_eid int)
  returns void
  security definer
  language plpgsql
as $$
  begin
-- we MUST have a published entity here, not checked here!
-- Note: we always do 'DELETE + INSERT' to not have to check
-- wether we must INSERT or UPDATE.

   if exists (select type from entities where eid=_eid) then
-- First ensure the object is deleted from the {{schema}} namespace
     execute format('delete from {{schema}}.cw_{{etype|lower}} as c where c.cw_eid=%s', _eid);
   {% if etype == 'FindingAid' %}
     execute format('delete from {{schema}}.cw_facomponent as fac where fac.cw_finding_aid = %s', _eid);
   {% endif %}
   end if;



-- Handle relations.
{%- for rdef, rdirs in rtypes[etype].items() %}
{%- for rtyprdir in rdirs %}
{%- set col='eid_from' if rdir == 'subject' else 'eid_to' %}
-- {{rtype}}
     delete from {{schema}}.{{rdef}}_relation as c
       where c.{{col}}=_eid;
{%- endfor %}
{%- endfor %}

{% if etype == 'FindingAid' %}
     delete from {{schema}}.index_relation as c
       where c.eid_to in (
         select cw_eid from cw_facomponent
           where cw_finding_aid=_eid);
{% endif %}

end;

$$;
{%- endfor %}


create or replace function {{schema}}.in_state_update()
  returns trigger
  security definer
  language plpgsql
as $$
  declare
    tname text;
begin
  if exists (select 1 from entities e
    where
      e.eid = new.eid_from and
      e.type in ({{etypes|map('sqlstr')|join(',')}}) ) then

    select lower(type) from entities where eid=new.eid_from
     into tname;
    if exists (select 1 from cw_state s
      where
        s.cw_name LIKE '%\_published' and
        s.cw_eid = new.eid_to) then
-- case where the object is published
      execute format('select {{schema}}.wf_related_update_for_%s(%s)', tname, new.eid_from);
    else
-- case where the object is unpublished
      execute format('select {{schema}}.wf_related_delete_for_%s(%s)', tname, new.eid_from);
    end if;
  end if;
  return new;
end;
$$;


-- handle relations...
{% for rdef in rnames -%}
create or replace function {{schema}}.{{rdef}}_relation_delete()
  returns trigger
  security definer
  language plpgsql
as $$
begin
  delete from {{schema}}.{{rdef}}_relation
      where eid_from=old.eid_from and eid_to=old.eid_to;
  return old;
end;
$$;

create or replace function {{schema}}.{{rdef}}_relation_update()
  returns trigger
  security definer
  language plpgsql
as $$
begin
  delete from {{schema}}.{{rdef}}_relation
    where eid_to=new.eid_to and eid_from=new.eid_from;
  if exists (select 1 from entities e
    where
      e.eid in (new.eid_from, new.eid_to) and
      e.type in ({{etypes|map('sqlstr')|join(',')}}) ) then
-- at least one side of the relation has a publication wf and must be
-- filtered
        if exists (
	  (
	    select 1 from entities e
              where
	        new.eid_from=e.eid and e.type not in ({{etypes|map('sqlstr')|join(',')}})
	    UNION
	    select 1 from cw_state cws, in_state_relation isr, entities e
              where
	        new.eid_from=e.eid and e.type in ({{etypes|map('sqlstr')|join(',')}}) and
                isr.eid_from=e.eid and isr.eid_to=cws.cw_eid and
                cws.cw_name LIKE '%\_published'
	  )
	  INTERSECT
	  (
	    select 1 from entities e
              where
	        new.eid_to=e.eid and e.type not in ({{etypes|map('sqlstr')|join(',')}})
	    UNION
	    select 1 from cw_state cws, in_state_relation isr, entities e
              where
	        new.eid_to=e.eid and e.type in ({{etypes|map('sqlstr')|join(',')}}) and
                isr.eid_from=e.eid and isr.eid_to=cws.cw_eid and
                cws.cw_name LIKE '%\_published'
	  )
	)
	then
          insert into {{schema}}.{{rdef}}_relation
            values(new.eid_from, new.eid_to);
	end if;
  else
-- handle non-filtered entities (eid_to)
    insert into {{schema}}.{{rdef}}_relation
        values(new.eid_from, new.eid_to);
  end if;
  return new;
end;
$$;
{%- endfor %}

{% for etype in etypes -%}
create or replace function {{schema}}.cw_{{etype|lower}}_update()
  returns trigger
  security definer
  language plpgsql
as $$
begin
  if exists (select 1 from cw_state s, in_state_relation r
    where
      s.cw_name LIKE '%\_published' and
      new.cw_eid = r.eid_from and
      s.cw_eid = r.eid_to) then
    perform({{schema}}.wf_related_update_for_{{etype|lower}}(new.cw_eid));
  else
    perform({{schema}}.wf_related_delete_for_{{etype|lower}}(new.cw_eid));
  end if;
  return new;
end;
$$;

create or replace function {{schema}}.cw_{{etype|lower}}_delete()
  returns trigger
  security definer
  language plpgsql
as $$
begin
  delete from {{schema}}.cw_{{etype|lower}}
    where cw_eid=old.cw_eid;
    {% if etype == 'FindingAid' %}
-- delete published FAComponents
    delete from {{schema}}.cw_facomponent as fac where fac.cw_finding_aid=old.cw_eid;
    {% endif %}
  return old;
end;
$$;
{%- endfor %}


create or replace function {{schema}}.cw_cwproperty_update()
  returns trigger
  security definer
  language plpgsql
as $$
begin
  if new.cw_pkey NOT IN ({{ignored_cwproperties}}) then
    delete from {{schema}}.cw_cwproperty
      where cw_pkey = new.cw_pkey;
    insert into {{schema}}.cw_cwproperty
      values(new.*);
  end if;
  return new;
end;
$$;

create or replace function {{schema}}.cw_cwproperty_delete()
  returns trigger
  security definer
  language plpgsql
as $$
begin
    delete from {{schema}}.cw_cwproperty
      where cw_pkey = old.cw_pkey;
  return old;
end;
$$;


create or replace function {{schema}}.cw_cwuser_update()
  returns trigger
  security definer
  language plpgsql
as $$
begin
  if new.cw_login IN ('anon', 'admin') then
    delete from {{schema}}.cw_cwuser
      where cw_login = new.cw_login;
    insert into {{schema}}.cw_cwuser
      values(new.*);
  end if;
  return new;
end;
$$;

create or replace function {{schema}}.cw_cwuser_delete()
  returns trigger
  security definer
  language plpgsql
as $$
begin
    delete from {{schema}}.cw_cwuser
      where cw_login = old.cw_login;
  return old;
end;
$$;


-- install triggers

drop trigger if exists {{schema}}_in_state_update on in_state_relation;
create trigger {{schema}}_in_state_update
    after insert or update on in_state_relation
    for each row execute procedure {{schema}}.in_state_update();

drop trigger if exists {{schema}}_cw_cwproperty_update on cw_cwproperty;
create trigger {{schema}}_cw_cwproperty_update
    after insert or update on cw_cwproperty
    for each row execute procedure {{schema}}.cw_cwproperty_update();
drop trigger if exists {{schema}}_cw_cwproperty_delete on cw_cwproperty;
create trigger {{schema}}_cw_cwproperty_delete
    after delete on cw_cwproperty
    for each row execute procedure {{schema}}.cw_cwproperty_delete();

drop trigger if exists {{schema}}_cw_cwuser_update on cw_cwuser;
create trigger {{schema}}_cw_cwuser_update
    after insert or update on cw_cwuser
    for each row execute procedure {{schema}}.cw_cwuser_update();
drop trigger if exists {{schema}}_cw_cwuser_delete on cw_cwuser;
create trigger {{schema}}_cw_cwuser_delete
    after delete on cw_cwuser
    for each row execute procedure {{schema}}.cw_cwuser_delete();

{% for etype in etypes -%}
drop trigger if exists {{schema}}_cw_{{etype|lower}}_update on cw_{{etype|lower}};
create trigger {{schema}}_cw_{{etype|lower}}_update
    after insert or update on cw_{{etype|lower}}
    for each row execute procedure {{schema}}.cw_{{etype|lower}}_update();

drop trigger if exists {{schema}}_cw_{{etype|lower}}_delete on cw_{{etype|lower}};
create trigger {{schema}}_cw_{{etype|lower}}_delete
    after delete on cw_{{etype|lower}}
    for each row execute procedure {{schema}}.cw_{{etype|lower}}_delete();
{% endfor -%}

{% for rdef in rnames -%}
drop trigger if exists {{schema}}_{{rdef}}_relation_update on {{rdef}}_relation;
create trigger {{schema}}_{{rdef}}_relation_update
    after insert or update on {{rdef}}_relation
    for each row execute procedure {{schema}}.{{rdef}}_relation_update();

drop trigger if exists {{schema}}_{{rdef}}_relation_delete on {{rdef}}_relation;
create trigger {{schema}}_{{rdef}}_relation_delete
    after delete on {{rdef}}_relation
    for each row execute procedure {{schema}}.{{rdef}}_relation_delete();
{% endfor -%}

create or replace function {{schema}}.unpublish_findingaid(eid integer)
  returns void
  language plpgsql
as $$
begin
  delete from {{schema}}.cw_findingaid where cw_eid=eid;
  delete from {{schema}}.cw_facomponent where cw_finding_aid=eid;
end;
$$;

create or replace function {{schema}}.publish_findingaid(eid integer)
  returns void
  language plpgsql
as $$
begin
  perform {{schema}}.unpublish_findingaid(eid);
  insert into {{schema}}.cw_findingaid select * from cw_findingaid where cw_eid=eid;
  insert into {{schema}}.cw_facomponent select * from cw_facomponent where cw_finding_aid=eid;
end;
$$;
