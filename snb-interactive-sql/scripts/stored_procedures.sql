create or replace function post_view_1(postid bigint)
returns table (content varchar, imagefile varchar, creationdate bigint) as $func$
begin
    return query
    select ps_content, ps_imagefile, ps_creationdate from post where ps_postid = postid;
end
$func$ LANGUAGE plpgsql;

create or replace function post_view_2(postid bigint)
returns table (personid bigint, firstname varchar, lastname varchar) as $func$
begin
    return query
    select p_personid, p_firstname, p_lastname from post, person where ps_postid = postid and ps_creatorid = p_personid;
end
$func$ LANGUAGE plpgsql;


create or replace function post_view_3(postid bigint)
returns table (forumid bigint, forumname varchar, modpersonid bigint, modfirstname varchar, modlastname varchar) as $func$
begin
    return query
    select f_forumid, f_title, p_personid, p_firstname, p_lastname
        from post ps, person p, forum f
	where
	  ps.ps_postid = (
	      with recursive reply_closure(id, parentid) as (
            select ps_postid, ps_replyof
            from post
            where ps_postid = postid
            union all
            select ps_postid, ps_replyof
            from reply_closure, post
            where parentid = ps_postid
          )
	      select id from reply_closure where parentid is null
	  )
	  and ps.ps_forumid = f.f_forumid and f.f_moderatorid = p.p_personid;
end
$func$ LANGUAGE plpgsql;


create or replace function post_view_4(postid bigint)
returns table (origpostid bigint, origpostcontent varchar, creationdate bigint, origauthorid bigint, origfirstname varchar, origlastname varchar, friendornot boolean) as $func$
begin
    return query
    select p2.ps_postid, p2.ps_content, p2.ps_creationdate, p_personid, p_firstname, p_lastname,
      	     (case when exists (
	     	   	       select 1 from knows
			       where p1.ps_creatorid = k_person1id and p2.ps_creatorid = k_person2id)
	      then true
	      else false
	      end)
        from post p1, post p2, person p
	where
	  p1.ps_postid = postid and p2.ps_replyof = p1.ps_postid and p2.ps_creatorid = p.p_personid
	order by 3 desc, 4;
end
$func$ LANGUAGE plpgsql;


create or replace function person_view_1(personid bigint)
returns table (firstname varchar, lastname varchar, gender varchar, birthday bigint, creatindate bigint, locationip varchar, browserused varchar, placeid bigint) as $func$
begin
    return query
    select p_firstname, p_lastname, p_gender, p_birthday, p_creationdate, p_locationip, p_browserused, p_placeid
    from person
    where p_personid = personid;
end
$func$ LANGUAGE plpgsql;

create or replace function person_view_2(personid bigint)
returns table (postid bigint, content varchar, imagefile varchar, postcreationdate bigint, origpostid bigint, origpersonid bigint, origfirstname varchar, origlastname varchar) as $func$
begin
    return query
    select ps.ps_postid, ps.ps_content, ps.ps_imagefile, ps.ps_creationdate, ops.ps_postid, p.p_personid, p.p_firstname, p.p_lastname
    from post ps, post ops, person p
    where ps.ps_creatorid = personid and
    ops.ps_postid = (
	      with recursive reply_closure(id, parentid) as (
            select ps_postid, ps_replyof
            from post
            where ps_postid = ps.ps_postid
            union all
            select ps_postid, ps_replyof
            from reply_closure, post
            where parentid = ps_postid
          )
	      select id from reply_closure where parentid is null
	  ) and
	  ops.ps_creatorid = p.p_personid
    order by ps.ps_creationdate desc
    limit 10;
end
$func$ LANGUAGE plpgsql;


create or replace function person_view_3(personid bigint)
returns table (friendpersonid bigint, friendfirstname varchar, friendlastname varchar, since bigint) as $func$
begin
    return query
    select p.p_personid, p.p_firstname, p.p_lastname, k.k_creationdate
        from knows k, person p
	where
	  k.k_person1id = personid and k.k_person2id = p.p_personid
	order by 4 desc, 1;
end
$func$ LANGUAGE plpgsql;

-- helper functions for update queries

create or replace function c_weight_upd(person1id bigint, person2id bigint)
returns real as $$
declare
    x real;
    y real;
begin
    if person1id is null or person2id is null then
        return 0;
    end if;
    select sum (case when ps2.ps_replyof is null then 1 else 0.5 end) into x from post ps1, post ps2
	   where ps1.ps_creatorid = person1id and ps1.ps_replyof = ps2.ps_postid and ps2.ps_creatorid = person2id;
    select sum (case when ps2.ps_replyof is null then 1 else 0.5 end) into y from post ps1, post ps2
	   where ps1.ps_creatorid = person2id and ps1.ps_replyof = ps2.ps_postid and ps2.ps_creatorid = person1id;
    return coalesce(x, 0) + coalesce(y, 0);  
end
$$ LANGUAGE plpgsql;

create or replace function k_weight_add(person1id bigint, person2id bigint)
returns void as $$
declare 
    cw real;
begin
    cw := c_weight_upd(person1id, person2id);
    if cw <> 0 then
        if person1id < person2id then
            insert into k_weight values (person1id, person2id, cw);
        else
            insert into k_weight values (person2id, person1id, cw);
        end if;
    end if;
end
$$ LANGUAGE plpgsql;


-- LDBC Update Queries 1 to 8

create or replace function LdbcUpdate8AddFriendship(person1id bigint, person2id bigint, creationdate timestamptz)
returns void as $$
begin
    insert into knows values(person1id, person2id, extract(epoch from creationdate::timestamptz) * 1000 );
	insert into knows values(person2id, person1id, extract(epoch from creationdate::timestamptz) * 1000 );
    perform k_weight_add(person1id, person2id);
end
$$ LANGUAGE plpgsql;


