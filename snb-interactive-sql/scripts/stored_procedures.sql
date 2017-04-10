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

