drop table k_weight;
drop view country;
drop table c_sum;

drop table post;
drop table forum;
drop table forum_person;
drop table forum_tag;
drop table organisation;
drop table person;
drop table person_email;
drop table person_tag;
drop table knows;
drop table likes;
drop table person_language;
drop table person_university;
drop table person_company;
drop table place;
drop table post_tag;
drop table tagclass;
drop table subclass;
drop table tag;
drop table tag_tagclass;

create table post (
    ps_postid bigint primary key,
    ps_imagefile varchar,
    ps_creationdate bigint not null,
    ps_locationip varchar not null,
    ps_browserused varchar not null,
    ps_language varchar,
    ps_content varchar not null,
    ps_length int not null,
    ps_creatorid bigint,
    ps_p_creatorid bigint,
    ps_locationid bigint,
    ps_forumid bigint,
    ps_replyof bigint,
    ps_country int
);

create table forum (
   f_forumid bigint primary key,
   f_title varchar not null,
   f_creationdate bigint not null, 
   f_moderatorid bigint
);

create table forum_person (
   fp_forumid bigint not null,
   fp_personid bigint not null,
   fp_creationdate bigint not null,
   primary key (fp_forumid, fp_personid)
);

create table forum_tag (
   ft_forumid bigint not null,
   ft_tagid bigint not null,
   primary key (ft_forumid, ft_tagid)
);

create table organisation (
   o_organisationid bigint primary key,
   o_type varchar not null,
   o_name varchar not null,
   o_url varchar not null,
   o_placeid bigint
);

create table person (
   p_personid bigint primary key,
   p_firstname varchar not null,
   p_lastname varchar not null,
   p_gender varchar not null,
   p_birthday bigint not null,
   p_creationdate bigint not null,
   p_locationip varchar not null,
   p_browserused varchar not null,
   p_placeid bigint,
    p_country int
);

-- additional index for p_placeid foreign key reference
create index p_placeid on person (p_placeid);

create table person_email (
   pe_personid bigint not null,
   pe_email varchar not null,
   primary key (pe_personid, pe_email)
);

create table person_tag (
   pt_personid bigint not null,
   pt_tagid bigint not null,
   primary key (pt_personid, pt_tagid)
);

create table knows (
   k_person1id bigint not null,
   k_person2id bigint not null,
   k_creationdate bigint,
   primary key (k_person1id, k_person2id)
);

create table likes (
   l_personid bigint not null,
   l_postid bigint not null,
   l_creationdate bigint not null,
   primary key (l_postid, l_personid) 
);

create index l_personid on likes (l_personid, l_creationdate, l_postid);

create table person_language (
   plang_personid bigint not null,
   plang_language varchar not null,	
   primary key (plang_personid, plang_language)
);

create table person_university (
   pu_personid bigint not null,
   pu_organisationid bigint not null,
   pu_classyear int not null,
   primary key (pu_personid, pu_organisationid)
);

create table person_company (
   pc_personid bigint not null,
   pc_organisationid bigint not null,
   pc_workfrom int not null,
   primary key (pc_personid, pc_organisationid)
);

create table place (
   pl_placeid bigint primary key,
   pl_name varchar not null,
   pl_url varchar not null,
   pl_type varchar not null,
   pl_containerplaceid bigint
);

create index pl_containerplaceid on place (pl_containerplaceid);

create table post_tag (
   pst_postid bigint not null,
   pst_tagid bigint not null,
   primary key (pst_postid, pst_tagid)
);

create index pst_tagid on post_tag (pst_tagid);

create table tagclass (
   tc_tagclassid bigint primary key,
   tc_name varchar not null,
   tc_url varchar not null
);

create table subclass (
   s_subtagclassid bigint not null,
   s_supertagclassid bigint not null,
   primary key (s_subtagclassid, s_supertagclassid)
);

create table tag (
   t_tagid bigint primary key,
   t_name varchar not null,
   t_url varchar not null
);

create table tag_tagclass (
   ttc_tagid bigint not null,
   ttc_tagclassid bigint not null,
   primary key (ttc_tagid, ttc_tagclassid)
);


create index k_p2 on knows (k_person2id, k_person1id);

create index ps_creatorid on post (ps_creatorid, ps_creationdate);
create index ps_p_creatorid on post (ps_p_creatorid);
create index ps_replyof on post (ps_replyof);
--create column index ps_replyof on post (ps_replyof, ps_creatorid, ps_creationdate) partition (ps_replyof int (0hexffff00));
create index ps_forumid on post (ps_forumid, ps_creatorid) ;

create index fp_personid on forum_person (fp_personid, fp_creationdate, fp_forumid);


create table k_weight (kw_p1 bigint, kw_p2 bigint, kw_weight real,
  primary key (kw_p1, kw_p2));

create table c_sum (cs_p1 bigint, cs_p2 bigint, cs_wnd int, cs_tag bigint, cs_cnt int, 
  primary key (cs_p1, cs_p2, cs_wnd, cs_tag));



create view country as select city.pl_placeid as ctry_city, ctry.pl_name as ctry_name from place city, place ctry where city.pl_containerplaceid = ctry.pl_placeid and ctry.pl_type = 'country';

