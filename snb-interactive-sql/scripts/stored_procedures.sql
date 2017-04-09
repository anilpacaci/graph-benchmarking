create procedure post_view_1 (in postid int) {
  declare content, imagefile varchar;
  declare creationdate int;
  result_names(content, imagefile, creationdate);

  whenever not found goto done1;
  declare cr1 cursor for 
      select ps_content, ps_imagefile, ps_creationdate
        from post
	where
	  ps_postid = postid;

  open cr1;
  while (1)
    {
      fetch cr1 into content, imagefile, creationdate;
      result (content, imagefile, creationdate);
    }

done1:
  close cr1;
}


create function post_view_1(postid int)
declare
    content varchar;
    imagefile varchar;
    creationdate int;
    result_names(content, imagefile, creationdate);
begin

end
