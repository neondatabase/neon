create or replace procedure do_commits() as $$
declare
    xid xid8;
	i integer;
begin
    for i in 1..1000000 loop
	    xid = txid_current();
		commit;
		if (pg_xact_status(xid) <> 'committed') then
		   raise exception 'CLOG corruption';
		end if;
	end loop;
end;
$$ language plpgsql;

call do_commits();
