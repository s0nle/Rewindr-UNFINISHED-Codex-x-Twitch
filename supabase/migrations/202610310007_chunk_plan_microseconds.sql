-- Fix recorded chunk boundaries with integer microseconds; applied migrations stay immutable.
create or replace function public.rewindr_accept_ingest_chunk(p_owner_id uuid,p_ingestion_id uuid,p_chunk jsonb,p_queue_name text default 'rewindr_jobs') returns jsonb
language plpgsql security definer set search_path=pg_catalog,public as $$
declare i public.rewindr_ingestions;c public.rewindr_ingest_chunks;j public.rewindr_jobs;m bigint;
  n integer;v_ordinal integer:=(p_chunk->>'ordinal')::integer;v_id uuid:=(p_chunk->>'fileId')::uuid;
  v_start numeric:=(p_chunk->>'startSeconds')::numeric;v_end numeric:=(p_chunk->>'endSeconds')::numeric;expected_end numeric;previous_end numeric;duration_us bigint;
begin
  if p_queue_name !~ '^rewindr_(jobs|verify_[a-f0-9]{32})$' then raise exception 'Invalid queue' using errcode='23514'; end if;
  select * into i from public.rewindr_ingestions where id=p_ingestion_id and owner_id=p_owner_id for update;
  if not found then raise exception 'Ingestion not found' using errcode='23503'; end if;
  if v_ordinal not between 0 and 719 or p_chunk->>'filename' is distinct from 'chunk_'||lpad(v_ordinal::text,4,'0')||'.mp4' then raise exception 'Invalid chunk identity' using errcode='23514'; end if;
  select * into c from public.rewindr_ingest_chunks where ingestion_id=i.id and ordinal=v_ordinal for update;
  if found then
    if c.sha256 is distinct from p_chunk->>'sha256' or c.bytes is distinct from (p_chunk->>'bytes')::bigint or c.start_seconds is distinct from v_start or c.end_seconds is distinct from v_end then raise exception 'Chunk identity conflict' using errcode='23514'; end if;
    select * into j from public.rewindr_jobs where id=c.job_id for update;
    if j.state='failed' then
      update public.rewindr_ingest_chunks set file_id=v_id where id=c.id returning * into c;
      select * into m from pgmq.send(j.queue_name,jsonb_build_object('jobId',j.id));
      update public.rewindr_jobs set state='queued',failures=0,error=null,message_id=m,available_at=clock_timestamp(),updated_at=clock_timestamp() where id=j.id;
    end if;
    return to_jsonb(c);
  end if;
  if i.sealed_at is not null then raise exception 'Ingestion already sealed' using errcode='23514'; end if;
  if i.mode='live' then
    select count(*),coalesce(max(end_seconds),0) into n,previous_end from public.rewindr_ingest_chunks where ingestion_id=i.id and owner_id=p_owner_id;
    if v_ordinal<>n or v_start<previous_end or v_end-v_start not between 4 and 65 or v_end>43200 then raise exception 'Invalid live source interval' using errcode='23514'; end if;
  else
    duration_us:=round(i.source_duration_seconds*1000000)::bigint;
    n:=greatest(0,(duration_us-4100000)/60000000)::integer+1;
    expected_end:=case when v_ordinal=n-1 then duration_us/1000000::numeric else (v_ordinal+1)*60 end;
    if v_ordinal not between 0 and n-1 or abs(v_start-v_ordinal*60)>0.001 or abs(v_end-expected_end)>0.001 then raise exception 'Invalid recorded source interval' using errcode='23514'; end if;
  end if;
  insert into public.rewindr_jobs(owner_id,queue_name,kind,dedupe_key,broadcast_id,input)
    values(p_owner_id,p_queue_name,'ingest_chunk',i.id::text||':'||v_ordinal,i.id,jsonb_build_object('chunkId',v_id)) returning * into j;
  insert into public.rewindr_ingest_chunks(id,owner_id,ingestion_id,ordinal,filename,start_seconds,end_seconds,file_id,sha256,bytes,job_id)
    values(v_id,p_owner_id,i.id,v_ordinal,p_chunk->>'filename',v_start,v_end,v_id,p_chunk->>'sha256',(p_chunk->>'bytes')::bigint,j.id) returning * into c;
  select * into m from pgmq.send(p_queue_name,jsonb_build_object('jobId',j.id));
  update public.rewindr_jobs set message_id=m where id=j.id;
  if i.mode='live' then update public.rewindr_ingestions set source_duration_seconds=v_end where id=i.id; end if;
  return to_jsonb(c);
end;
$$;

create or replace function public.rewindr_seal_ingestion(p_owner_id uuid,p_id uuid) returns void
language plpgsql security definer set search_path=pg_catalog,public as $$
declare i public.rewindr_ingestions;n integer;actual integer;duration_us bigint;
begin
  select * into i from public.rewindr_ingestions where id=p_id and owner_id=p_owner_id for update;
  if not found then raise exception 'Ingestion not found' using errcode='23503'; end if;
  if i.mode='recorded' then
    duration_us:=round(i.source_duration_seconds*1000000)::bigint;
    n:=greatest(0,(duration_us-4100000)/60000000)::integer+1;
    select count(*) into actual from public.rewindr_ingest_chunks where ingestion_id=i.id and owner_id=p_owner_id;
    if actual<>n then raise exception 'Source chunks are missing' using errcode='23514'; end if;
  end if;
  update public.rewindr_ingestions set sealed_at=coalesce(sealed_at,clock_timestamp()) where id=i.id;
end;
$$;
