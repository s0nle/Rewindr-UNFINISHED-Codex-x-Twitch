alter table public.rewindr_ingestions add column mode text not null default 'recorded' check(mode in ('recorded','live'));
alter table public.rewindr_ingestions drop constraint rewindr_ingestions_source_duration_seconds_check;
alter table public.rewindr_ingestions add constraint rewindr_ingestions_source_duration_seconds_check
  check(source_duration_seconds between 0 and 43200 and (mode='live' or source_duration_seconds>=4));

create function public.rewindr_create_live_ingestion(p_owner_id uuid,p_input jsonb) returns jsonb
language plpgsql security definer set search_path=pg_catalog,public as $$
declare i public.rewindr_ingestions;v_id uuid:=(p_input->>'id')::uuid;
begin
  select * into i from public.rewindr_ingestions where id=v_id;
  if found then
    if i.owner_id<>p_owner_id or i.mode<>'live' or i.title is distinct from p_input->>'title' or i.index_id is distinct from p_input->>'indexId'
      or i.source_offset_seconds is distinct from (p_input->>'sourceOffsetSeconds')::numeric or i.source_fingerprint is distinct from p_input->>'sourceFingerprint' then
      raise exception 'Live ingestion identity conflict' using errcode='23514'; end if;
    return to_jsonb(i);
  end if;
  if p_input->>'mode' is distinct from 'live' then raise exception 'Expected live source' using errcode='23514'; end if;
  insert into public.rewindr_broadcasts(id,owner_id,title,index_id,created_at,source_offset_seconds)
    values(v_id,p_owner_id,p_input->>'title',p_input->>'indexId',clock_timestamp(),(p_input->>'sourceOffsetSeconds')::numeric);
  insert into public.rewindr_ingestions(id,owner_id,title,index_id,source_duration_seconds,source_offset_seconds,source_fingerprint,mode)
    values(v_id,p_owner_id,p_input->>'title',p_input->>'indexId',0,(p_input->>'sourceOffsetSeconds')::numeric,p_input->>'sourceFingerprint','live') returning * into i;
  return to_jsonb(i);
end;
$$;
revoke all on function public.rewindr_create_live_ingestion(uuid,jsonb) from public,anon,authenticated;
grant execute on function public.rewindr_create_live_ingestion(uuid,jsonb) to service_role;

create or replace function public.rewindr_accept_ingest_chunk(p_owner_id uuid,p_ingestion_id uuid,p_chunk jsonb,p_queue_name text default 'rewindr_jobs') returns jsonb
language plpgsql security definer set search_path=pg_catalog,public as $$
declare i public.rewindr_ingestions;c public.rewindr_ingest_chunks;j public.rewindr_jobs;m bigint;
  n integer;v_ordinal integer:=(p_chunk->>'ordinal')::integer;v_id uuid:=(p_chunk->>'fileId')::uuid;
  v_start numeric:=(p_chunk->>'startSeconds')::numeric;v_end numeric:=(p_chunk->>'endSeconds')::numeric;expected_end numeric;previous_end numeric;
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
    n:=greatest(0,floor((i.source_duration_seconds-4.1)/60)::integer)+1;
    expected_end:=case when v_ordinal=n-1 then i.source_duration_seconds else (v_ordinal+1)*60 end;
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
declare i public.rewindr_ingestions;n integer;actual integer;
begin
  select * into i from public.rewindr_ingestions where id=p_id and owner_id=p_owner_id for update;
  if not found then raise exception 'Ingestion not found' using errcode='23503'; end if;
  if i.mode='recorded' then
    n:=greatest(0,floor((i.source_duration_seconds-4.1)/60)::integer)+1;
    select count(*) into actual from public.rewindr_ingest_chunks where ingestion_id=i.id and owner_id=p_owner_id;
    if actual<>n then raise exception 'Source chunks are missing' using errcode='23514'; end if;
  end if;
  update public.rewindr_ingestions set sealed_at=coalesce(sealed_at,clock_timestamp()) where id=i.id;
end;
$$;
