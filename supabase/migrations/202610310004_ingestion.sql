create table public.rewindr_ingestions (
  id uuid primary key,
  owner_id uuid not null,
  title text not null check(length(title) between 1 and 200),
  index_id text not null,
  source_duration_seconds numeric not null check(source_duration_seconds between 4 and 43200),
  source_offset_seconds numeric not null check(source_offset_seconds>=0 and source_offset_seconds<'Infinity'::numeric),
  source_fingerprint text not null check(source_fingerprint ~ '^[a-f0-9]{64}$'),
  sealed_at timestamptz,
  created_at timestamptz not null default now(),
  unique(owner_id,id),
  foreign key(owner_id,id) references public.rewindr_broadcasts(owner_id,id) on delete cascade
);
alter table public.rewindr_jobs drop constraint rewindr_jobs_kind_check,drop constraint rewindr_jobs_check;
alter table public.rewindr_jobs add constraint rewindr_jobs_kind_check check(kind in ('recording','chunk_upload','ingest_chunk'));
alter table public.rewindr_jobs add constraint rewindr_jobs_check check(
  (kind='recording' and recording_id is not null and broadcast_id is null) or
  (kind in ('chunk_upload','ingest_chunk') and recording_id is null and broadcast_id is not null));

create table public.rewindr_ingest_chunks (
  id uuid primary key,
  owner_id uuid not null,
  ingestion_id uuid not null,
  ordinal integer not null check(ordinal between 0 and 719),
  filename text not null,
  start_seconds numeric not null check(start_seconds>=0 and start_seconds<'Infinity'::numeric),
  end_seconds numeric not null check(end_seconds-start_seconds between 4 and 65),
  file_id uuid not null,
  sha256 text not null check(sha256 ~ '^[a-f0-9]{64}$'),
  bytes bigint not null check(bytes between 1 and 50000000),
  job_id uuid not null unique references public.rewindr_jobs(id) on delete cascade,
  checkpoint jsonb not null default '{"phase":"queued"}',
  created_at timestamptz not null default now(),
  unique(ingestion_id,ordinal),
  foreign key(owner_id,ingestion_id) references public.rewindr_ingestions(owner_id,id) on delete cascade,
  check(checkpoint->>'phase' is not null and checkpoint->>'phase' in ('queued','stored','creating_upload','uploading','asset_processing','submitting_index','indexing','ready','failed')),
  check(not(checkpoint ? 'storage') or coalesce((checkpoint->'storage'->>'sha256'=sha256 and (checkpoint->'storage'->>'bytes')::bigint=bytes
    and checkpoint->'storage'->>'bucket' ~ '^[a-z0-9][a-z0-9_-]{1,62}$'
    and checkpoint->'storage'->>'path'=owner_id::text||'/'||ingestion_id::text||'/'||encode(sha256(convert_to('ingest:'||id::text,'UTF8')),'hex')||'/'||sha256||'.mp4'),false)),
  check(checkpoint->>'phase'<>'ready' or coalesce((checkpoint ? 'storage' and length(checkpoint->>'indexedAssetId')>0 and length(checkpoint->>'assetId')>0),false))
);
create index rewindr_ingest_chunks_owner_source on public.rewindr_ingest_chunks(owner_id,ingestion_id,ordinal);
create unique index rewindr_ingest_asset_unique on public.rewindr_ingest_chunks(ingestion_id,(checkpoint->>'indexedAssetId')) where checkpoint ? 'indexedAssetId';
alter table public.rewindr_ingestions enable row level security;
alter table public.rewindr_ingest_chunks enable row level security;
revoke all on public.rewindr_ingestions,public.rewindr_ingest_chunks from public,anon,authenticated;
grant select on public.rewindr_ingestions,public.rewindr_ingest_chunks to service_role;

create function public.rewindr_create_ingestion(p_owner_id uuid,p_input jsonb) returns jsonb
language plpgsql security definer set search_path=pg_catalog,public as $$
declare i public.rewindr_ingestions; v_id uuid:=(p_input->>'id')::uuid;
begin
  select * into i from public.rewindr_ingestions where id=v_id;
  if found then
    if i.owner_id<>p_owner_id or i.title is distinct from p_input->>'title' or i.index_id is distinct from p_input->>'indexId'
      or i.source_duration_seconds is distinct from (p_input->>'sourceDurationSeconds')::numeric
      or i.source_offset_seconds is distinct from (p_input->>'sourceOffsetSeconds')::numeric or i.source_fingerprint is distinct from p_input->>'sourceFingerprint' then
      raise exception 'Ingestion identity conflict' using errcode='23514'; end if;
    return to_jsonb(i);
  end if;
  insert into public.rewindr_broadcasts(id,owner_id,title,index_id,created_at,source_offset_seconds)
    values(v_id,p_owner_id,p_input->>'title',p_input->>'indexId',clock_timestamp(),(p_input->>'sourceOffsetSeconds')::numeric);
  insert into public.rewindr_ingestions(id,owner_id,title,index_id,source_duration_seconds,source_offset_seconds,source_fingerprint)
    values(v_id,p_owner_id,p_input->>'title',p_input->>'indexId',(p_input->>'sourceDurationSeconds')::numeric,(p_input->>'sourceOffsetSeconds')::numeric,p_input->>'sourceFingerprint') returning * into i;
  return to_jsonb(i);
end;
$$;

create function public.rewindr_accept_ingest_chunk(p_owner_id uuid,p_ingestion_id uuid,p_chunk jsonb,p_queue_name text default 'rewindr_jobs') returns jsonb
language plpgsql security definer set search_path=pg_catalog,public as $$
declare i public.rewindr_ingestions;c public.rewindr_ingest_chunks;j public.rewindr_jobs;m bigint;
  n integer;v_ordinal integer:=(p_chunk->>'ordinal')::integer;v_id uuid:=(p_chunk->>'fileId')::uuid;expected_end numeric;
begin
  if p_queue_name !~ '^rewindr_(jobs|verify_[a-f0-9]{32})$' then raise exception 'Invalid queue' using errcode='23514'; end if;
  select * into i from public.rewindr_ingestions where id=p_ingestion_id and owner_id=p_owner_id for update;
  if not found then raise exception 'Ingestion not found' using errcode='23503'; end if;
  n:=greatest(0,floor((i.source_duration_seconds-4.1)/60)::integer)+1;
  expected_end:=case when v_ordinal=n-1 then i.source_duration_seconds else (v_ordinal+1)*60 end;
  if v_ordinal not between 0 and n-1 or p_chunk->>'filename' is distinct from 'chunk_'||lpad(v_ordinal::text,4,'0')||'.mp4'
    or abs((p_chunk->>'startSeconds')::numeric-v_ordinal*60)>0.001 or abs((p_chunk->>'endSeconds')::numeric-expected_end)>0.001 then
    raise exception 'Invalid source interval' using errcode='23514'; end if;
  select * into c from public.rewindr_ingest_chunks where ingestion_id=i.id and ordinal=v_ordinal for update;
  if found then
    if c.sha256 is distinct from p_chunk->>'sha256' or c.bytes is distinct from (p_chunk->>'bytes')::bigint then raise exception 'Chunk identity conflict' using errcode='23514'; end if;
    select * into j from public.rewindr_jobs where id=c.job_id for update;
    if j.state='failed' then
      update public.rewindr_ingest_chunks set file_id=v_id where id=c.id returning * into c;
      select * into m from pgmq.send(j.queue_name,jsonb_build_object('jobId',j.id));
      update public.rewindr_jobs set state='queued',failures=0,error=null,message_id=m,available_at=clock_timestamp(),updated_at=clock_timestamp() where id=j.id;
    end if;
    return to_jsonb(c);
  end if;
  if i.sealed_at is not null then raise exception 'Ingestion already sealed' using errcode='23514'; end if;
  insert into public.rewindr_jobs(owner_id,queue_name,kind,dedupe_key,broadcast_id,input)
    values(p_owner_id,p_queue_name,'ingest_chunk',i.id::text||':'||v_ordinal,i.id,jsonb_build_object('chunkId',v_id)) returning * into j;
  insert into public.rewindr_ingest_chunks(id,owner_id,ingestion_id,ordinal,filename,start_seconds,end_seconds,file_id,sha256,bytes,job_id)
    values(v_id,p_owner_id,i.id,v_ordinal,p_chunk->>'filename',v_ordinal*60,expected_end,v_id,p_chunk->>'sha256',(p_chunk->>'bytes')::bigint,j.id) returning * into c;
  select * into m from pgmq.send(p_queue_name,jsonb_build_object('jobId',j.id));
  update public.rewindr_jobs set message_id=m where id=j.id;
  return to_jsonb(c);
end;
$$;

create function public.rewindr_checkpoint_ingest_chunk(p_job_id uuid,p_lease_token uuid,p_checkpoint jsonb,p_complete boolean default false) returns void
language plpgsql security definer set search_path=pg_catalog,public as $$
declare j public.rewindr_jobs;c public.rewindr_ingest_chunks;old jsonb;
begin
  select * into j from public.rewindr_jobs where id=p_job_id and kind='ingest_chunk' and state='running' and lease_token=p_lease_token and lease_until>clock_timestamp() for update;
  if not found then raise exception 'Job lease lost' using errcode='P0002'; end if;
  select * into c from public.rewindr_ingest_chunks where id=(j.input->>'chunkId')::uuid and job_id=j.id and owner_id=j.owner_id and ingestion_id=j.broadcast_id for update;
  if not found then raise exception 'Ingest chunk not found' using errcode='23503'; end if;
  old:=c.checkpoint;
  if (old ? 'storage' and old->'storage' is distinct from p_checkpoint->'storage')
    or (old ? 'assetId' and old->>'assetId' is distinct from p_checkpoint->>'assetId')
    or (old ? 'indexedAssetId' and old->>'indexedAssetId' is distinct from p_checkpoint->>'indexedAssetId')
    or (old ? 'upload' and ((old->'upload')-'completedChunks') is distinct from ((p_checkpoint->'upload')-'completedChunks')) then
    raise exception 'Provider or media identity is immutable' using errcode='23514'; end if;
  if p_complete then
    if old->>'phase'<>'indexing' or p_checkpoint->>'phase'<>'ready' or p_checkpoint->>'providerStatus'<>'ready' then raise exception 'Chunk is not ready' using errcode='23514'; end if;
  elsif p_checkpoint->>'phase'='ready' then raise exception 'Use atomic completion' using errcode='23514'; end if;
  update public.rewindr_ingest_chunks set checkpoint=p_checkpoint where id=c.id;
  if p_complete then
    perform pgmq.archive(j.queue_name,j.message_id);
    update public.rewindr_jobs set state='completed',failures=0,error=null,lease_token=null,lease_until=null,updated_at=clock_timestamp() where id=j.id;
  end if;
end;
$$;

create function public.rewindr_seal_ingestion(p_owner_id uuid,p_id uuid) returns void
language plpgsql security definer set search_path=pg_catalog,public as $$
declare i public.rewindr_ingestions;n integer;actual integer;
begin
  select * into i from public.rewindr_ingestions where id=p_id and owner_id=p_owner_id for update;
  if not found then raise exception 'Ingestion not found' using errcode='23503'; end if;
  n:=greatest(0,floor((i.source_duration_seconds-4.1)/60)::integer)+1;
  select count(*) into actual from public.rewindr_ingest_chunks where ingestion_id=i.id and owner_id=p_owner_id;
  if actual<>n then raise exception 'Source chunks are missing' using errcode='23514'; end if;
  update public.rewindr_ingestions set sealed_at=coalesce(sealed_at,clock_timestamp()) where id=i.id;
end;
$$;

create function public.rewindr_get_ingestion(p_owner_id uuid,p_id uuid) returns jsonb
language sql stable security invoker set search_path=pg_catalog,public as $$
  select jsonb_build_object('ingestion',to_jsonb(i),'chunks',(select coalesce(jsonb_agg(jsonb_build_object('id',c.id,'ordinal',c.ordinal,'filename',c.filename,
    'startSeconds',c.start_seconds,'endSeconds',c.end_seconds,'phase',c.checkpoint->>'phase','stored',c.checkpoint ? 'storage',
    'jobId',j.id,'jobState',j.state,'attempts',j.attempts,'error',coalesce(j.error,c.checkpoint->>'error')) order by c.ordinal),'[]'::jsonb)
    from public.rewindr_ingest_chunks c join public.rewindr_jobs j on j.id=c.job_id where c.ingestion_id=i.id and c.owner_id=p_owner_id))
  from public.rewindr_ingestions i where i.id=p_id and i.owner_id=p_owner_id;
$$;

-- Only indexed-ready ingestion chunks enter the existing search/playback model.
create or replace function public.rewindr_get_broadcast(p_owner_id uuid,p_id uuid) returns jsonb
language sql stable security invoker set search_path=pg_catalog,public as $$
  select jsonb_strip_nulls(jsonb_build_object('id',b.id,'title',b.title,'indexId',b.index_id,'createdAt',b.created_at,
    'sourceOffsetSeconds',b.source_offset_seconds,'sourceRecordingId',b.source_recording_id,
    'ingestionId',(select id from public.rewindr_ingestions where id=b.id and owner_id=p_owner_id),
    'chunks',(select coalesce(jsonb_agg(item order by source_start),'[]'::jsonb) from (
      select c.start_seconds source_start,jsonb_strip_nulls(jsonb_build_object('filename',c.filename,'indexedAssetId',c.indexed_asset_id,'assetId',c.asset_id,
        'startSeconds',c.start_seconds,'endSeconds',c.end_seconds,'storage',case when c.storage_path is not null then jsonb_build_object('bucket',c.storage_bucket,
        'path',c.storage_path,'sha256',c.storage_sha256,'bytes',c.storage_bytes,'uploadedAt',c.storage_uploaded_at) else null end)) item
        from public.rewindr_chunks c where c.broadcast_id=b.id and c.owner_id=p_owner_id
      union all
      select c.start_seconds,jsonb_build_object('filename',c.filename,'indexedAssetId',c.checkpoint->>'indexedAssetId','assetId',c.checkpoint->>'assetId',
        'startSeconds',c.start_seconds,'endSeconds',c.end_seconds,'storage',c.checkpoint->'storage','storageKey','ingest:'||c.id::text)
        from public.rewindr_ingest_chunks c where c.ingestion_id=b.id and c.owner_id=p_owner_id and c.checkpoint->>'phase'='ready'
    ) all_chunks))) from public.rewindr_broadcasts b where b.id=p_id and b.owner_id=p_owner_id;
$$;

revoke all on function public.rewindr_create_ingestion(uuid,jsonb),public.rewindr_accept_ingest_chunk(uuid,uuid,jsonb,text),
  public.rewindr_checkpoint_ingest_chunk(uuid,uuid,jsonb,boolean),public.rewindr_seal_ingestion(uuid,uuid),public.rewindr_get_ingestion(uuid,uuid) from public,anon,authenticated;
grant execute on function public.rewindr_create_ingestion(uuid,jsonb),public.rewindr_accept_ingest_chunk(uuid,uuid,jsonb,text),
  public.rewindr_checkpoint_ingest_chunk(uuid,uuid,jsonb,boolean),public.rewindr_seal_ingestion(uuid,uuid),public.rewindr_get_ingestion(uuid,uuid) to service_role;

-- Existing contracts extended without changing applied migrations.
create or replace function public.rewindr_save_broadcast(p_owner_id uuid,p_broadcast jsonb)
returns void language plpgsql security invoker set search_path=pg_catalog,public as $$
declare
  v_id uuid := (p_broadcast->>'id')::uuid;
  v_chunk jsonb;
  v_ordinal integer := 0;
  v_media jsonb;
  v_ref jsonb;
  v_old_index text;
begin
  if exists(select 1 from public.rewindr_ingestions where id=v_id) then raise exception 'Use ingestion operations for this broadcast' using errcode='23514'; end if;
  if jsonb_typeof(p_broadcast->'chunks') is distinct from 'array' or jsonb_array_length(p_broadcast->'chunks') not between 1 and 2000 then
    raise exception 'Invalid chunk array' using errcode='23514';
  end if;
  if exists(select 1 from public.rewindr_broadcasts where id=v_id and owner_id<>p_owner_id) then
    raise exception 'Broadcast belongs to another owner' using errcode='42501';
  end if;
  select index_id into v_old_index from public.rewindr_broadcasts where id=v_id and owner_id=p_owner_id for update;
  insert into public.rewindr_broadcasts(id,owner_id,title,index_id,created_at,source_offset_seconds,source_recording_id)
    values(v_id,p_owner_id,p_broadcast->>'title',p_broadcast->>'indexId',(p_broadcast->>'createdAt')::timestamptz,
      (p_broadcast->>'sourceOffsetSeconds')::numeric,(p_broadcast->>'sourceRecordingId')::uuid)
    on conflict(id) do update set title=excluded.title,index_id=excluded.index_id,
      source_offset_seconds=excluded.source_offset_seconds,source_recording_id=excluded.source_recording_id
      where rewindr_broadcasts.owner_id=excluded.owner_id;
  if not found then raise exception 'Broadcast owner conflict' using errcode='42501'; end if;
  -- An existing attached file cannot be reassigned to different footage by a metadata update.
  if exists(select 1 from public.rewindr_chunks c where c.broadcast_id=v_id and c.owner_id=p_owner_id and c.storage_path is not null
    and (v_old_index is distinct from p_broadcast->>'indexId' or not exists(
      select 1 from jsonb_array_elements(p_broadcast->'chunks') j where j->>'indexedAssetId'=c.indexed_asset_id
        and j->>'filename'=c.filename and (j->>'startSeconds')::numeric=c.start_seconds and (j->>'endSeconds')::numeric=c.end_seconds))) then
    raise exception 'Cannot remap or remove chunks with stored media' using errcode='23514';
  end if;
  select coalesce(jsonb_object_agg(indexed_asset_id,jsonb_build_object('path',storage_path,'bucket',storage_bucket,
    'sha256',storage_sha256,'bytes',storage_bytes,'uploadedAt',storage_uploaded_at)),'{}'::jsonb) into v_media
    from public.rewindr_chunks where broadcast_id=v_id and owner_id=p_owner_id and storage_path is not null;
  delete from public.rewindr_chunks where broadcast_id=v_id and owner_id=p_owner_id;
  for v_chunk in select value from jsonb_array_elements(p_broadcast->'chunks') loop
    v_ref := v_media->(v_chunk->>'indexedAssetId');
    insert into public.rewindr_chunks(broadcast_id,owner_id,ordinal,filename,start_seconds,end_seconds,indexed_asset_id,asset_id,
      storage_path,storage_bucket,storage_sha256,storage_bytes,storage_uploaded_at)
      values(v_id,p_owner_id,v_ordinal,v_chunk->>'filename',(v_chunk->>'startSeconds')::numeric,(v_chunk->>'endSeconds')::numeric,
        v_chunk->>'indexedAssetId',v_chunk->>'assetId',v_ref->>'path',v_ref->>'bucket',v_ref->>'sha256',
        (v_ref->>'bytes')::bigint,(v_ref->>'uploadedAt')::timestamptz);
    v_ordinal := v_ordinal+1;
  end loop;
  if exists(select 1 from(select start_seconds,lag(end_seconds) over(order by start_seconds,ordinal) previous_end
    from public.rewindr_chunks where broadcast_id=v_id and owner_id=p_owner_id) ordered where start_seconds<previous_end-0.1) then
    raise exception 'Overlapping chunks' using errcode='23514';
  end if;
end;
$$;

create or replace function public.rewindr_finish_job(p_job_id uuid,p_lease_token uuid,p_outcome text,p_delay_seconds integer default 0,p_error text default null)
returns void language plpgsql security definer set search_path=pg_catalog,public as $$
declare j public.rewindr_jobs; m bigint; next_state text; failure_count integer; previous_phase text;
begin
  if p_outcome not in ('completed','pending','retry','failed','needs_review') or p_delay_seconds not between 0 and 300
    or length(p_error)>500 then raise exception 'Invalid job outcome' using errcode='23514'; end if;
  select * into j from public.rewindr_jobs where id=p_job_id and state='running' and lease_token=p_lease_token and lease_until>clock_timestamp() for update;
  if not found then raise exception 'Job lease lost' using errcode='P0002'; end if;
  if j.kind in ('chunk_upload','ingest_chunk') and p_outcome='completed' then raise exception 'Use atomic chunk completion' using errcode='23514'; end if;
  failure_count:=case when p_outcome='retry' then j.failures+1 when p_outcome='pending' then 0 else j.failures end;
  next_state:=case when p_outcome='pending' then 'queued' when p_outcome='retry' and failure_count<5 then 'retrying' when p_outcome='retry' then 'failed' else p_outcome end;
  if next_state in ('failed','needs_review') and j.kind='recording' then
    select phase into previous_phase from public.rewindr_recordings where id=j.recording_id and owner_id=j.owner_id for update;
    if previous_phase in ('creating_index','creating_upload','submitting_index') then next_state:='needs_review'; end if;
    update public.rewindr_recordings set phase='failed',error=coalesce(p_error,'Background job stopped; inspect its job status.'),
      provider_error=case when previous_phase in ('creating_index','creating_upload','submitting_index') then jsonb_build_object('operation',previous_phase) else provider_error end,
      updated_at=clock_timestamp() where id=j.recording_id and owner_id=j.owner_id;
  end if;
  if next_state='failed' and j.kind='ingest_chunk' then
    select checkpoint->>'phase' into previous_phase from public.rewindr_ingest_chunks where job_id=j.id for update;
    if previous_phase in ('creating_upload','submitting_index') then next_state:='needs_review'; end if;
  end if;
  perform pgmq.archive(j.queue_name,j.message_id);
  if next_state in ('queued','retrying') then select * into m from pgmq.send(j.queue_name,jsonb_build_object('jobId',j.id),p_delay_seconds); end if;
  update public.rewindr_jobs set state=next_state,failures=failure_count,message_id=coalesce(m,message_id),lease_token=null,lease_until=null,
    error=case when next_state='completed' or p_outcome='pending' then null else p_error end,
    available_at=clock_timestamp()+make_interval(secs=>p_delay_seconds),updated_at=clock_timestamp() where id=j.id;
end;
$$;

create or replace function public.rewindr_retry_job(p_owner_id uuid,p_job_id uuid) returns jsonb
language plpgsql security definer set search_path=pg_catalog,public as $$
declare j public.rewindr_jobs; r public.rewindr_recordings; m bigint; cp jsonb;
begin
  select * into j from public.rewindr_jobs where id=p_job_id and owner_id=p_owner_id for update;
  if not found then raise exception 'Job not found' using errcode='P0002'; end if;
  if j.state<>'failed' then raise exception 'Only failed jobs can retry; needs_review requires provider reconciliation' using errcode='23514'; end if;
  if j.kind='recording' then
    select * into r from public.rewindr_recordings where id=j.recording_id and owner_id=j.owner_id for update;
    if r.phase='failed' then
      if r.provider_error->>'operation' in ('creating_index','creating_upload','submitting_index') then raise exception 'Review provider submission first' using errcode='23514'; end if;
      update public.rewindr_recordings set phase=case when r.indexed_asset_id is not null then 'indexing' when r.upload is not null and (r.upload->>'completedChunks')::integer<(r.upload->>'totalChunks')::integer then 'uploading'
        when r.asset_id is not null then 'asset_processing' else 'queued' end,error=null,poll_errors=0,updated_at=clock_timestamp() where id=r.id;
    end if;
  end if;
  if j.kind='ingest_chunk' then
    select checkpoint into cp from public.rewindr_ingest_chunks where job_id=j.id for update;
    if cp->>'phase' in ('creating_upload','submitting_index') then raise exception 'Review provider submission first' using errcode='23514'; end if;
    if cp->>'phase'='failed' then
      cp:=jsonb_set(cp-'error','{phase}',to_jsonb(case when cp ? 'indexedAssetId' then 'indexing' when cp ? 'upload' then 'uploading' when cp ? 'assetId' then 'asset_processing' when cp ? 'storage' then 'stored' else 'queued' end));
      update public.rewindr_ingest_chunks set checkpoint=cp where job_id=j.id;
    end if;
  end if;
  select * into m from pgmq.send(j.queue_name,jsonb_build_object('jobId',j.id));
  update public.rewindr_jobs set state='queued',message_id=m,failures=0,error=null,available_at=clock_timestamp(),updated_at=clock_timestamp() where id=j.id returning * into j;
  return to_jsonb(j);
end;
$$;
