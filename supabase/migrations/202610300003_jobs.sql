-- Durable delivery uses Supabase Queues (pgmq). Job rows hold application state.
create extension if not exists pgmq;
select pgmq.create('rewindr_jobs');

create table public.rewindr_jobs (
  id uuid primary key default gen_random_uuid(),
  owner_id uuid not null references public.rewindr_broadcasters(id) on delete cascade,
  queue_name text not null check (queue_name ~ '^rewindr_(jobs|verify_[a-f0-9]{32})$'),
  kind text not null check (kind in ('recording','chunk_upload')),
  dedupe_key text not null,
  recording_id uuid,
  broadcast_id uuid,
  input jsonb not null default '{}'::jsonb,
  state text not null default 'queued' check (state in ('queued','running','retrying','completed','failed','needs_review')),
  attempts integer not null default 0 check (attempts >= 0),
  failures integer not null default 0 check (failures >= 0),
  message_id bigint,
  lease_token uuid,
  lease_until timestamptz,
  worker_id uuid,
  available_at timestamptz not null default now(),
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  error text check (length(error) <= 500),
  unique(owner_id,kind,dedupe_key),
  foreign key(owner_id,recording_id) references public.rewindr_recordings(owner_id,id) on delete cascade,
  foreign key(owner_id,broadcast_id) references public.rewindr_broadcasts(owner_id,id) on delete cascade,
  check ((kind='recording' and recording_id is not null and broadcast_id is null)
      or (kind='chunk_upload' and recording_id is null and broadcast_id is not null))
);
create index rewindr_jobs_owner_state on public.rewindr_jobs(owner_id,state,created_at);
create table public.rewindr_workers (
  id uuid primary key,
  queue_name text not null,
  last_seen_at timestamptz not null default now()
);
alter table public.rewindr_jobs enable row level security;
alter table public.rewindr_workers enable row level security;
revoke all on public.rewindr_jobs,public.rewindr_workers from public,anon,authenticated;
grant select on public.rewindr_jobs,public.rewindr_workers to service_role;
revoke all on pgmq.q_rewindr_jobs,pgmq.a_rewindr_jobs from public,anon,authenticated;

-- These narrow backend-only wrappers do not expose pgmq_public to browsers.
create function public.rewindr_enqueue_job(p_owner_id uuid,p_kind text,p_resource_id uuid,p_input jsonb default '{}'::jsonb,p_queue_name text default 'rewindr_jobs')
returns jsonb language plpgsql security definer set search_path=pg_catalog,public as $$
declare j public.rewindr_jobs; k text; m bigint;
begin
  if p_queue_name !~ '^rewindr_(jobs|verify_[a-f0-9]{32})$' then raise exception 'Invalid queue' using errcode='23514'; end if;
  if p_kind='recording' then
    perform 1 from public.rewindr_recordings where id=p_resource_id and owner_id=p_owner_id;
    if not found then raise exception 'Recording not found' using errcode='23503'; end if;
    k:=p_resource_id::text;
  elsif p_kind='chunk_upload' then
    perform 1 from public.rewindr_chunks where broadcast_id=p_resource_id and owner_id=p_owner_id and indexed_asset_id=p_input->>'indexedAssetId' and filename=p_input->>'filename';
    if not found then raise exception 'Chunk not found' using errcode='23503'; end if;
    if p_input->>'sha256' is null or p_input->>'sha256' !~ '^[a-f0-9]{64}$'
      or coalesce((p_input->>'bytes')::bigint,0) not between 1 and 50000000
      or p_input->>'fileId' is null or (p_input->>'fileId')::uuid is null then raise exception 'Invalid upload job' using errcode='23514'; end if;
    k:=p_resource_id::text || ':' || (p_input->>'indexedAssetId');
  else raise exception 'Invalid job kind' using errcode='23514'; end if;
  insert into public.rewindr_jobs(id,owner_id,queue_name,kind,dedupe_key,recording_id,broadcast_id,input)
    values(case when p_kind='recording' then p_resource_id else gen_random_uuid() end,p_owner_id,p_queue_name,p_kind,k,
      case when p_kind='recording' then p_resource_id end,case when p_kind='chunk_upload' then p_resource_id end,p_input)
    on conflict(owner_id,kind,dedupe_key) do nothing returning * into j;
  if j.id is null then
    select * into j from public.rewindr_jobs where owner_id=p_owner_id and kind=p_kind and dedupe_key=k for update;
    if j.queue_name<>p_queue_name or (p_kind='chunk_upload' and (j.input-'fileId') is distinct from (p_input-'fileId')) then
      raise exception 'Job identity conflict' using errcode='23514';
    end if;
    -- An explicit reupload may repair a missing staged copy after disk failure.
    -- Running/retrying jobs retain their original file; needs_review is not reset.
    if p_kind='chunk_upload' and j.state='failed' then
      select * into m from pgmq.send(p_queue_name,jsonb_build_object('jobId',j.id));
      update public.rewindr_jobs set input=p_input,state='queued',message_id=m,failures=0,error=null,
        available_at=clock_timestamp(),updated_at=clock_timestamp() where id=j.id returning * into j;
    end if;
    return to_jsonb(j);
  end if;
  select * into m from pgmq.send(p_queue_name,jsonb_build_object('jobId',j.id));
  update public.rewindr_jobs set message_id=m where id=j.id returning * into j;
  return to_jsonb(j);
end;
$$;

-- The initial recording metadata and its message commit in the same transaction.
-- Updates/checkpoints never insert a second job. Existing work can be reconciled.
create function public.rewindr_enqueue_new_recording() returns trigger
language plpgsql security definer set search_path=pg_catalog,public as $$
begin
  if new.phase not in ('ready','failed') then perform public.rewindr_enqueue_job(new.owner_id,'recording',new.id); end if;
  return new;
end;
$$;
create trigger rewindr_new_recording_job after insert on public.rewindr_recordings
  for each row execute function public.rewindr_enqueue_new_recording();

create function public.rewindr_claim_job(p_queue_name text,p_worker_id uuid,p_lease_seconds integer default 300)
returns jsonb language plpgsql security definer set search_path=pg_catalog,public as $$
declare msg record; j public.rewindr_jobs;
begin
  if p_queue_name !~ '^rewindr_(jobs|verify_[a-f0-9]{32})$' or p_lease_seconds not between 60 and 900 then raise exception 'Invalid lease' using errcode='23514'; end if;
  insert into public.rewindr_workers(id,queue_name) values(p_worker_id,p_queue_name)
    on conflict(id) do update set queue_name=excluded.queue_name,last_seen_at=clock_timestamp();
  select * into msg from pgmq.read(p_queue_name,p_lease_seconds,1);
  if not found then return null; end if;
  select * into j from public.rewindr_jobs where id=(msg.message->>'jobId')::uuid and queue_name=p_queue_name for update;
  if not found or j.state in ('completed','failed','needs_review') then perform pgmq.archive(p_queue_name,msg.msg_id); return null; end if;
  if j.lease_until>clock_timestamp() then return null; end if;
  update public.rewindr_jobs set state='running',attempts=attempts+1,lease_token=gen_random_uuid(),worker_id=p_worker_id,
    lease_until=clock_timestamp()+make_interval(secs=>p_lease_seconds),message_id=msg.msg_id,updated_at=clock_timestamp()
    where id=j.id returning * into j;
  return to_jsonb(j);
end;
$$;

create function public.rewindr_renew_job(p_job_id uuid,p_lease_token uuid,p_lease_seconds integer default 300)
returns void language plpgsql security definer set search_path=pg_catalog,public as $$
declare j public.rewindr_jobs;
begin
  if p_lease_seconds not between 60 and 900 then raise exception 'Invalid lease' using errcode='23514'; end if;
  select * into j from public.rewindr_jobs where id=p_job_id and state='running' and lease_token=p_lease_token and lease_until>clock_timestamp() for update;
  if not found then raise exception 'Job lease lost' using errcode='P0002'; end if;
  update public.rewindr_jobs set lease_until=clock_timestamp()+make_interval(secs=>p_lease_seconds),updated_at=clock_timestamp() where id=j.id;
  perform pgmq.set_vt(j.queue_name,j.message_id,p_lease_seconds);
  update public.rewindr_workers set last_seen_at=clock_timestamp() where id=j.worker_id;
end;
$$;

-- Fencing prevents an expired worker from overwriting the next worker's state.
create function public.rewindr_checkpoint_recording(p_job_id uuid,p_lease_token uuid,p_record jsonb)
returns void language plpgsql security definer set search_path=pg_catalog,public as $$
declare j public.rewindr_jobs;
begin
  select * into j from public.rewindr_jobs where id=p_job_id and kind='recording' and state='running' and lease_token=p_lease_token and lease_until>clock_timestamp() for update;
  if not found then raise exception 'Job lease lost' using errcode='P0002'; end if;
  update public.rewindr_recordings set phase=p_record->>'phase',media=p_record->'media',index_id=p_record->>'indexId',asset_id=p_record->>'assetId',
    indexed_asset_id=p_record->>'indexedAssetId',upload=p_record->'upload',provider_status=p_record->>'providerStatus',error=p_record->>'error',
    poll_errors=(p_record->>'pollErrors')::integer,provider_error=p_record->'providerError',updated_at=clock_timestamp()
    where id=j.recording_id and owner_id=j.owner_id and id=(p_record->>'id')::uuid and filename=p_record->>'filename'
      and sha256=p_record->>'sha256' and bytes=(p_record->>'bytes')::bigint;
  if not found then raise exception 'Recording identity conflict' using errcode='23514'; end if;
end;
$$;

create function public.rewindr_complete_chunk_job(p_job_id uuid,p_lease_token uuid,p_storage jsonb)
returns void language plpgsql security definer set search_path=pg_catalog,public as $$
declare j public.rewindr_jobs;
begin
  select * into j from public.rewindr_jobs where id=p_job_id and kind='chunk_upload' and state='running' and lease_token=p_lease_token and lease_until>clock_timestamp() for update;
  if not found then raise exception 'Job lease lost' using errcode='P0002'; end if;
  if j.input->>'sha256' is distinct from p_storage->>'sha256' or (j.input->>'bytes')::bigint is distinct from (p_storage->>'bytes')::bigint then
    raise exception 'Media identity conflict' using errcode='23514'; end if;
  perform public.rewindr_attach_chunk_storage(j.owner_id,j.broadcast_id,j.input->>'indexedAssetId',p_storage);
  perform pgmq.archive(j.queue_name,j.message_id);
  update public.rewindr_jobs set state='completed',lease_token=null,lease_until=null,error=null,failures=0,updated_at=clock_timestamp() where id=j.id;
end;
$$;

create function public.rewindr_finish_job(p_job_id uuid,p_lease_token uuid,p_outcome text,p_delay_seconds integer default 0,p_error text default null)
returns void language plpgsql security definer set search_path=pg_catalog,public as $$
declare j public.rewindr_jobs; m bigint; next_state text; failure_count integer; previous_phase text;
begin
  if p_outcome not in ('completed','pending','retry','failed','needs_review') or p_delay_seconds not between 0 and 300
    or length(p_error)>500 then raise exception 'Invalid job outcome' using errcode='23514'; end if;
  select * into j from public.rewindr_jobs where id=p_job_id and state='running' and lease_token=p_lease_token and lease_until>clock_timestamp() for update;
  if not found then raise exception 'Job lease lost' using errcode='P0002'; end if;
  if j.kind='chunk_upload' and p_outcome='completed' then raise exception 'Use atomic chunk completion' using errcode='23514'; end if;
  failure_count:=case when p_outcome='retry' then j.failures+1 when p_outcome='pending' then 0 else j.failures end;
  next_state:=case when p_outcome='pending' then 'queued' when p_outcome='retry' and failure_count<5 then 'retrying' when p_outcome='retry' then 'failed' else p_outcome end;
  if next_state in ('failed','needs_review') and j.kind='recording' then
    select phase into previous_phase from public.rewindr_recordings where id=j.recording_id and owner_id=j.owner_id for update;
    if previous_phase in ('creating_index','creating_upload','submitting_index') then next_state:='needs_review'; end if;
    update public.rewindr_recordings set phase='failed',error=coalesce(p_error,'Background job stopped; inspect its job status.'),
      provider_error=case when previous_phase in ('creating_index','creating_upload','submitting_index') then jsonb_build_object('operation',previous_phase) else provider_error end,
      updated_at=clock_timestamp() where id=j.recording_id and owner_id=j.owner_id;
  end if;
  perform pgmq.archive(j.queue_name,j.message_id);
  if next_state in ('queued','retrying') then select * into m from pgmq.send(j.queue_name,jsonb_build_object('jobId',j.id),p_delay_seconds); end if;
  update public.rewindr_jobs set state=next_state,failures=failure_count,message_id=coalesce(m,message_id),lease_token=null,lease_until=null,
    error=case when next_state='completed' or p_outcome='pending' then null else p_error end,
    available_at=clock_timestamp()+make_interval(secs=>p_delay_seconds),updated_at=clock_timestamp() where id=j.id;
end;
$$;

create function public.rewindr_retry_job(p_owner_id uuid,p_job_id uuid) returns jsonb
language plpgsql security definer set search_path=pg_catalog,public as $$
declare j public.rewindr_jobs; r public.rewindr_recordings; m bigint;
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
  select * into m from pgmq.send(j.queue_name,jsonb_build_object('jobId',j.id));
  update public.rewindr_jobs set state='queued',message_id=m,failures=0,error=null,available_at=clock_timestamp(),updated_at=clock_timestamp() where id=j.id returning * into j;
  return to_jsonb(j);
end;
$$;

create function public.rewindr_queue_status(p_owner_id uuid,p_queue_name text default 'rewindr_jobs') returns jsonb
language sql security definer set search_path=pg_catalog,public as $$
  select jsonb_build_object('queue',p_queue_name,'pending',count(*) filter(where state in ('queued','running','retrying')),
    'failed',count(*) filter(where state='failed'),'needsReview',count(*) filter(where state='needs_review'),
    'workerLastSeenAt',(select max(last_seen_at) from public.rewindr_workers where queue_name=p_queue_name),
    'workerOnline',exists(select 1 from public.rewindr_workers where queue_name=p_queue_name and last_seen_at>clock_timestamp()-interval '90 seconds'))
  from public.rewindr_jobs where owner_id=p_owner_id and queue_name=p_queue_name;
$$;

revoke all on function public.rewindr_enqueue_job(uuid,text,uuid,jsonb,text),public.rewindr_enqueue_new_recording(),
  public.rewindr_claim_job(text,uuid,integer),public.rewindr_renew_job(uuid,uuid,integer),public.rewindr_checkpoint_recording(uuid,uuid,jsonb),
  public.rewindr_complete_chunk_job(uuid,uuid,jsonb),public.rewindr_finish_job(uuid,uuid,text,integer,text),
  public.rewindr_retry_job(uuid,uuid),public.rewindr_queue_status(uuid,text) from public,anon,authenticated;
grant execute on function public.rewindr_enqueue_job(uuid,text,uuid,jsonb,text),public.rewindr_claim_job(text,uuid,integer),public.rewindr_renew_job(uuid,uuid,integer),
  public.rewindr_checkpoint_recording(uuid,uuid,jsonb),public.rewindr_complete_chunk_job(uuid,uuid,jsonb),public.rewindr_finish_job(uuid,uuid,text,integer,text),
  public.rewindr_retry_job(uuid,uuid),public.rewindr_queue_status(uuid,text) to service_role;
