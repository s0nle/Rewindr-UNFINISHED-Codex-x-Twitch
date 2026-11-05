create table public.rewindr_searches (
  id uuid primary key,
  owner_id uuid not null,
  broadcast_id uuid not null,
  question text not null check(length(question) between 1 and 1000),
  settings jsonb not null,
  snapshot jsonb not null,
  state jsonb not null,
  version integer not null default 0 check(version>=0),
  phase text not null default 'ready' check(phase in ('ready','recovering')),
  pending_token uuid,
  pending_until timestamptz,
  last_request_id uuid,
  created_at timestamptz not null default now(),
  expires_at timestamptz not null default now()+interval '6 hours',
  unique(owner_id,id),
  foreign key(owner_id,broadcast_id) references public.rewindr_broadcasts(owner_id,id) on delete cascade,
  check(snapshot->>'id'=broadcast_id::text and jsonb_typeof(state)='object'),
  check(jsonb_array_length(state->'current')<=3 and jsonb_array_length(state->'rejected')<=12 and jsonb_array_length(state->'pool')<=400 and (state->>'round')::integer between 0 and 3)
);
alter table public.rewindr_jobs drop constraint rewindr_jobs_kind_check,drop constraint rewindr_jobs_check;
alter table public.rewindr_jobs add constraint rewindr_jobs_kind_check check(kind in ('recording','chunk_upload','ingest_chunk','candidate_analysis'));
alter table public.rewindr_jobs add constraint rewindr_jobs_check check((kind='recording' and recording_id is not null and broadcast_id is null) or
  (kind in ('chunk_upload','ingest_chunk','candidate_analysis') and recording_id is null and broadcast_id is not null));
create table public.rewindr_analyses (
  id uuid primary key default gen_random_uuid(),
  owner_id uuid not null,
  broadcast_id uuid not null,
  cache_key text not null check(cache_key ~ '^[a-f0-9]{64}$'),
  question text not null check(length(question) between 1 and 1000),
  model text not null check(model in ('pegasus1.5','pegasus1.6')),
  revision text not null,
  plan jsonb not null check(jsonb_typeof(plan)='array' and jsonb_array_length(plan)<=3),
  checkpoint jsonb not null,
  status text not null check(status in ('queued','processing','ready','failed','needs_review','unavailable','skipped')),
  result jsonb,
  error text check(length(error)<=500),
  job_id uuid unique references public.rewindr_jobs(id) on delete cascade,
  created_at timestamptz not null default now(),
  unique(owner_id,id),unique(owner_id,cache_key),
  foreign key(owner_id,broadcast_id) references public.rewindr_broadcasts(owner_id,id) on delete cascade,
  check(status<>'ready' or result is not null)
);
create table public.rewindr_search_analyses (
  owner_id uuid not null,
  search_id uuid not null,
  candidate_id text not null check(candidate_id ~ '^[a-f0-9]{64}$'),
  analysis_id uuid not null,
  primary key(search_id,candidate_id),
  foreign key(owner_id,search_id) references public.rewindr_searches(owner_id,id) on delete cascade,
  foreign key(owner_id,analysis_id) references public.rewindr_analyses(owner_id,id) on delete cascade
);
create index rewindr_searches_owner_created on public.rewindr_searches(owner_id,created_at);
create index rewindr_search_analysis_cache on public.rewindr_search_analyses(analysis_id);
alter table public.rewindr_searches enable row level security;
alter table public.rewindr_analyses enable row level security;
alter table public.rewindr_search_analyses enable row level security;
revoke all on public.rewindr_searches,public.rewindr_analyses,public.rewindr_search_analyses from public,anon,authenticated;
grant select on public.rewindr_searches,public.rewindr_analyses,public.rewindr_search_analyses to service_role;

create function public.rewindr_create_search(p_owner_id uuid,p_id uuid,p_snapshot jsonb,p_question text,p_settings jsonb,p_state jsonb) returns jsonb
language plpgsql security definer set search_path=pg_catalog,public as $$
declare s public.rewindr_searches;
begin
  insert into public.rewindr_searches(id,owner_id,broadcast_id,question,settings,snapshot,state)
    values(p_id,p_owner_id,(p_snapshot->>'id')::uuid,p_question,p_settings,p_snapshot,p_state) on conflict(id) do nothing;
  select * into s from public.rewindr_searches where id=p_id and owner_id=p_owner_id;
  if not found or s.question is distinct from p_question or s.settings is distinct from p_settings or s.broadcast_id is distinct from (p_snapshot->>'id')::uuid then
    raise exception 'Search identity conflict' using errcode='23514'; end if;
  return to_jsonb(s);
end;
$$;

create function public.rewindr_reserve_recovery(p_owner_id uuid,p_id uuid,p_version integer,p_request_id uuid) returns jsonb
language plpgsql security definer set search_path=pg_catalog,public as $$
declare s public.rewindr_searches; rejected jsonb;
begin
  select * into s from public.rewindr_searches where id=p_id and owner_id=p_owner_id for update;
  if not found then raise exception 'Search not found' using errcode='23503'; end if;
  if s.expires_at<clock_timestamp() then raise exception 'Search expired' using errcode='P0003'; end if;
  if s.last_request_id=p_request_id then return jsonb_build_object('owned',false,'replayed',true,'search',to_jsonb(s)); end if;
  if s.version<>p_version or (s.state->>'round')::integer>=3 then raise exception 'Recovery state changed or limit reached' using errcode='23514'; end if;
  if s.pending_until>clock_timestamp() then
    if s.pending_token=p_request_id then return jsonb_build_object('owned',false,'replayed',false,'search',to_jsonb(s)); end if;
    raise exception 'Recovery already running' using errcode='23514';
  end if;
  select coalesce(jsonb_agg(jsonb_build_object('startSeconds',c->'matchStartSeconds','endSeconds',c->'matchEndSeconds')),'[]'::jsonb) into rejected from jsonb_array_elements(s.state->'current') c;
  update public.rewindr_searches set phase='recovering',pending_token=p_request_id,pending_until=clock_timestamp()+interval '5 minutes',
    state=jsonb_set(jsonb_set(state,'{rejected}',(state->'rejected')||rejected),'{current}','[]'::jsonb)
    where id=s.id returning * into s;
  return jsonb_build_object('owned',true,'replayed',false,'search',to_jsonb(s));
end;
$$;

create function public.rewindr_commit_recovery(p_owner_id uuid,p_id uuid,p_request_id uuid,p_state jsonb) returns jsonb
language plpgsql security definer set search_path=pg_catalog,public as $$
declare s public.rewindr_searches;
begin
  select * into s from public.rewindr_searches where id=p_id and owner_id=p_owner_id and pending_token=p_request_id and pending_until>clock_timestamp() for update;
  if not found then raise exception 'Recovery lease lost' using errcode='23514'; end if;
  if p_state->'rejected' is distinct from s.state->'rejected' or (p_state->>'round')::integer<>(s.state->>'round')::integer+1 then raise exception 'Recovery exclusions are immutable' using errcode='23514'; end if;
  update public.rewindr_searches set state=p_state,version=version+1,phase='ready',last_request_id=p_request_id,pending_token=null,pending_until=null
    where id=s.id returning * into s;
  return to_jsonb(s);
end;
$$;

create function public.rewindr_fail_recovery(p_owner_id uuid,p_id uuid,p_request_id uuid) returns void
language plpgsql security definer set search_path=pg_catalog,public as $$
begin
  update public.rewindr_searches set phase='ready',pending_token=null,pending_until=null,
    state=jsonb_set(state,'{warnings}','["Recovery could not be completed. Previous rejections were kept; retry when the provider is available."]'::jsonb)
    where id=p_id and owner_id=p_owner_id and pending_token=p_request_id;
end;
$$;

create function public.rewindr_close_search(p_owner_id uuid,p_id uuid) returns void
language plpgsql security definer set search_path=pg_catalog,public as $$
begin update public.rewindr_searches set expires_at=least(expires_at,clock_timestamp()) where id=p_id and owner_id=p_owner_id;end;
$$;
revoke all on function public.rewindr_close_search(uuid,uuid) from public,anon,authenticated;
grant execute on function public.rewindr_close_search(uuid,uuid) to service_role;

create function public.rewindr_request_analysis(p_owner_id uuid,p_search_id uuid,p_version integer,p_candidate_id text,p_cache_key text,p_model text,p_revision text,p_plan jsonb,p_available boolean,p_queue_name text default 'rewindr_jobs') returns jsonb
language plpgsql security definer set search_path=pg_catalog,public as $$
declare s public.rewindr_searches;a public.rewindr_analyses;j public.rewindr_jobs;m bigint;cp jsonb;
begin
  if p_queue_name !~ '^rewindr_(jobs|verify_[a-f0-9]{32})$' then raise exception 'Invalid queue' using errcode='23514'; end if;
  select * into s from public.rewindr_searches where id=p_search_id and owner_id=p_owner_id for update;
  if not found or s.version<>p_version or s.phase<>'ready' or s.expires_at<clock_timestamp() or not exists(select 1 from jsonb_array_elements(s.state->'current') c where c->>'candidateId'=p_candidate_id) then
    raise exception 'Displayed candidates changed' using errcode='23514'; end if;
  select jsonb_build_object('parts',coalesce(jsonb_agg(jsonb_build_object('phase','queued')),'[]'::jsonb)) into cp from jsonb_array_elements(p_plan);
  insert into public.rewindr_analyses(owner_id,broadcast_id,cache_key,question,model,revision,plan,checkpoint,status,error)
    values(p_owner_id,s.broadcast_id,p_cache_key,s.question,p_model,p_revision,p_plan,cp,case when p_available then 'queued' else 'unavailable' end,
      case when not p_available then 'Source media or analysis configuration is unavailable. Playback and recovery remain available.' end)
    on conflict(owner_id,cache_key) do nothing;
  select * into a from public.rewindr_analyses where owner_id=p_owner_id and cache_key=p_cache_key for update;
  if a.broadcast_id<>s.broadcast_id or a.question<>s.question or a.model<>p_model or a.revision<>p_revision then raise exception 'Analysis cache identity conflict' using errcode='23514'; end if;
  if a.status='unavailable' and p_available then
    update public.rewindr_analyses set status='queued',error=null where id=a.id returning * into a;
  end if;
  if a.status='skipped' then
    select * into m from pgmq.send(p_queue_name,jsonb_build_object('jobId',a.job_id));
    update public.rewindr_jobs set state='queued',failures=0,error=null,message_id=m,available_at=clock_timestamp(),updated_at=clock_timestamp() where id=a.job_id;
    update public.rewindr_analyses set status='queued',error=null where id=a.id returning * into a;
  elsif a.job_id is null and a.status='queued' then
    insert into public.rewindr_jobs(owner_id,queue_name,kind,dedupe_key,broadcast_id,input)
      values(p_owner_id,p_queue_name,'candidate_analysis',a.id::text,s.broadcast_id,jsonb_build_object('analysisId',a.id)) returning * into j;
    select * into m from pgmq.send(p_queue_name,jsonb_build_object('jobId',j.id));
    update public.rewindr_jobs set message_id=m where id=j.id;
    update public.rewindr_analyses set job_id=j.id where id=a.id returning * into a;
  end if;
  insert into public.rewindr_search_analyses(owner_id,search_id,candidate_id,analysis_id) values(p_owner_id,s.id,p_candidate_id,a.id)
    on conflict(search_id,candidate_id) do update set analysis_id=excluded.analysis_id;
  return to_jsonb(a);
end;
$$;

create function public.rewindr_search_analysis_rows(p_owner_id uuid,p_id uuid) returns jsonb
language sql stable security invoker set search_path=pg_catalog,public as $$
  select coalesce(jsonb_agg(jsonb_build_object('candidateId',l.candidate_id,'analysis',to_jsonb(a))),'[]'::jsonb)
  from public.rewindr_search_analyses l join public.rewindr_analyses a on a.id=l.analysis_id and a.owner_id=l.owner_id
  where l.owner_id=p_owner_id and l.search_id=p_id;
$$;
create function public.rewindr_analysis_active(p_owner_id uuid,p_id uuid) returns boolean
language sql stable security invoker set search_path=pg_catalog,public as $$
  select exists(select 1 from public.rewindr_search_analyses l join public.rewindr_searches s on s.id=l.search_id and s.owner_id=l.owner_id
    where l.owner_id=p_owner_id and l.analysis_id=p_id and s.expires_at>clock_timestamp()
      and exists(select 1 from jsonb_array_elements(s.state->'current') c where c->>'candidateId'=l.candidate_id));
$$;

create function public.rewindr_checkpoint_analysis(p_job_id uuid,p_lease_token uuid,p_checkpoint jsonb,p_status text default 'processing',p_result jsonb default null,p_error text default null) returns void
language plpgsql security definer set search_path=pg_catalog,public as $$
declare j public.rewindr_jobs;a public.rewindr_analyses;k integer;old_id text;new_id text;
begin
  select * into j from public.rewindr_jobs where id=p_job_id and kind='candidate_analysis' and state='running' and lease_token=p_lease_token and lease_until>clock_timestamp() for update;
  if not found then raise exception 'Job lease lost' using errcode='P0002'; end if;
  select * into a from public.rewindr_analyses where id=(j.input->>'analysisId')::uuid and job_id=j.id and owner_id=j.owner_id and broadcast_id=j.broadcast_id for update;
  if not found then raise exception 'Analysis not found' using errcode='23503'; end if;
  if p_status not in ('processing','ready','failed','needs_review','skipped') or jsonb_array_length(p_checkpoint->'parts')<>jsonb_array_length(a.plan) or length(p_error)>500 then raise exception 'Invalid analysis checkpoint' using errcode='23514'; end if;
  for k in 0..jsonb_array_length(a.plan)-1 loop
    old_id:=a.checkpoint->'parts'->k->>'taskId';new_id:=p_checkpoint->'parts'->k->>'taskId';
    if old_id is not null and old_id is distinct from new_id then raise exception 'Analysis task identity is immutable' using errcode='23514'; end if;
  end loop;
  if p_status='ready' and (p_result is null or exists(select 1 from jsonb_array_elements(p_checkpoint->'parts') part where part->>'phase' is distinct from 'ready')) then raise exception 'Analysis is incomplete' using errcode='23514'; end if;
  if p_status='skipped' and exists(select 1 from jsonb_array_elements(p_checkpoint->'parts') part where part->>'phase' in ('submitting','processing')) then raise exception 'Submitted analysis must be reconciled' using errcode='23514'; end if;
  update public.rewindr_analyses set checkpoint=p_checkpoint,status=p_status,result=p_result,error=p_error where id=a.id;
  if p_status<>'processing' then
    perform pgmq.archive(j.queue_name,j.message_id);
    update public.rewindr_jobs set state=case when p_status in ('ready','skipped') then 'completed' else p_status end,
      lease_token=null,lease_until=null,error=p_error,updated_at=clock_timestamp() where id=j.id;
  end if;
end;
$$;

revoke all on function public.rewindr_create_search(uuid,uuid,jsonb,text,jsonb,jsonb),public.rewindr_reserve_recovery(uuid,uuid,integer,uuid),
  public.rewindr_commit_recovery(uuid,uuid,uuid,jsonb),public.rewindr_fail_recovery(uuid,uuid,uuid),
  public.rewindr_request_analysis(uuid,uuid,integer,text,text,text,text,jsonb,boolean,text),public.rewindr_search_analysis_rows(uuid,uuid),
  public.rewindr_analysis_active(uuid,uuid),public.rewindr_checkpoint_analysis(uuid,uuid,jsonb,text,jsonb,text) from public,anon,authenticated;
grant execute on function public.rewindr_create_search(uuid,uuid,jsonb,text,jsonb,jsonb),public.rewindr_reserve_recovery(uuid,uuid,integer,uuid),
  public.rewindr_commit_recovery(uuid,uuid,uuid,jsonb),public.rewindr_fail_recovery(uuid,uuid,uuid),
  public.rewindr_request_analysis(uuid,uuid,integer,text,text,text,text,jsonb,boolean,text),public.rewindr_search_analysis_rows(uuid,uuid),
  public.rewindr_analysis_active(uuid,uuid),public.rewindr_checkpoint_analysis(uuid,uuid,jsonb,text,jsonb,text) to service_role;

-- Extend durable worker contracts; prior migrations stay immutable.
create or replace function public.rewindr_claim_job(p_queue_name text,p_worker_id uuid,p_lease_seconds integer default 300)
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
  if j.kind='candidate_analysis' then
    perform pg_advisory_xact_lock(hashtext('rewindr-analysis-'||j.owner_id::text));
    if (select count(*) from public.rewindr_jobs where owner_id=j.owner_id and kind='candidate_analysis' and state='running' and lease_until>clock_timestamp())>=2 then
      perform pgmq.set_vt(p_queue_name,msg.msg_id,5);return null;
    end if;
  end if;
  update public.rewindr_jobs set state='running',attempts=attempts+1,lease_token=gen_random_uuid(),worker_id=p_worker_id,
    lease_until=clock_timestamp()+make_interval(secs=>p_lease_seconds),message_id=msg.msg_id,updated_at=clock_timestamp()
    where id=j.id returning * into j;
  return to_jsonb(j);
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
  if j.kind in ('chunk_upload','ingest_chunk','candidate_analysis') and p_outcome='completed' then raise exception 'Use atomic chunk completion' using errcode='23514'; end if;
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
  if next_state in ('failed','needs_review') and j.kind='candidate_analysis' then
    if exists(select 1 from public.rewindr_analyses a,jsonb_array_elements(a.checkpoint->'parts') p where a.job_id=j.id and p->>'phase'='submitting') then next_state:='needs_review'; end if;
    update public.rewindr_analyses set status=next_state,error=p_error where job_id=j.id;
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
  if j.kind='candidate_analysis' then
    if exists(select 1 from public.rewindr_analyses a,jsonb_array_elements(a.checkpoint->'parts') p where a.job_id=j.id and p->>'phase'='submitting') then raise exception 'Review analysis submission first' using errcode='23514'; end if;
    update public.rewindr_analyses set status='processing',error=null where job_id=j.id;
  end if;
  select * into m from pgmq.send(j.queue_name,jsonb_build_object('jobId',j.id));
  update public.rewindr_jobs set state='queued',message_id=m,failures=0,error=null,available_at=clock_timestamp(),updated_at=clock_timestamp() where id=j.id returning * into j;
  return to_jsonb(j);
end;
$$;
