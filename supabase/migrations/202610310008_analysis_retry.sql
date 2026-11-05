alter table public.rewindr_analyses add column attempt_started_at timestamptz not null default clock_timestamp();
alter table public.rewindr_analyses add column attempt_history jsonb not null default '[]'::jsonb check(jsonb_typeof(attempt_history)='array');

-- Requeue only after a terminal job; preserve live/unknown task identities.
create function public.rewindr_requeue_analysis(p_owner_id uuid,p_id uuid) returns jsonb
language plpgsql security definer set search_path=pg_catalog,public as $$
declare a public.rewindr_analyses;j public.rewindr_jobs;cp jsonb;v_plan jsonb;k integer;m bigint;
begin
  select * into a from public.rewindr_analyses where owner_id=p_owner_id and id=p_id for update;
  if not found then raise exception 'Analysis not found' using errcode='23503'; end if;
  if a.status not in ('failed','skipped') then return to_jsonb(a); end if;
  select * into j from public.rewindr_jobs where id=a.job_id and owner_id=p_owner_id for update;
  if not found or j.state not in ('failed','completed') then raise exception 'Analysis job changed' using errcode='23514'; end if;
  if exists(select 1 from jsonb_array_elements(a.checkpoint->'parts') p where p->>'phase'='submitting') then
    update public.rewindr_jobs set state='needs_review' where id=j.id;
    update public.rewindr_analyses set status='needs_review',error='Unconfirmed task creation requires reconciliation.' where id=a.id returning * into a;
    return to_jsonb(a);
  end if;
  cp:=a.checkpoint;v_plan:=a.plan;
  for k in 0..jsonb_array_length(v_plan)-1 loop
    if cp->'parts'->k->>'failureKind' in ('provider_terminal','invalid_evidence') then
      cp:=jsonb_set(cp,array['parts',k::text],jsonb_build_object('phase','queued'));
      v_plan:=jsonb_set(v_plan,array[k::text,'id'],to_jsonb(gen_random_uuid()::text));
    end if;
  end loop;
  update public.rewindr_analyses set checkpoint=cp,plan=v_plan,status='queued',result=null,error=null,attempt_started_at=clock_timestamp(),
    attempt_history=attempt_history||jsonb_build_array(jsonb_build_object('startedAt',a.attempt_started_at,'plan',a.plan,'checkpoint',a.checkpoint,'error',a.error))
    where id=a.id returning * into a;
  select * into m from pgmq.send(j.queue_name,jsonb_build_object('jobId',j.id));
  update public.rewindr_jobs set state='queued',failures=0,error=null,message_id=m,lease_token=null,lease_until=null,available_at=clock_timestamp(),updated_at=clock_timestamp() where id=j.id;
  return to_jsonb(a);
end;
$$;
revoke all on function public.rewindr_requeue_analysis(uuid,uuid) from public,anon,authenticated;
grant execute on function public.rewindr_requeue_analysis(uuid,uuid) to service_role;

create or replace function public.rewindr_request_analysis(p_owner_id uuid,p_search_id uuid,p_version integer,p_candidate_id text,p_cache_key text,p_model text,p_revision text,p_plan jsonb,p_available boolean,p_queue_name text default 'rewindr_jobs') returns jsonb
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
  if a.status in ('failed','skipped') and p_available then
    a:=jsonb_populate_record(null::public.rewindr_analyses,public.rewindr_requeue_analysis(p_owner_id,a.id));
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


create or replace function public.rewindr_retry_job(p_owner_id uuid,p_job_id uuid) returns jsonb
language plpgsql security definer set search_path=pg_catalog,public as $$
declare j public.rewindr_jobs; r public.rewindr_recordings; m bigint; cp jsonb; a public.rewindr_analyses;
begin
  select * into j from public.rewindr_jobs where id=p_job_id and owner_id=p_owner_id;
  if found and j.kind='candidate_analysis' then
    if j.state<>'failed' then raise exception 'Only failed analyses can retry' using errcode='23514'; end if;
    a:=jsonb_populate_record(null::public.rewindr_analyses,public.rewindr_requeue_analysis(p_owner_id,(j.input->>'analysisId')::uuid));
    if a.status='needs_review' then raise exception 'Review analysis submission first' using errcode='23514'; end if;
    select * into j from public.rewindr_jobs where id=p_job_id;
    return to_jsonb(j);
  end if;
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
