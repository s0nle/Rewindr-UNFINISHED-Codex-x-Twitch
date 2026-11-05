-- Keep the original migrations immutable; wrap writes with lifecycle guards/budgets.
alter function public.rewindr_get_broadcast(uuid,uuid) rename to rewindr_get_broadcast_v10;
create function public.rewindr_get_broadcast(p_owner_id uuid,p_id uuid) returns jsonb language sql stable security invoker set search_path=pg_catalog,public as $$
  select public.rewindr_get_broadcast_v10(p_owner_id,p_id) where exists(select 1 from public.rewindr_broadcasts where owner_id=p_owner_id and id=p_id and deleting_at is null);
$$;
alter function public.rewindr_save_broadcast(uuid,jsonb) rename to rewindr_save_broadcast_v10;
create function public.rewindr_save_broadcast(p_owner_id uuid,p_broadcast jsonb) returns void language plpgsql security invoker set search_path=pg_catalog,public as $$
begin
  perform 1 from public.rewindr_broadcasters where id=p_owner_id for update;
  if exists(select 1 from public.rewindr_broadcasts where id=(p_broadcast->>'id')::uuid and owner_id=p_owner_id and deleting_at is not null) then raise exception 'Archive is deleting' using errcode='23514';end if;
  perform public.rewindr_save_broadcast_v10(p_owner_id,p_broadcast);
end;$$;

alter function public.rewindr_create_search(uuid,uuid,jsonb,text,jsonb,jsonb) rename to rewindr_create_search_v10;
create function public.rewindr_create_search(p_owner_id uuid,p_id uuid,p_snapshot jsonb,p_question text,p_settings jsonb,p_state jsonb) returns jsonb
language plpgsql security definer set search_path=pg_catalog,public as $$
begin
  perform 1 from public.rewindr_broadcasters where id=p_owner_id for update;
  if public.rewindr_get_broadcast(p_owner_id,(p_snapshot->>'id')::uuid) is null then raise exception 'Archive unavailable' using errcode='23514';end if;
  perform public.rewindr_reserve_usage(p_owner_id,'search','initial:'||p_id::text);
  return public.rewindr_create_search_v10(p_owner_id,p_id,p_snapshot,p_question,p_settings,p_state);
end;$$;
alter function public.rewindr_reserve_recovery(uuid,uuid,integer,uuid) rename to rewindr_reserve_recovery_v10;
create function public.rewindr_reserve_recovery(p_owner_id uuid,p_id uuid,p_version integer,p_request_id uuid) returns jsonb
language plpgsql security definer set search_path=pg_catalog,public as $$
declare bid uuid;
begin
  perform 1 from public.rewindr_broadcasters where id=p_owner_id for update;
  select broadcast_id into bid from public.rewindr_searches where owner_id=p_owner_id and id=p_id;
  if bid is null then raise exception 'Search not found' using errcode='23503';end if;
  if public.rewindr_get_broadcast(p_owner_id,bid) is null then raise exception 'Archive unavailable' using errcode='23514';end if;
  perform public.rewindr_reserve_usage(p_owner_id,'search','recovery:'||p_id::text||':'||p_request_id::text,2);
  return public.rewindr_reserve_recovery_v10(p_owner_id,p_id,p_version,p_request_id);
end;$$;
alter function public.rewindr_request_analysis(uuid,uuid,integer,text,text,text,text,jsonb,boolean,text) rename to rewindr_request_analysis_v10;
create function public.rewindr_request_analysis(p_owner_id uuid,p_search_id uuid,p_version integer,p_candidate_id text,p_cache_key text,p_model text,p_revision text,p_plan jsonb,p_available boolean,p_queue_name text default 'rewindr_jobs') returns jsonb
language plpgsql security definer set search_path=pg_catalog,public as $$
declare result jsonb;bid uuid;
begin
  perform 1 from public.rewindr_broadcasters where id=p_owner_id for update;
  select broadcast_id into bid from public.rewindr_searches where owner_id=p_owner_id and id=p_search_id;
  if public.rewindr_get_broadcast(p_owner_id,bid) is null then raise exception 'Archive unavailable' using errcode='23514';end if;
  result:=public.rewindr_request_analysis_v10(p_owner_id,p_search_id,p_version,p_candidate_id,p_cache_key,p_model,p_revision,p_plan,p_available,p_queue_name);
  if result->>'job_id' is not null then perform public.rewindr_reserve_usage(p_owner_id,'analysis',(result->>'job_id')||':'||coalesce(result->>'attempt_started_at',result->>'created_at'));end if;
  return result;
end;$$;
alter function public.rewindr_create_ingestion(uuid,jsonb) rename to rewindr_create_ingestion_v10;
create function public.rewindr_create_ingestion(p_owner_id uuid,p_input jsonb) returns jsonb language plpgsql security definer set search_path=pg_catalog,public as $$
begin
  perform 1 from public.rewindr_broadcasters where id=p_owner_id for update;
  if exists(select 1 from public.rewindr_broadcasts where owner_id=p_owner_id and id=(p_input->>'id')::uuid and deleting_at is not null) then raise exception 'Archive deleting' using errcode='23514';end if;
  perform public.rewindr_reserve_usage(p_owner_id,'ingestion',p_input->>'id');
  return public.rewindr_create_ingestion_v10(p_owner_id,p_input);
end;$$;
alter function public.rewindr_accept_ingest_chunk(uuid,uuid,jsonb,text) rename to rewindr_accept_ingest_chunk_v10;
create function public.rewindr_accept_ingest_chunk(p_owner_id uuid,p_ingestion_id uuid,p_chunk jsonb,p_queue_name text default 'rewindr_jobs') returns jsonb
language plpgsql security definer set search_path=pg_catalog,public as $$
begin
  perform 1 from public.rewindr_broadcasters where id=p_owner_id for update;
  if public.rewindr_get_broadcast(p_owner_id,p_ingestion_id) is null then raise exception 'Archive unavailable' using errcode='23503';end if;
  perform public.rewindr_reserve_usage(p_owner_id,'upload_bytes',p_ingestion_id::text||':'||(p_chunk->>'ordinal'),(p_chunk->>'bytes')::bigint);
  return public.rewindr_accept_ingest_chunk_v10(p_owner_id,p_ingestion_id,p_chunk,p_queue_name);
end;$$;
create function public.rewindr_usage_today(p_owner_id uuid) returns jsonb language sql security definer set search_path=pg_catalog,public as $$
  select coalesce(jsonb_agg(row_to_json(t)),'[]'::jsonb) from(select metric,sum(amount) amount from public.rewindr_usage where owner_id=p_owner_id and created_at>=date_trunc('day',clock_timestamp() at time zone 'UTC') at time zone 'UTC' group by metric)t;
$$;
create function public.rewindr_link_revoked() returns trigger language plpgsql security definer set search_path=pg_catalog,public as $$
begin delete from public.rewindr_agent_credentials where owner_id=old.owner_id;delete from public.rewindr_publications where owner_id=old.owner_id;return old;end;$$;
create trigger rewindr_channel_unlinked after delete on public.rewindr_twitch_links for each row execute function public.rewindr_link_revoked();
create function public.rewindr_export_removed() returns trigger language plpgsql security definer set search_path=pg_catalog,public as $$
begin update public.rewindr_publications set export_id=null,version=version+1,updated_at=clock_timestamp() where owner_id=old.owner_id and export_id=old.id;return old;end;$$;
create trigger rewindr_export_removing before delete on public.rewindr_exports for each row execute function public.rewindr_export_removed();
do $$declare f record;begin
  for f in select p.oid::regprocedure sig from pg_proc p join pg_namespace n on n.oid=p.pronamespace where n.nspname='public' and (p.proname in ('rewindr_get_broadcast','rewindr_save_broadcast','rewindr_create_search','rewindr_reserve_recovery','rewindr_request_analysis','rewindr_create_ingestion','rewindr_accept_ingest_chunk','rewindr_usage_today','rewindr_link_revoked','rewindr_export_removed')) loop
    execute format('revoke all on function %s from public,anon,authenticated',f.sig);execute format('grant execute on function %s to service_role',f.sig);
  end loop;
end;$$;
