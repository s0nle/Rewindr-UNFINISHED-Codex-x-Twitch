create function public.rewindr_begin_export_expiry(p_owner_id uuid,p_id uuid,p_export_id uuid) returns jsonb
language plpgsql security definer set search_path=pg_catalog,public as $$
declare e public.rewindr_exports;j public.rewindr_media_tasks;objects jsonb;
begin
  perform 1 from public.rewindr_broadcasters where id=p_owner_id for update;
  select * into j from public.rewindr_media_tasks where owner_id=p_owner_id and kind='purge' and input->>'exportId'=p_export_id::text order by created_at desc limit 1;
  if found then return to_jsonb(j);end if;
  select * into e from public.rewindr_exports where owner_id=p_owner_id and id=p_export_id and expires_at<=clock_timestamp() for update;
  if not found then raise exception 'Export not expired or not found' using errcode='23514';end if;
  if exists(select 1 from public.rewindr_media_tasks where id=e.id and state in ('queued','running','retrying')) then raise exception 'Finish active export work first' using errcode='23514';end if;
  objects:=case when e.storage is null then '[]'::jsonb else jsonb_build_array(jsonb_build_object('bucket',e.storage->>'bucket','path',e.storage->>'path')) end;
  insert into public.rewindr_media_tasks(id,owner_id,kind,input) values(p_id,p_owner_id,'purge',jsonb_build_object('exportId',e.id,'objects',objects)) returning * into j;
  return to_jsonb(j);
end;$$;
revoke all on function public.rewindr_begin_export_expiry(uuid,uuid,uuid) from public,anon,authenticated;
grant execute on function public.rewindr_begin_export_expiry(uuid,uuid,uuid) to service_role;
create function public.rewindr_prune_operational(p_owner_id uuid) returns void language plpgsql security definer set search_path=pg_catalog,public as $$
begin
  delete from public.rewindr_usage where owner_id=p_owner_id and created_at<clock_timestamp()-interval '31 days';
  delete from public.rewindr_twitch_oauth_states where owner_id=p_owner_id and expires_at<clock_timestamp();
  delete from public.rewindr_agent_credentials where owner_id=p_owner_id and expires_at<clock_timestamp();
  delete from public.rewindr_media_tasks where owner_id=p_owner_id and state='completed' and updated_at<clock_timestamp()-interval '90 days';
end;$$;
revoke all on function public.rewindr_prune_operational(uuid) from public,anon,authenticated;
grant execute on function public.rewindr_prune_operational(uuid) to service_role;
alter function public.rewindr_retry_job(uuid,uuid) rename to rewindr_retry_job_v10;
create function public.rewindr_retry_job(p_owner_id uuid,p_job_id uuid) returns jsonb language plpgsql security definer set search_path=pg_catalog,public as $$
declare bid uuid;
begin
  perform 1 from public.rewindr_broadcasters where id=p_owner_id for update;
  select broadcast_id into bid from public.rewindr_jobs where owner_id=p_owner_id and id=p_job_id;
  if bid is not null and public.rewindr_get_broadcast(p_owner_id,bid) is null then raise exception 'Archive unavailable' using errcode='23514';end if;
  return public.rewindr_retry_job_v10(p_owner_id,p_job_id);
end;$$;
revoke all on function public.rewindr_retry_job(uuid,uuid) from public,anon,authenticated;
grant execute on function public.rewindr_retry_job(uuid,uuid) to service_role;
