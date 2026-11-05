create function public.rewindr_search_budget(p_owner_id uuid,p_id uuid,p_broadcast_id uuid) returns void language plpgsql security definer set search_path=pg_catalog,public as $$
begin
  perform 1 from public.rewindr_broadcasters where id=p_owner_id for update;
  if public.rewindr_get_broadcast(p_owner_id,p_broadcast_id) is null then raise exception 'Archive unavailable' using errcode='23514';end if;
  perform public.rewindr_reserve_usage(p_owner_id,'search','initial:'||p_id::text);
end;$$;
create function public.rewindr_search_archive_available(p_owner_id uuid,p_id uuid) returns boolean language sql security definer set search_path=pg_catalog,public as $$
  select exists(select 1 from public.rewindr_searches s join public.rewindr_broadcasts b on b.owner_id=s.owner_id and b.id=s.broadcast_id where s.owner_id=p_owner_id and s.id=p_id and b.deleting_at is null);
$$;
alter function public.rewindr_create_live_ingestion(uuid,jsonb) rename to rewindr_create_live_ingestion_v10;
create function public.rewindr_create_live_ingestion(p_owner_id uuid,p_input jsonb) returns jsonb language plpgsql security definer set search_path=pg_catalog,public as $$
begin
  perform 1 from public.rewindr_broadcasters where id=p_owner_id for update;
  if exists(select 1 from public.rewindr_broadcasts where owner_id=p_owner_id and id=(p_input->>'id')::uuid and deleting_at is not null) then raise exception 'Archive deleting' using errcode='23514';end if;
  perform public.rewindr_reserve_usage(p_owner_id,'ingestion',p_input->>'id');
  return public.rewindr_create_live_ingestion_v10(p_owner_id,p_input);
end;$$;
revoke all on function public.rewindr_search_budget(uuid,uuid,uuid),public.rewindr_search_archive_available(uuid,uuid),public.rewindr_create_live_ingestion(uuid,jsonb) from public,anon,authenticated;
grant execute on function public.rewindr_search_budget(uuid,uuid,uuid),public.rewindr_search_archive_available(uuid,uuid),public.rewindr_create_live_ingestion(uuid,jsonb) to service_role;
