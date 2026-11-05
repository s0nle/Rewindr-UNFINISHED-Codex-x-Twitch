-- Durable clip exports, explicit viewer selections, usage budgets and archive tombstones.
alter table public.rewindr_broadcasts add column deleting_at timestamptz;
create table public.rewindr_usage(
  owner_id uuid not null references public.rewindr_broadcasters(id) on delete cascade,
  metric text not null,event_id text not null check(length(event_id) between 1 and 200),
  amount bigint not null check(amount>0),created_at timestamptz not null default clock_timestamp(),
  primary key(owner_id,metric,event_id)
);
create index rewindr_usage_day on public.rewindr_usage(owner_id,metric,created_at);
create table public.rewindr_exports(
  id uuid primary key,owner_id uuid not null,broadcast_id uuid not null,
  title text not null check(length(title) between 1 and 100),snapshot jsonb not null,
  start_ms bigint not null check(start_ms>=0),end_ms bigint not null,
  storage jsonb,ready_at timestamptz,expires_at timestamptz not null default clock_timestamp()+interval '7 days',
  created_at timestamptz not null default clock_timestamp(),
  foreign key(owner_id,broadcast_id) references public.rewindr_broadcasts(owner_id,id) on delete cascade,
  check(end_ms-start_ms between 1000 and 180000),unique(owner_id,id)
);
create table public.rewindr_media_tasks(
  id uuid primary key,owner_id uuid not null references public.rewindr_broadcasters(id) on delete cascade,
  kind text not null check(kind in ('export','purge')),input jsonb not null,
  state text not null default 'queued' check(state in ('queued','running','retrying','completed','failed')),
  failures integer not null default 0,attempts integer not null default 0,completed_objects integer not null default 0 check(completed_objects>=0),
  lease_token uuid,lease_until timestamptz,available_at timestamptz not null default clock_timestamp(),
  error text,created_at timestamptz not null default clock_timestamp(),updated_at timestamptz not null default clock_timestamp()
);
create index rewindr_media_available on public.rewindr_media_tasks(available_at) where state in ('queued','running','retrying');
create table public.rewindr_publications(
  channel_id text primary key check(channel_id ~ '^[0-9]{1,30}$'),
  owner_id uuid not null references public.rewindr_broadcasters(id) on delete cascade,
  export_id uuid,version bigint not null default 0,updated_at timestamptz not null default clock_timestamp(),
  foreign key(owner_id,export_id) references public.rewindr_exports(owner_id,id) on delete cascade
);
create table public.rewindr_agent_credentials(
  owner_id uuid primary key references public.rewindr_broadcasters(id) on delete cascade,
  token_hash text not null unique check(token_hash ~ '^[a-f0-9]{64}$'),
  expires_at timestamptz not null default clock_timestamp()+interval '30 days',created_at timestamptz not null default clock_timestamp()
);
do $$declare t text;begin
  foreach t in array array['rewindr_usage','rewindr_exports','rewindr_media_tasks','rewindr_publications','rewindr_agent_credentials'] loop
    execute format('alter table public.%I enable row level security',t);
    execute format('revoke all on public.%I from public,anon,authenticated',t);
  end loop;
end;$$;
grant select on public.rewindr_usage,public.rewindr_exports,public.rewindr_media_tasks,public.rewindr_publications to service_role;

create function public.rewindr_reserve_usage(p_owner_id uuid,p_metric text,p_event_id text,p_amount bigint default 1) returns void
language plpgsql security definer set search_path=pg_catalog,public as $$
declare cap bigint;used bigint;old bigint;
begin
  cap:=case p_metric when 'search' then 100 when 'analysis' then 60 when 'export' then 20 when 'ingestion' then 4 when 'upload_bytes' then 4000000000 end;
  if cap is null or p_amount<1 or p_amount>cap then raise exception 'Invalid usage reservation' using errcode='23514';end if;
  perform pg_advisory_xact_lock(hashtext(p_owner_id::text||':'||p_metric));
  select amount into old from public.rewindr_usage where owner_id=p_owner_id and metric=p_metric and event_id=p_event_id;
  if found then if old<>p_amount then raise exception 'Usage identity conflict' using errcode='23514';end if;return;end if;
  select coalesce(sum(amount),0) into used from public.rewindr_usage where owner_id=p_owner_id and metric=p_metric and created_at>=date_trunc('day',clock_timestamp() at time zone 'UTC') at time zone 'UTC';
  if used+p_amount>cap then raise exception 'Daily channel usage limit reached' using errcode='P0004';end if;
  insert into public.rewindr_usage(owner_id,metric,event_id,amount) values(p_owner_id,p_metric,p_event_id,p_amount);
end;$$;

create function public.rewindr_begin_export(p_owner_id uuid,p_id uuid,p_broadcast_id uuid,p_title text,p_start_ms bigint,p_end_ms bigint) returns jsonb
language plpgsql security definer set search_path=pg_catalog,public as $$
declare e public.rewindr_exports;s jsonb;
begin
  perform 1 from public.rewindr_broadcasters where id=p_owner_id for update;
  select * into e from public.rewindr_exports where id=p_id;
  if found then
    if e.owner_id<>p_owner_id or e.broadcast_id<>p_broadcast_id or e.title<>p_title or e.start_ms<>p_start_ms or e.end_ms<>p_end_ms then raise exception 'Export identity conflict' using errcode='23514';end if;return to_jsonb(e);
  end if;
  perform 1 from public.rewindr_broadcasts where owner_id=p_owner_id and id=p_broadcast_id and deleting_at is null for update;
  if not found then raise exception 'Archive unavailable' using errcode='23514';end if;
  if (select count(*) from public.rewindr_media_tasks where owner_id=p_owner_id and kind='export' and state in ('queued','running','retrying'))>=2 then raise exception 'Two exports already pending' using errcode='P0004';end if;
  s:=public.rewindr_get_broadcast(p_owner_id,p_broadcast_id);
  if p_start_ms<0 or p_end_ms-p_start_ms not between 1000 and 180000 then raise exception 'Clip must last 1 to 180 seconds' using errcode='23514';end if;
  -- The service additionally validates contiguous stored coverage before this call.
  if not exists(select 1 from jsonb_array_elements(s->'chunks') c where c ? 'storage' and (c->>'startSeconds')::numeric<=p_start_ms/1000.0 and (c->>'endSeconds')::numeric>p_start_ms/1000.0) then raise exception 'Clip source not stored' using errcode='23514';end if;
  perform public.rewindr_reserve_usage(p_owner_id,'export',p_id::text);
  insert into public.rewindr_exports(id,owner_id,broadcast_id,title,snapshot,start_ms,end_ms) values(p_id,p_owner_id,p_broadcast_id,p_title,s,p_start_ms,p_end_ms) returning * into e;
  insert into public.rewindr_media_tasks(id,owner_id,kind,input) values(p_id,p_owner_id,'export',jsonb_build_object('broadcastId',p_broadcast_id));
  return to_jsonb(e);
end;$$;

create function public.rewindr_claim_media(p_owner_id uuid default null) returns jsonb
language plpgsql security definer set search_path=pg_catalog,public as $$
declare j public.rewindr_media_tasks;
begin
  select * into j from public.rewindr_media_tasks where (p_owner_id is null or owner_id=p_owner_id) and ((state in ('queued','retrying') and available_at<=clock_timestamp()) or (state='running' and lease_until<clock_timestamp())) order by available_at,id for update skip locked limit 1;
  if not found then return null;end if;
  update public.rewindr_media_tasks set state='running',lease_token=gen_random_uuid(),lease_until=clock_timestamp()+interval '5 minutes',attempts=attempts+1,updated_at=clock_timestamp() where id=j.id returning * into j;
  return to_jsonb(j);
end;$$;
create function public.rewindr_media_guard(p_id uuid,p_token uuid) returns void
language plpgsql security definer set search_path=pg_catalog,public as $$
begin
  update public.rewindr_media_tasks set lease_until=clock_timestamp()+interval '5 minutes',updated_at=clock_timestamp() where id=p_id and state='running' and lease_token=p_token and lease_until>clock_timestamp();
  if not found then raise exception 'Media lease lost' using errcode='P0002';end if;
end;$$;
create function public.rewindr_export_checkpoint(p_id uuid,p_token uuid,p_storage jsonb) returns void
language plpgsql security definer set search_path=pg_catalog,public as $$
declare owner uuid;existing jsonb;
begin
  perform public.rewindr_media_guard(p_id,p_token);
  select owner_id into owner from public.rewindr_media_tasks where id=p_id and kind='export' for update;
  if owner is null or p_storage->>'path' is distinct from owner::text||'/exports/'||p_id::text||'.mp4'
    or not coalesce((p_storage->>'bytes')::bigint between 1 and 150000000,false) or not coalesce(p_storage->>'sha256' ~ '^[a-f0-9]{64}$',false)
    or not coalesce(p_storage->>'bucket' ~ '^[a-z0-9][a-z0-9_-]{1,62}$',false) then raise exception 'Invalid export storage' using errcode='23514';end if;
  select storage into existing from public.rewindr_exports where id=p_id and owner_id=owner for update;
  if existing is not null and existing<>p_storage then raise exception 'Export content is immutable' using errcode='23514';end if;
  update public.rewindr_exports set storage=p_storage where id=p_id and owner_id=owner;
  if not found then raise exception 'Export missing' using errcode='23514';end if;
end;$$;
create function public.rewindr_finish_media(p_id uuid,p_token uuid,p_error text default null,p_terminal boolean default false) returns void
language plpgsql security definer set search_path=pg_catalog,public as $$
declare j public.rewindr_media_tasks;
begin
  perform public.rewindr_media_guard(p_id,p_token);
  select * into j from public.rewindr_media_tasks where id=p_id for update;
  if p_error is null then
    if j.kind='purge' and j.completed_objects<>jsonb_array_length(j.input->'objects') then raise exception 'Cleanup is not complete' using errcode='23514';end if;
    if j.kind='export' then
      update public.rewindr_exports set ready_at=clock_timestamp() where id=j.id and owner_id=j.owner_id and storage is not null;
      if not found then raise exception 'Export storage not checkpointed' using errcode='23514';end if;
    elsif j.input->>'exportId' is not null then
      delete from public.rewindr_exports where owner_id=j.owner_id and id=(j.input->>'exportId')::uuid and expires_at<=clock_timestamp();
    else
      delete from public.rewindr_broadcasts where owner_id=j.owner_id and id=(j.input->>'broadcastId')::uuid and deleting_at is not null;
    end if;
    update public.rewindr_media_tasks set state='completed',lease_token=null,lease_until=null,error=null,updated_at=clock_timestamp() where id=j.id;
  else
    update public.rewindr_media_tasks set state=case when p_terminal or failures>=4 then 'failed' else 'retrying' end,failures=failures+1,error=left(p_error,300),lease_token=null,lease_until=null,available_at=clock_timestamp()+make_interval(secs=>least(300,power(2,least(failures+1,8))::int)),updated_at=clock_timestamp() where id=j.id;
  end if;
end;$$;

create function public.rewindr_progress_media(p_id uuid,p_token uuid,p_completed integer) returns void
language plpgsql security definer set search_path=pg_catalog,public as $$
declare j public.rewindr_media_tasks;
begin
  perform public.rewindr_media_guard(p_id,p_token);select * into j from public.rewindr_media_tasks where id=p_id for update;
  if j.kind<>'purge' or p_completed<j.completed_objects or p_completed>jsonb_array_length(j.input->'objects') then raise exception 'Invalid cleanup progress' using errcode='23514';end if;
  update public.rewindr_media_tasks set completed_objects=p_completed where id=p_id;
end;$$;
revoke all on function public.rewindr_progress_media(uuid,uuid,integer) from public,anon,authenticated;
grant execute on function public.rewindr_progress_media(uuid,uuid,integer) to service_role;
create function public.rewindr_retry_media(p_owner_id uuid,p_id uuid) returns void
language plpgsql security definer set search_path=pg_catalog,public as $$
declare j public.rewindr_media_tasks;
begin
  perform 1 from public.rewindr_broadcasters where id=p_owner_id for update;
  select * into j from public.rewindr_media_tasks where id=p_id and owner_id=p_owner_id for update;
  if j.kind='export' and (public.rewindr_get_broadcast(p_owner_id,(j.input->>'broadcastId')::uuid) is null or not exists(select 1 from public.rewindr_exports where id=p_id and owner_id=p_owner_id and expires_at>clock_timestamp())) then raise exception 'Export source unavailable or expired' using errcode='23514';end if;
  update public.rewindr_media_tasks set state='queued',failures=0,error=null,available_at=clock_timestamp() where owner_id=p_owner_id and id=p_id and state='failed';
  if not found then raise exception 'Only failed media tasks can retry' using errcode='23514';end if;
end;$$;

create function public.rewindr_set_publication(p_owner_id uuid,p_channel text,p_export_id uuid,p_version bigint) returns jsonb
language plpgsql security definer set search_path=pg_catalog,public as $$
declare p public.rewindr_publications;
begin
  perform 1 from public.rewindr_broadcasters where id=p_owner_id for update;
  perform 1 from public.rewindr_twitch_links where owner_id=p_owner_id and twitch_user_id=p_channel for update;
  if not found then raise exception 'Channel is not linked' using errcode='23514';end if;
  insert into public.rewindr_publications(channel_id,owner_id) values(p_channel,p_owner_id) on conflict do nothing;
  select * into p from public.rewindr_publications where channel_id=p_channel for update;
  if p.owner_id<>p_owner_id or p.version<>p_version then raise exception 'Published selection changed; refresh' using errcode='23514';end if;
  if p_export_id is not null then
    perform 1 from public.rewindr_exports e join public.rewindr_broadcasts b on b.owner_id=e.owner_id and b.id=e.broadcast_id
      where e.owner_id=p_owner_id and e.id=p_export_id and e.ready_at is not null and e.expires_at>clock_timestamp() and b.deleting_at is null for update of b;
    if not found then raise exception 'Export not ready or expired' using errcode='23514';end if;
  end if;
  update public.rewindr_publications set export_id=p_export_id,version=version+1,updated_at=clock_timestamp() where channel_id=p_channel returning * into p;
  return to_jsonb(p);
end;$$;

create function public.rewindr_begin_purge(p_owner_id uuid,p_id uuid,p_broadcast_id uuid,p_confirm text) returns jsonb
language plpgsql security definer set search_path=pg_catalog,public as $$
declare b public.rewindr_broadcasts;j public.rewindr_media_tasks;items jsonb;
begin
  perform 1 from public.rewindr_broadcasters where id=p_owner_id for update;
  select * into j from public.rewindr_media_tasks where id=p_id;
  if found then if j.owner_id<>p_owner_id or j.kind<>'purge' or j.input->>'broadcastId'<>p_broadcast_id::text then raise exception 'Purge identity conflict' using errcode='23514';end if;return to_jsonb(j);end if;
  select * into b from public.rewindr_broadcasts where owner_id=p_owner_id and id=p_broadcast_id for update;
  if not found or b.title is distinct from p_confirm then raise exception 'Confirm the exact archive title' using errcode='23514';end if;
  if b.deleting_at is not null then raise exception 'Archive already deleting; inspect its task' using errcode='23514';end if;
  if exists(select 1 from public.rewindr_jobs where owner_id=p_owner_id and broadcast_id=b.id and state in ('queued','running','retrying'))
    or exists(select 1 from public.rewindr_media_tasks where owner_id=p_owner_id and input->>'broadcastId'=b.id::text and state in ('queued','running','retrying')) then raise exception 'Finish or reconcile active archive work before deletion' using errcode='23514';end if;
  select coalesce(jsonb_agg(distinct v),'[]'::jsonb) into items from (
    select jsonb_build_object('bucket',storage_bucket,'path',storage_path) v from public.rewindr_chunks where owner_id=p_owner_id and broadcast_id=b.id and storage_path is not null
    union all select jsonb_build_object('bucket',checkpoint->'storage'->>'bucket','path',checkpoint->'storage'->>'path') from public.rewindr_ingest_chunks where owner_id=p_owner_id and ingestion_id=b.id and checkpoint ? 'storage'
    union all select jsonb_build_object('bucket',storage->>'bucket','path',storage->>'path') from public.rewindr_exports where owner_id=p_owner_id and broadcast_id=b.id and storage is not null
  ) resources;
  update public.rewindr_broadcasts set deleting_at=clock_timestamp() where id=b.id;
  update public.rewindr_ingestions set sealed_at=coalesce(sealed_at,clock_timestamp()) where id=b.id and owner_id=p_owner_id;
  update public.rewindr_publications set export_id=null,version=version+1,updated_at=clock_timestamp() where owner_id=p_owner_id and export_id in(select id from public.rewindr_exports where owner_id=p_owner_id and broadcast_id=b.id);
  insert into public.rewindr_media_tasks(id,owner_id,kind,input) values(p_id,p_owner_id,'purge',jsonb_build_object('broadcastId',b.id,'title',b.title,'objects',items,'providerSnapshot',public.rewindr_get_broadcast_v10(p_owner_id,b.id),'unfinishedProviderAssets',(select coalesce(jsonb_agg(jsonb_build_object('assetId',checkpoint->>'assetId','indexedAssetId',checkpoint->>'indexedAssetId','phase',checkpoint->>'phase','indexId',b.index_id)),'[]'::jsonb) from public.rewindr_ingest_chunks where ingestion_id=b.id and owner_id=p_owner_id),'providerAssetsRetained',true)) returning * into j;
  return to_jsonb(j);
end;$$;

-- Hosted onboarding requires a cryptographically verified broadcaster JWT at the EBS.
create function public.rewindr_onboard_channel(p_channel text,p_user jsonb,p_token_hash text,p_max_channels integer default 1) returns uuid
language plpgsql security definer set search_path=pg_catalog,public as $$
declare owner uuid;
begin
  if p_channel is distinct from p_user->>'id' or p_channel !~ '^[0-9]{1,30}$' then raise exception 'Channel identity mismatch' using errcode='23514';end if;
  perform pg_advisory_xact_lock(hashtext('rewindr-twitch-link'));
  select owner_id into owner from public.rewindr_twitch_links where twitch_user_id=p_channel;
  if not found then
    if p_max_channels not between 1 and 100 or (select count(*) from public.rewindr_twitch_links)>=p_max_channels then raise exception 'Hosted channel capacity reached' using errcode='P0004';end if;
    owner:=gen_random_uuid();insert into public.rewindr_broadcasters(id,label) values(owner,'Twitch channel '||p_channel);
    perform public.rewindr_twitch_link(owner,p_user);
  end if;
  insert into public.rewindr_agent_credentials(owner_id,token_hash) values(owner,p_token_hash)
    on conflict(owner_id) do update set token_hash=excluded.token_hash,created_at=clock_timestamp(),expires_at=clock_timestamp()+interval '30 days';
  return owner;
end;$$;
create function public.rewindr_agent_owner(p_hash text) returns uuid
language sql security definer set search_path=pg_catalog,public as $$
  select a.owner_id from public.rewindr_agent_credentials a join public.rewindr_twitch_links t on t.owner_id=a.owner_id where token_hash=p_hash and expires_at>clock_timestamp();
$$;
create function public.rewindr_revoke_agent(p_owner_id uuid) returns void
language sql security definer set search_path=pg_catalog,public as $$ delete from public.rewindr_agent_credentials where owner_id=p_owner_id;$$;

do $$declare f record;begin
  for f in select p.oid::regprocedure sig from pg_proc p join pg_namespace n on n.oid=p.pronamespace where n.nspname='public' and p.proname in
    ('rewindr_reserve_usage','rewindr_begin_export','rewindr_claim_media','rewindr_media_guard','rewindr_export_checkpoint','rewindr_finish_media','rewindr_retry_media','rewindr_set_publication','rewindr_begin_purge','rewindr_onboard_channel','rewindr_agent_owner','rewindr_revoke_agent') loop
    execute format('revoke all on function %s from public,anon,authenticated',f.sig);execute format('grant execute on function %s to service_role',f.sig);
  end loop;
end;$$;
