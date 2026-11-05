-- Additive, application-owned tables. Videos remain on disk in this stage.
create table public.rewindr_broadcasters (
  id uuid primary key,
  label text not null,
  twitch_user_id text unique,
  created_at timestamptz not null default now()
);

create table public.rewindr_recordings (
  id uuid primary key,
  owner_id uuid not null references public.rewindr_broadcasters(id) on delete cascade,
  filename text not null check (length(filename) between 1 and 200),
  bytes bigint not null check (bytes between 0 and 4000000000),
  sha256 text not null check (sha256 ~ '^[a-f0-9]{64}$'),
  creator_authorized boolean not null check (creator_authorized),
  authorized_at timestamptz not null,
  created_at timestamptz not null,
  updated_at timestamptz not null,
  phase text not null check (phase in ('queued','probing','creating_index','creating_upload','uploading','asset_processing','submitting_index','indexing','ready','failed')),
  media jsonb,
  index_id text,
  asset_id text,
  indexed_asset_id text,
  upload jsonb,
  provider_status text,
  error text,
  poll_errors integer check (poll_errors >= 0),
  provider_error jsonb,
  unique (owner_id, id)
);
create index rewindr_recordings_owner_phase on public.rewindr_recordings(owner_id, phase, created_at);

create table public.rewindr_broadcasts (
  id uuid primary key,
  owner_id uuid not null references public.rewindr_broadcasters(id) on delete cascade,
  title text not null check (length(title) between 1 and 200),
  index_id text not null check (length(index_id) between 1 and 200),
  created_at timestamptz not null,
  source_offset_seconds numeric not null default 0 check (source_offset_seconds >= 0 and source_offset_seconds < 'Infinity'::numeric),
  source_recording_id uuid,
  unique (owner_id, id),
  foreign key (owner_id, source_recording_id) references public.rewindr_recordings(owner_id, id)
);
create index rewindr_broadcasts_owner_created on public.rewindr_broadcasts(owner_id, created_at);

create table public.rewindr_chunks (
  id uuid primary key default gen_random_uuid(),
  broadcast_id uuid not null,
  owner_id uuid not null,
  ordinal integer not null check (ordinal >= 0),
  filename text not null check (length(filename) between 5 and 200 and right(lower(filename), 4) = '.mp4'),
  start_seconds numeric not null check (start_seconds >= 0 and start_seconds < 'Infinity'::numeric),
  end_seconds numeric not null check (end_seconds > start_seconds and end_seconds - start_seconds <= 65),
  indexed_asset_id text not null check (length(indexed_asset_id) between 1 and 200),
  asset_id text,
  status text not null default 'ready' check (status in ('queued','uploading','indexing','ready','failed')),
  storage_path text,
  foreign key (owner_id, broadcast_id) references public.rewindr_broadcasts(owner_id, id) on delete cascade,
  unique (broadcast_id, ordinal),
  unique (broadcast_id, filename),
  unique (broadcast_id, indexed_asset_id)
);
create index rewindr_chunks_owner_broadcast_time on public.rewindr_chunks(owner_id, broadcast_id, start_seconds);

-- No browser access yet. Backend secret keys bypass RLS; application queries must
-- still scope every operation to the server-selected owner, never a request owner.
alter table public.rewindr_broadcasters enable row level security;
alter table public.rewindr_recordings enable row level security;
alter table public.rewindr_broadcasts enable row level security;
alter table public.rewindr_chunks enable row level security;
revoke all on public.rewindr_broadcasters, public.rewindr_recordings, public.rewindr_broadcasts, public.rewindr_chunks from public, anon, authenticated;
grant usage on schema public to service_role;
grant select, insert, update, delete on public.rewindr_broadcasters, public.rewindr_recordings, public.rewindr_broadcasts, public.rewindr_chunks to service_role;

-- PostgreSQL functions run within one transaction: replacing a mapping either
-- saves its parent and every valid chunk, or rolls the entire operation back.
create function public.rewindr_save_broadcast(p_owner_id uuid, p_broadcast jsonb)
returns void language plpgsql security invoker set search_path = pg_catalog, public as $$
declare
  v_id uuid := (p_broadcast->>'id')::uuid;
  v_chunk jsonb;
  v_ordinal integer := 0;
begin
  if jsonb_typeof(p_broadcast->'chunks') is distinct from 'array'
     or jsonb_array_length(p_broadcast->'chunks') not between 1 and 2000 then
    raise exception 'Invalid chunk array' using errcode = '23514';
  end if;
  if exists (select 1 from public.rewindr_broadcasts where id = v_id and owner_id <> p_owner_id) then
    raise exception 'Broadcast belongs to another owner' using errcode = '42501';
  end if;
  insert into public.rewindr_broadcasts(id,owner_id,title,index_id,created_at,source_offset_seconds,source_recording_id)
  values (v_id,p_owner_id,p_broadcast->>'title',p_broadcast->>'indexId',
    (p_broadcast->>'createdAt')::timestamptz,(p_broadcast->>'sourceOffsetSeconds')::numeric,
    (p_broadcast->>'sourceRecordingId')::uuid)
  on conflict (id) do update set title=excluded.title,index_id=excluded.index_id,
    source_offset_seconds=excluded.source_offset_seconds,source_recording_id=excluded.source_recording_id
    where rewindr_broadcasts.owner_id = excluded.owner_id;
  if not found then raise exception 'Broadcast owner conflict' using errcode = '42501'; end if;
  delete from public.rewindr_chunks where broadcast_id=v_id and owner_id=p_owner_id;
  for v_chunk in select value from jsonb_array_elements(p_broadcast->'chunks') loop
    insert into public.rewindr_chunks(broadcast_id,owner_id,ordinal,filename,start_seconds,end_seconds,indexed_asset_id,asset_id)
    values (v_id,p_owner_id,v_ordinal,v_chunk->>'filename',(v_chunk->>'startSeconds')::numeric,
      (v_chunk->>'endSeconds')::numeric,v_chunk->>'indexedAssetId',v_chunk->>'assetId');
    v_ordinal := v_ordinal + 1;
  end loop;
  if exists (select 1 from (
    select start_seconds,lag(end_seconds) over (order by start_seconds,ordinal) previous_end
    from public.rewindr_chunks where broadcast_id=v_id and owner_id=p_owner_id
  ) ordered where start_seconds < previous_end - 0.1) then
    raise exception 'Overlapping chunks' using errcode = '23514';
  end if;
end;
$$;
revoke all on function public.rewindr_save_broadcast(uuid,jsonb) from public,anon,authenticated;
grant execute on function public.rewindr_save_broadcast(uuid,jsonb) to service_role;

-- A single database snapshot avoids mixed parent/chunk reads and PostgREST's
-- per-table row limit truncating long broadcasts with more than 1000 chunks.
create function public.rewindr_get_broadcast(p_owner_id uuid,p_id uuid)
returns jsonb language sql stable security invoker set search_path = pg_catalog, public as $$
  select jsonb_strip_nulls(jsonb_build_object('id',b.id,'title',b.title,'indexId',b.index_id,
    'createdAt',b.created_at,'sourceOffsetSeconds',b.source_offset_seconds,
    'sourceRecordingId',b.source_recording_id,'chunks',(
      select coalesce(jsonb_agg(jsonb_strip_nulls(jsonb_build_object('filename',c.filename,
        'indexedAssetId',c.indexed_asset_id,'assetId',c.asset_id,'startSeconds',c.start_seconds,
        'endSeconds',c.end_seconds)) order by c.ordinal),'[]'::jsonb)
      from public.rewindr_chunks c where c.broadcast_id=b.id and c.owner_id=p_owner_id)))
  from public.rewindr_broadcasts b where b.id=p_id and b.owner_id=p_owner_id;
$$;
revoke all on function public.rewindr_get_broadcast(uuid,uuid) from public,anon,authenticated;
grant execute on function public.rewindr_get_broadcast(uuid,uuid) to service_role;
