alter table public.rewindr_chunks
  add column storage_bucket text,
  add column storage_sha256 text,
  add column storage_bytes bigint,
  add column storage_uploaded_at timestamptz;
alter table public.rewindr_chunks add constraint rewindr_chunk_storage_complete check (
  (storage_path is null and storage_bucket is null and storage_sha256 is null and storage_bytes is null and storage_uploaded_at is null)
  or (storage_path is not null and storage_bucket is not null and storage_sha256 is not null and storage_bytes is not null and storage_uploaded_at is not null
    and storage_bucket ~ '^[a-z0-9][a-z0-9_-]{1,62}$' and storage_sha256 ~ '^[a-f0-9]{64}$'
    and storage_bytes between 1 and 50000000
    and storage_path = owner_id::text || '/' || broadcast_id::text || '/' || encode(sha256(convert_to(indexed_asset_id,'UTF8')),'hex') || '/' || storage_sha256 || '.mp4')
);

create function public.rewindr_attach_chunk_storage(p_owner_id uuid,p_broadcast_id uuid,p_indexed_asset_id text,p_storage jsonb)
returns void language plpgsql security invoker set search_path=pg_catalog,public as $$
begin
  perform 1 from public.rewindr_broadcasts where id=p_broadcast_id and owner_id=p_owner_id for update;
  if not found then raise exception 'Broadcast not found for owner' using errcode='23503'; end if;
  if exists(select 1 from public.rewindr_chunks where broadcast_id=p_broadcast_id and owner_id=p_owner_id
    and indexed_asset_id=p_indexed_asset_id and storage_path is not null and
    (storage_sha256 is distinct from p_storage->>'sha256' or storage_bucket is distinct from p_storage->>'bucket' or storage_path is distinct from p_storage->>'path'
      or storage_bytes is distinct from (p_storage->>'bytes')::bigint)) then
    raise exception 'Existing media is immutable' using errcode='23514';
  end if;
  update public.rewindr_chunks set storage_path=p_storage->>'path',storage_bucket=p_storage->>'bucket',
    storage_sha256=p_storage->>'sha256',storage_bytes=(p_storage->>'bytes')::bigint,
    storage_uploaded_at=coalesce(storage_uploaded_at,(p_storage->>'uploadedAt')::timestamptz)
    where broadcast_id=p_broadcast_id and owner_id=p_owner_id and indexed_asset_id=p_indexed_asset_id;
  if not found then raise exception 'Chunk not found' using errcode='23503'; end if;
end;
$$;
revoke all on function public.rewindr_attach_chunk_storage(uuid,uuid,text,jsonb) from public,anon,authenticated;
grant execute on function public.rewindr_attach_chunk_storage(uuid,uuid,text,jsonb) to service_role;

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

create or replace function public.rewindr_get_broadcast(p_owner_id uuid,p_id uuid)
returns jsonb language sql stable security invoker set search_path=pg_catalog,public as $$
  select jsonb_strip_nulls(jsonb_build_object('id',b.id,'title',b.title,'indexId',b.index_id,'createdAt',b.created_at,
    'sourceOffsetSeconds',b.source_offset_seconds,'sourceRecordingId',b.source_recording_id,'chunks',(
      select coalesce(jsonb_agg(jsonb_strip_nulls(jsonb_build_object('filename',c.filename,'indexedAssetId',c.indexed_asset_id,
        'assetId',c.asset_id,'startSeconds',c.start_seconds,'endSeconds',c.end_seconds,
        'storage',case when c.storage_path is not null then jsonb_build_object('bucket',c.storage_bucket,'path',c.storage_path,
          'sha256',c.storage_sha256,'bytes',c.storage_bytes,'uploadedAt',c.storage_uploaded_at) else null end)) order by c.ordinal),'[]'::jsonb)
        from public.rewindr_chunks c where c.broadcast_id=b.id and c.owner_id=p_owner_id)))
    from public.rewindr_broadcasts b where b.id=p_id and b.owner_id=p_owner_id;
$$;
