-- Global references are necessary: API and workers can share a spool across owners/queues.
create function public.rewindr_staged_file_status(p_file_ids uuid[]) returns jsonb
language sql stable security definer set search_path=pg_catalog,public as $$
  with files as(select unnest(p_file_ids) id), refs as(
    select f.id,j.state,j.updated_at,'chunk_upload' kind,
      case when c.storage_sha256=j.input->>'sha256' and c.storage_bytes=(j.input->>'bytes')::bigint then
        jsonb_build_object('bucket',c.storage_bucket,'path',c.storage_path) end replica
    from files f join public.rewindr_jobs j on j.kind='chunk_upload' and j.input->>'fileId'=f.id::text
    left join public.rewindr_chunks c on c.owner_id=j.owner_id and c.broadcast_id=j.broadcast_id and c.indexed_asset_id=j.input->>'indexedAssetId'
    union all
    select f.id,j.state,j.updated_at,'ingest_chunk',
      case when c.checkpoint->'storage'->>'sha256'=c.sha256 and (c.checkpoint->'storage'->>'bytes')::bigint=c.bytes then
        jsonb_build_object('bucket',c.checkpoint->'storage'->>'bucket','path',c.checkpoint->'storage'->>'path') end
    from files f join public.rewindr_ingest_chunks c on c.file_id=f.id join public.rewindr_jobs j on j.id=c.job_id
  ), summary as(
    select f.id,count(r.id) n,max(r.updated_at) touched,
      bool_and(r.state='completed' and r.replica->>'path' is not null) completed,
      bool_and(r.kind='ingest_chunk' and r.state in ('completed','failed','needs_review') and r.replica->>'path' is not null) recoverable,
      coalesce(jsonb_agg(r.replica) filter(where r.replica->>'path' is not null),'[]'::jsonb) replicas
    from files f left join refs r on r.id=f.id group by f.id
  ) select coalesce(jsonb_agg(jsonb_build_object('fileId',id,'state',case when n=0 then 'orphan' when completed then 'completed' when recoverable then 'recoverable' else 'keep' end,
    'updatedAt',touched,'replicas',replicas)),'[]'::jsonb) from summary;
$$;
revoke all on function public.rewindr_staged_file_status(uuid[]) from public,anon,authenticated;
grant execute on function public.rewindr_staged_file_status(uuid[]) to service_role;
