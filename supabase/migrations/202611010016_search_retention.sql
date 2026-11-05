alter function public.rewindr_prune_operational(uuid) rename to rewindr_prune_operational_v14;
create function public.rewindr_prune_operational(p_owner_id uuid) returns void
language plpgsql security definer set search_path=pg_catalog,public as $$
begin
  perform 1 from public.rewindr_broadcasters where id=p_owner_id for update;
  perform public.rewindr_prune_operational_v14(p_owner_id);
  delete from public.rewindr_searches s where owner_id=p_owner_id and expires_at<clock_timestamp()-interval '1 day'
    and not exists(select 1 from public.rewindr_search_analyses sa join public.rewindr_analyses a on a.id=sa.analysis_id left join public.rewindr_jobs j on j.id=a.job_id
      where sa.search_id=s.id and (a.status in ('queued','processing','needs_review') or j.state in ('queued','running','retrying','needs_review')));
  delete from public.rewindr_analyses a where owner_id=p_owner_id and created_at<clock_timestamp()-interval '7 days' and status in ('ready','unavailable','skipped')
    and not exists(select 1 from public.rewindr_search_analyses sa where sa.analysis_id=a.id)
    and not exists(select 1 from public.rewindr_jobs j where j.id=a.job_id and j.state in ('queued','running','retrying','needs_review'));
end;$$;
revoke all on function public.rewindr_prune_operational(uuid) from public,anon,authenticated;
grant execute on function public.rewindr_prune_operational(uuid) to service_role;
