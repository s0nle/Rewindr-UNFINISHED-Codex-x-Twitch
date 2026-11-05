create table public.rewindr_twitch_links(
  owner_id uuid primary key references public.rewindr_broadcasters(id) on delete cascade,
  twitch_user_id text not null unique check(twitch_user_id ~ '^[0-9]{1,30}$'),
  login text not null check(length(login) between 1 and 100),display_name text not null check(length(display_name) between 1 and 200),
  profile_image_url text not null,verified_at timestamptz not null default clock_timestamp()
);
create table public.rewindr_twitch_oauth_states(
  state_hash text primary key check(state_hash ~ '^[a-f0-9]{64}$'),browser_hash text not null check(browser_hash ~ '^[a-f0-9]{64}$'),
  owner_id uuid not null references public.rewindr_broadcasters(id) on delete cascade,
  expires_at timestamptz not null default clock_timestamp()+interval '10 minutes'
);
alter table public.rewindr_twitch_links enable row level security;
alter table public.rewindr_twitch_oauth_states enable row level security;
revoke all on public.rewindr_twitch_links,public.rewindr_twitch_oauth_states from public,anon,authenticated;
grant select on public.rewindr_twitch_links to service_role;
create function public.rewindr_twitch_begin(p_owner_id uuid,p_state_hash text,p_browser_hash text) returns void
language plpgsql security definer set search_path=pg_catalog,public as $$
begin
  delete from public.rewindr_twitch_oauth_states where expires_at<clock_timestamp();
  insert into public.rewindr_twitch_oauth_states(owner_id,state_hash,browser_hash) values(p_owner_id,p_state_hash,p_browser_hash);
end;$$;
create function public.rewindr_twitch_consume(p_state_hash text,p_browser_hash text) returns uuid
language plpgsql security definer set search_path=pg_catalog,public as $$
declare owner uuid;
begin
  delete from public.rewindr_twitch_oauth_states where state_hash=p_state_hash and browser_hash=p_browser_hash and expires_at>clock_timestamp() returning owner_id into owner;
  if owner is null then raise exception 'OAuth state expired or invalid' using errcode='23514';end if;return owner;
end;$$;
create function public.rewindr_twitch_link(p_owner_id uuid,p_user jsonb) returns jsonb
language plpgsql security definer set search_path=pg_catalog,public as $$
declare current public.rewindr_twitch_links;
begin
  perform pg_advisory_xact_lock(hashtext('rewindr-twitch-link'));
  select * into current from public.rewindr_twitch_links where owner_id=p_owner_id;
  if found and current.twitch_user_id is distinct from p_user->>'id' then raise exception 'Disconnect existing channel before relinking' using errcode='23514';end if;
  insert into public.rewindr_twitch_links(owner_id,twitch_user_id,login,display_name,profile_image_url)
    values(p_owner_id,p_user->>'id',p_user->>'login',p_user->>'display_name',p_user->>'profile_image_url')
    on conflict(owner_id) do update set login=excluded.login,display_name=excluded.display_name,profile_image_url=excluded.profile_image_url,verified_at=clock_timestamp() returning * into current;
  return to_jsonb(current);
end;$$;
create function public.rewindr_twitch_unlink(p_owner_id uuid) returns void
language plpgsql security definer set search_path=pg_catalog,public as $$
begin delete from public.rewindr_twitch_links where owner_id=p_owner_id;delete from public.rewindr_twitch_oauth_states where owner_id=p_owner_id;end;$$;
revoke all on function public.rewindr_twitch_begin(uuid,text,text),public.rewindr_twitch_consume(text,text),public.rewindr_twitch_link(uuid,jsonb),public.rewindr_twitch_unlink(uuid) from public,anon,authenticated;
grant execute on function public.rewindr_twitch_begin(uuid,text,text),public.rewindr_twitch_consume(text,text),public.rewindr_twitch_link(uuid,jsonb),public.rewindr_twitch_unlink(uuid) to service_role;
