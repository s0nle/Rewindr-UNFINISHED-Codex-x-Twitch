import { readFile,readdir } from 'node:fs/promises';
import { resolve } from 'node:path';
import type { PGlite } from '@electric-sql/pglite';

// PGlite cannot load the native pgmq extension. This test-only SQL implementation
// exercises our transactions/leases; queue:verify separately tests real pgmq.
export async function applyTestMigrations(db:PGlite) {
  await db.exec(`
    create schema pgmq;
    create type pgmq.message_record as(msg_id bigint,read_ct integer,enqueued_at timestamptz,vt timestamptz,message jsonb);
    create function pgmq.create(queue_name text) returns void language plpgsql as $$ begin
      execute format('create table pgmq.%I(msg_id bigserial primary key,read_ct integer not null default 0,enqueued_at timestamptz not null default clock_timestamp(),vt timestamptz not null default clock_timestamp(),message jsonb not null)','q_'||queue_name);
      execute format('create table pgmq.%I(like pgmq.%I including defaults)','a_'||queue_name,'q_'||queue_name);
    end $$;
    create function pgmq.send(queue_name text,msg jsonb,delay integer default 0) returns setof bigint language plpgsql as $$ begin
      return query execute format('insert into pgmq.%I(message,vt) values($1,clock_timestamp()+make_interval(secs=>$2)) returning msg_id','q_'||queue_name) using msg,delay;
    end $$;
    create function pgmq.read(queue_name text,vt integer,qty integer) returns setof pgmq.message_record language plpgsql as $$ begin
      return query execute format('with chosen as(select msg_id from pgmq.%I where vt<=clock_timestamp() order by msg_id limit $1 for update skip locked) update pgmq.%I q set vt=clock_timestamp()+make_interval(secs=>$2),read_ct=q.read_ct+1 from chosen where q.msg_id=chosen.msg_id returning q.*','q_'||queue_name,'q_'||queue_name) using qty,vt;
    end $$;
    create function pgmq.archive(queue_name text,msg_id bigint) returns boolean language plpgsql as $$ declare n bigint; begin
      execute format('with gone as(delete from pgmq.%I where msg_id=$1 returning *) insert into pgmq.%I select * from gone','q_'||queue_name,'a_'||queue_name) using msg_id;
      get diagnostics n=row_count; return n>0;
    end $$;
    create function pgmq.set_vt(queue_name text,msg_id bigint,vt_offset integer) returns setof pgmq.message_record language plpgsql as $$ begin
      return query execute format('update pgmq.%I set vt=clock_timestamp()+make_interval(secs=>$2) where msg_id=$1 returning *','q_'||queue_name) using msg_id,vt_offset;
    end $$;
  `);
  for(const file of(await readdir(resolve('supabase/migrations'))).filter(name=>name.endsWith('.sql')).sort()) {
    const sql=await readFile(resolve('supabase/migrations',file),'utf8');
    await db.exec(file==='202610300003_jobs.sql'?sql.replace('create extension if not exists pgmq;','-- Native extension replaced only in this test harness.'):sql);
  }
}
