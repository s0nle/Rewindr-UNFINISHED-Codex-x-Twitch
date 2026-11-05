-- Keep exports compatible with the project's global Storage file-size limit.
alter table public.rewindr_exports add constraint rewindr_export_storage_size
  check(storage is null or (storage->>'bytes')::bigint<=50000000);
