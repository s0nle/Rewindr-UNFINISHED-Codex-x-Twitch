// A rejected write remains visible to its caller without poisoning future saves.
export function createSessionWriter(write:()=>Promise<void>) {
  let pending=Promise.resolve();
  return {
    save:()=>pending=pending.catch(()=>{}).then(write),
    flush:()=>pending
  };
}
