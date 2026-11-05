export function analysisConfig(env:NodeJS.ProcessEnv=process.env):{enabled:boolean;model:'pegasus1.5'|'pegasus1.6'}{
  const enabled=env.CANDIDATE_ANALYSIS_ENABLED?.trim()??'true';if(!['true','false'].includes(enabled))throw Error('CANDIDATE_ANALYSIS_ENABLED must be true or false.');
  const model=env.TWELVELABS_ANALYSIS_MODEL?.trim()||'pegasus1.5';if(model!=='pegasus1.5'&&model!=='pegasus1.6')throw Error('Use a supported Pegasus analysis model.');
  return {enabled:enabled==='true',model};
}
