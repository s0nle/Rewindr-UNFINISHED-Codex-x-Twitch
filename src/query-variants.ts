// Conservative query engineering: source facts stay intact; there is no runtime
// text-model key and no invented clothing, location, companion or brand detail.
const rules:[RegExp,string,string][]=[
  [/\blady\b/gi,'woman','woman'],[/\bguy\b/gi,'man','man'],[/\bhoodie\b/gi,'hooded sweatshirt','hooded top'],
  [/\bfloral\b/gi,'flower-patterned','floral-patterned'],[/\bwalks by\b/gi,'walks past','passes by'],
  [/\bwalks past\b/gi,'walks past','passes by'],[/\basks for (?:a )?photo\b/gi,'requests a photo','asks for a photograph'],
  [/\basks for (?:a )?picture\b/gi,'requests a picture','asks for a photograph'],[/\bcomes up\b/gi,'approaches','comes over'],
];
export function queryVariants(original:string) {
  const protectedPhrases:string[]=[];
  const protect=(text:string)=>text.replace(/"[^"\n]*"|'[^'\n]*'|\b[A-Z][a-zA-Z]+(?:\s+[A-Z][a-zA-Z]+)+\b/g,value=>{protectedPhrases.push(value);return `\uE000${protectedPhrases.length-1}\uE001`;});
  const masked=protect(original.trim());
  const restore=(text:string)=>text.replace(/\uE000(\d+)\uE001/g,(_all,index)=>protectedPhrases[Number(index)]!);
  const natural=(variant:1|2)=>{let text=masked;for(const [pattern,first,second]of rules)text=text.replace(pattern,variant===1?first:second);return restore(text);};
  const first=natural(1),second=natural(2);
  const variants=[first,second,`Footage where ${first}`,`Video showing ${second}`,`Find the moment: ${original.trim()}`,`A scene where ${original.trim()}`];
  const seen=new Set([original.trim().toLowerCase()]);return variants.filter(value=>{const key=value.toLowerCase();if(value.length>1200||seen.has(key))return false;seen.add(key);return true;});
}
