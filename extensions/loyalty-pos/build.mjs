import {build} from 'esbuild';
// Local bundling only. Shopify CLI validation and native runtime are release gates.
await build({entryPoints:['src/CustomerBlock.tsx','src/HistoryModal.tsx','src/preview.tsx'],outdir:'dist',bundle:true,format:'esm',target:'es2022',jsx:'automatic',jsxImportSource:'preact',minify:true,sourcemap:false});
