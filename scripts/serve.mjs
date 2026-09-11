import { createServer } from 'node:http';
import { readFile } from 'node:fs/promises';
import { resolve, extname } from 'node:path';
const root = resolve('site');
const port = Number(process.env.PORT || 4173);
const types = {'.html':'text/html', '.js':'text/javascript', '.css':'text/css', '.svg':'image/svg+xml', '.json':'application/json', '.map':'application/json'};
createServer(async (req,res) => {
  const path = resolve(root, '.' + decodeURIComponent(new URL(req.url, 'http://localhost').pathname));
  if (path !== root && !path.startsWith(root + '/')) { res.writeHead(403).end(); return; }
  try { const file = path === root ? path + '/index.html' : path; res.setHeader('Content-Type', types[extname(file)] || 'application/octet-stream'); res.end(await readFile(file)); }
  catch { res.writeHead(404).end('Not found'); }
}).listen(port, '0.0.0.0', () => console.log(`ReactiveWeb showcase on http://localhost:${port}`));
