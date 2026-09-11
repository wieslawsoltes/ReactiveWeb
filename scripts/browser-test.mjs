import assert from 'node:assert/strict';
import { createServer } from 'node:http';
import { readFile, mkdir } from 'node:fs/promises';
import { resolve, extname } from 'node:path';
import { chromium } from 'playwright';

// Ephemeral fixture server for the packaged showcase. No development preview is started.
const root = resolve('site');
const server = createServer(async(req,res)=>{
  try {
    const pathname = new URL(req.url,'http://localhost').pathname;
    const file = resolve(root,'.'+(pathname==='/'?'/index.html':decodeURIComponent(pathname)));
    if(!file.startsWith(root+'/')) { res.writeHead(403).end();return; }
    res.setHeader('Content-Type',({'.html':'text/html','.js':'text/javascript','.css':'text/css','.svg':'image/svg+xml'})[extname(file)]||'application/octet-stream');
    res.end(await readFile(file));
  } catch {res.writeHead(404).end();}
});
await new Promise(resolve=>server.listen(0,'127.0.0.1',resolve));
const browser = await chromium.launch({headless:true,args:['--no-sandbox']});
const page = await browser.newPage({viewport:{width:1440,height:1120},deviceScaleFactor:1});
page.setDefaultTimeout(12000);
const failures=[];page.on('pageerror',error=>{failures.push(error.message);console.error('Browser error:',error.message);});
const base=`http://127.0.0.1:${server.address().port}`;
let checks=0;
async function check(name,fn){await fn();checks++;console.log(`✓ ${name}`);}
async function waitText(selector,expected){await page.waitForFunction(({selector,expected})=>document.querySelector(selector)?.textContent?.includes(expected),{selector,expected});}
async function goto(name){await page.locator(`[data-example="${name}"]`).click();await page.waitForFunction(name=>location.hash==='#'+name && document.querySelector('[data-example="'+name+'"]')?.classList.contains('active'),name);}
try {
  await page.goto(base,{waitUntil:'load'});
  await check('profile binding, computed value and command execution',async()=>{
    await page.locator('#first-name').fill('Wiesław');await waitText('#full-name','Wiesław Morgan');
    await page.locator('#save').click();await waitText('#save-status','Profile saved');
    await page.locator('#first-name').fill('');assert.equal(await page.locator('#save').isDisabled(),true);
    await page.locator('#reset').click();await waitText('#full-name','Alex Morgan');
  });
  await check('nested property replacement and delayed changes',async()=>{
    await goto('properties');await page.locator('#replace').click();await waitText('#city-preview','Kraków');
    await page.locator('#city').fill('Gdynia');await waitText('#state','Gdynia');
    await page.locator('#batch').click();await waitText('#city-preview','Wrocław');
  });
  await check('asynchronous command cancellation, gating and observed exception',async()=>{
    await goto('commands');await page.locator('#run').click();await waitText('#progress','10%');
    await page.locator('#cancel').click();await page.waitForFunction(()=>document.querySelector('#cancel').disabled);
    await page.locator('#allowed').uncheck();assert.equal(await page.locator('#run').isDisabled(),true);
    await page.locator('#fail').click();await waitText('#events','deliberate failure');
  });
  await check('shadow DOM component detach and reactivation',async()=>{
    await goto('bindings');await page.locator('demo-counter button').click();await waitText('#state','3');
    await page.locator('#toggle-component').click();assert.equal(await page.locator('demo-counter').count(),0);
    await page.locator('#counter-input').fill('9');await page.locator('#toggle-component').click();
    assert.equal(await page.locator('demo-counter strong').textContent(),'9');
  });
  await check('activation leases share and dispose timer scope',async()=>{
    await goto('activation');await page.locator('#activate').click();await waitText('#state','"ReferenceCount": 1');
    await page.locator('#lease').click();await waitText('#state','"ReferenceCount": 2');
    await page.locator('#deactivate').click();await waitText('#state','"ReferenceCount": 1');
    await page.locator('#lease').click();await waitText('#state','"IsActive": false');
  });
  await check('routing host renders and restores view models',async()=>{
    await goto('routing');await waitText('#route-host','Workspace home');assert.equal(await page.locator('#back').isDisabled(),true);
    await page.locator('#navigate').click();await waitText('#route-host','Project details');
    await page.locator('#back').click();await waitText('#route-host','Workspace home');
  });
  await check('interaction confirmation and cancellation return typed outputs',async()=>{
    await goto('interactions');await page.locator('#ask').click();await page.locator('dialog button[value="confirm"]').click();await waitText('#decision','confirmed');
    await page.locator('#ask').click();await page.locator('dialog button[value="cancel"]').click();await waitText('#decision','cancelled');
  });
  await check('async validation gates command and clears obsolete errors',async()=>{
    await goto('validation');await page.locator('#username').fill('admin');await waitText('#username-errors','reserved');assert.equal(await page.locator('#submit-validation').isDisabled(),true);
    await page.locator('#username').fill('alexandra');await page.waitForFunction(()=>!document.querySelector('#submit-validation').disabled);
    await page.locator('#age').fill('12');await waitText('#age-errors','18 and 120');assert.equal(await page.locator('#submit-validation').isDisabled(),true);
  });
  await check('observable collection live filtering and batched mutations',async()=>{
    await goto('collections');assert.equal(await page.locator('#tasks li').count(),3);
    await page.locator('#only-active').check();assert.equal(await page.locator('#tasks li').count(),2);
    await page.locator('#task-name').fill('Verify shipping package');await page.locator('#add-task').click();assert.equal(await page.locator('#tasks li').count(),3);
    await page.locator('#batch-tasks').click();assert.equal(await page.locator('#tasks li').count(),6);
  });
  await check('persistent draft survives reload and invalidation',async()=>{
    await goto('persistence');await page.locator('#draft').fill('A durable reactive draft');await page.locator('#flush').click();await waitText('#persist-status','Saved at');
    await page.reload({waitUntil:'load'});assert.equal(await page.locator('#draft').inputValue(),'A durable reactive draft');
    await page.locator('#forget').click();await waitText('#persist-status','cleared');
  });
  await check('message bus latest channel and singleton resolution',async()=>{
    await goto('messaging');await page.locator('#send-message').click();await waitText('#received','View model updated');
    await page.locator('#latest').click();await waitText('#events','ListenIncludeLatest');
    await page.locator('#service').click();await waitText('#state','"Id": 1');
  });
  await check('schema-generated computed property updates',async()=>{
    await goto('generation');await waitText('#total','$37.50');await page.locator('#quantity').fill('4');await waitText('#total','$50.00');
  });
  await check('React StrictMode hooks update view and release activation',async()=>{
    await goto('react');await page.locator('#react-increment').click();assert.equal(await page.locator('[data-testid="react-count"]').textContent(),'1');
    await goto('overview');await waitText('#full-name','Alex Morgan');
  });
  await check('desktop, dark theme and mobile layout avoid horizontal overflow',async()=>{
    await mkdir('test-results',{recursive:true});
    await page.screenshot({path:'test-results/showcase-desktop.png',fullPage:true});
    await page.locator('#theme-toggle').click();assert.equal(await page.locator('html').getAttribute('data-theme'),'dark');
    await page.screenshot({path:'test-results/showcase-dark.png',fullPage:true});
    await page.setViewportSize({width:390,height:844});
    assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth),true);
    await page.screenshot({path:'test-results/showcase-mobile.png',fullPage:true});
  });
  assert.deepEqual(failures,[],'No uncaught browser errors');
  console.log(`${checks} browser checks passed; zero uncaught errors.`);
} finally {await browser.close();await new Promise(resolve=>server.close(resolve));}
