const {chromium} = require('playwright');
(async () => {
  const browser = await chromium.launch({headless: true, executablePath: process.env.CHROMIUM});
  const result = {version: browser.version(), scripts: 360, scriptBytes: 47500, carriers: {tcp: [], fabric: []}};
  try {
    for (let trial = 0; trial < 10; trial++) {
      for (const name of trial % 2 ? ['fabric', 'tcp'] : ['tcp', 'fabric']) {
        const context = await browser.newContext();
        try {
          const page = await context.newPage();
          for (const cache of ['cold', 'warm']) {
            const start = performance.now();
            await page.goto(process.argv[name === 'tcp' ? 2 : 3], {waitUntil: 'load', timeout: 180000});
            const loadMs = performance.now() - start;
            const modules = await page.evaluate(() => Array.from({length: 360}, (_, i) => globalThis['module' + i] === 1).filter(Boolean).length);
            if (modules !== 360) throw new Error('incomplete graph: ' + modules);
            result.carriers[name].push({trial, cache, loadMs, modules});
          }
        } finally { await context.close(); }
      }
    }
    console.log(JSON.stringify(result));
  } finally { await browser.close(); }
})().catch(error => { console.error(error); process.exit(1); });
