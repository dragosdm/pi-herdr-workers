#!/usr/bin/env node
// Usage: node audit-command.mjs PANE OUTPUT_DIR '{"id":"case","action":"snapshot"}'
// A bounded filesystem event wait avoids tiny pane viewport truncation.
import fs from 'node:fs';
import path from 'node:path';
import { spawnSync } from 'node:child_process';
const [pane, root, raw] = process.argv.slice(2);
const request = JSON.parse(raw);
const file = path.join(root, `${pane.replace(/[^a-z0-9-]/gi, '_')}.jsonl`);
await new Promise((resolve, reject) => {
  const check = () => {
    const lines = fs.existsSync(file) ? fs.readFileSync(file, 'utf8').trim().split('\n') : [];
    for (const line of lines) {
      let entry; try { entry = JSON.parse(line).data; } catch { continue; }
      if (entry?.request?.id !== request.id) continue;
      clearTimeout(timer); watcher.close();
      console.log(JSON.stringify(entry)); resolve(); return;
    }
  };
  const watcher = fs.watch(root, check);
  const timer = setTimeout(() => { watcher.close(); reject(new Error(`Audit timeout: ${request.id}`)); }, 150000);
  const sent = spawnSync('herdr', ['agent', 'prompt', pane, '/audit ' + raw], { encoding: 'utf8' });
  if (sent.status !== 0) { clearTimeout(timer); watcher.close(); reject(new Error(sent.stderr)); return; }
  check();
});
