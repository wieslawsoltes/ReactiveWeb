import { rm } from 'node:fs/promises';
for (const path of ['dist', 'site', '.test']) await rm(path, { recursive: true, force: true });
