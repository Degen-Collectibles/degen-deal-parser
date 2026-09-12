import {defineConfig} from '@playwright/test';
export default defineConfig({testDir:'tests/browser',workers:1,timeout:30000,use:{baseURL:process.env.LOYALTY_PREVIEW_URL||'http://127.0.0.1:8767',headless:true},reporter:'list'});
