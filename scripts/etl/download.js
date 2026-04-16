#!/usr/bin/env node
/**
 * download.js - Universal CMS data downloader
 * Reads cms-manifest.json and downloads all ZIP/CSV files
 * Extracts ZIPs, organizes by category
 */
const fs = require('fs');
const path = require('path');
const https = require('https');
const http = require('http');
const { execSync } = require('child_process');
const manifest = require('../cms-manifest.json');

const DATA_DIR = path.join(__dirname, '..', '..', 'data');
const CONCURRENT_DOWNLOADS = 3;

function ensureDir(dir) {
  if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
}

function downloadFile(url, dest) {
  return new Promise((resolve, reject) => {
    const file = fs.createWriteStream(dest);
    const protocol = url.startsWith('https') ? https : http;
    console.log(`  Downloading: ${url}`);
    protocol.get(url, (response) => {
      if (response.statusCode === 301 || response.statusCode === 302) {
        file.close();
        fs.unlinkSync(dest);
        return downloadFile(response.headers.location, dest).then(resolve).catch(reject);
      }
      if (response.statusCode !== 200) {
        file.close();
        fs.unlinkSync(dest);
        return reject(new Error(`HTTP ${response.statusCode} for ${url}`));
      }
      response.pipe(file);
      file.on('finish', () => { file.close(); resolve(dest); });
    }).on('error', (err) => {
      file.close();
      if (fs.existsSync(dest)) fs.unlinkSync(dest);
      reject(err);
    });
  });
}

function extractZip(zipPath, destDir) {
  ensureDir(destDir);
  try {
    execSync(`unzip -o "${zipPath}" -d "${destDir}"`, { stdio: 'pipe' });
    console.log(`  Extracted: ${path.basename(zipPath)}`);
  } catch (err) {
    console.error(`  Failed to extract ${zipPath}: ${err.message}`);
  }
}

async function downloadSource(key, source) {
  const category = source.category || key;
  const categoryDir = path.join(DATA_DIR, category);
  ensureDir(categoryDir);

  const url = source.url;
  if (!url) {
    console.log(`  Skipping ${key}: no direct URL (pattern-based)`);
    return;
  }

  const filename = path.basename(new URL(url).pathname) || `${key}.zip`;
  const destPath = path.join(categoryDir, filename);

  if (fs.existsSync(destPath)) {
    console.log(`  Already exists: ${filename}`);
  } else {
    try {
      await downloadFile(url, destPath);
    } catch (err) {
      console.error(`  Error downloading ${key}: ${err.message}`);
      return;
    }
  }

  if (destPath.endsWith('.zip')) {
    const extractDir = path.join(categoryDir, path.basename(filename, '.zip'));
    extractZip(destPath, extractDir);
  }
}

async function downloadMonthlyFiles() {
  const monthly = manifest.additional_monthly_enrollment;
  if (!monthly) return;

  const enrollDir = path.join(DATA_DIR, 'enrollment');
  ensureDir(enrollDir);

  for (const month of monthly.months) {
    const url = monthly.url_pattern
      .replace('{month}', month)
      .replace('{year}', '2026');
    const filename = path.basename(url);
    const destPath = path.join(enrollDir, filename);

    if (fs.existsSync(destPath)) {
      console.log(`  Already exists: ${filename}`);
      continue;
    }
    try {
      await downloadFile(url, destPath);
      if (destPath.endsWith('.zip')) {
        extractZip(destPath, path.join(enrollDir, path.basename(filename, '.zip')));
      }
    } catch (err) {
      console.error(`  Monthly ${month}: ${err.message}`);
    }
  }
}

async function downloadServiceAreaFiles() {
  const sa = manifest.additional_service_area;
  if (!sa) return;

  const saDir = path.join(DATA_DIR, 'service_area');
  ensureDir(saDir);

  const months = ['january', 'february', 'march', 'april'];
  for (const month of months) {
    for (const [prefix, pattern] of [['ma', sa.ma_url_pattern], ['pdp', sa.pdp_url_pattern]]) {
      const url = pattern.replace('{month}', month).replace('{year}', '2026');
      const filename = path.basename(url);
      const destPath = path.join(saDir, filename);
      if (fs.existsSync(destPath)) continue;
      try {
        await downloadFile(url, destPath);
        if (destPath.endsWith('.zip')) {
          extractZip(destPath, path.join(saDir, path.basename(filename, '.zip')));
        }
      } catch (err) {
        console.error(`  Service area ${prefix}-${month}: ${err.message}`);
      }
    }
  }
}

async function main() {
  console.log('=== CMS Medicare Data Downloader ===');
  console.log(`Data directory: ${DATA_DIR}\n`);
  ensureDir(DATA_DIR);

  // Download primary sources from manifest
  const sources = manifest.sources || {};
  const keys = Object.keys(sources)
    .sort((a, b) => (sources[a].priority || 99) - (sources[b].priority || 99));

  for (const key of keys) {
    console.log(`\n[${key}] ${sources[key].description}`);
    await downloadSource(key, sources[key]);
  }

  // Download monthly enrollment files
  console.log('\n[monthly_enrollment] Downloading monthly CPSC files...');
  await downloadMonthlyFiles();

  // Download service area files
  console.log('\n[service_area] Downloading service area files...');
  await downloadServiceAreaFiles();

  console.log('\n=== Download complete ===');
  console.log('Run ingestion scripts next: npm run ingest:all');
}

main().catch(console.error);
