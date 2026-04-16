#!/usr/bin/env node
/**
 * ingest-landscape.js - Ingest MA/PDP Landscape CSV into plans table
 * Handles the primary plan directory data for quote engine
 */
const fs = require('fs');
const path = require('path');
const csv = require('csv-parser');
const { Pool } = require('pg');
require('dotenv').config({ path: path.join(__dirname, '..', '..', '.env') });

const pool = new Pool({ connectionString: process.env.DATABASE_URL });
const DATA_DIR = path.join(__dirname, '..', '..', 'data');

const BATCH_SIZE = 500;

async function findCsvFiles(category) {
  const dir = path.join(DATA_DIR, category);
  const files = [];
  if (!fs.existsSync(dir)) return files;
  const walk = (d) => {
    for (const f of fs.readdirSync(d)) {
      const full = path.join(d, f);
      if (fs.statSync(full).isDirectory()) walk(full);
      else if (f.toLowerCase().endsWith('.csv')) files.push(full);
    }
  };
  walk(dir);
  return files;
}

function parseRow(row) {
  // Normalize header keys (CMS CSVs have inconsistent casing)
  const norm = {};
  for (const [k, v] of Object.entries(row)) {
    norm[k.trim().toLowerCase().replace(/[\s-]+/g, '_')] = v?.trim() || null;
  }
  return norm;
}

async function ingestLandscape() {
  console.log('=== Ingesting Landscape/Plan Data ===');
  const files = await findCsvFiles('landscape');
  if (files.length === 0) {
    console.log('No landscape CSV files found. Run download first.');
    return;
  }

  let totalRows = 0;
  for (const file of files) {
    console.log(`Processing: ${path.basename(file)}`);
    const rows = [];

    await new Promise((resolve, reject) => {
      fs.createReadStream(file)
        .pipe(csv())
        .on('data', (raw) => {
          const r = parseRow(raw);
          rows.push(r);
        })
        .on('end', resolve)
        .on('error', reject);
    });

    // Batch upsert
    for (let i = 0; i < rows.length; i += BATCH_SIZE) {
      const batch = rows.slice(i, i + BATCH_SIZE);
      const values = [];
      const placeholders = [];
      let idx = 1;

      for (const r of batch) {
        placeholders.push(`($${idx++}, $${idx++}, $${idx++}, $${idx++}, $${idx++}, $${idx++}, $${idx++}, $${idx++}, $${idx++}, $${idx++}, $${idx++}, $${idx++}, $${idx++}, $${idx++}, $${idx++}, $${idx++})`);
        values.push(
          r.contract_id || r.contract_number,
          r.plan_id || r.pbp_id || r.plan_number,
          r.segment_id || r.segment || '000',
          parseInt(r.plan_year || '2026'),
          r.plan_name || r.plan_marketing_name,
          r.plan_type || r.plan_type_code,
          r.organization_name || r.org_name || r.parent_organization,
          r.county_name || r.county,
          r.state || r.state_code,
          r.fips_state_county_code || r.county_fips || r.fips,
          parseFloat(r.monthly_premium || r.premium || '0') || 0,
          parseFloat(r.drug_premium || r.part_d_premium || '0') || 0,
          parseFloat(r.monthly_drug_deductible || r.drug_deductible || '0') || 0,
          r.snp_type || null,
          r.plan_type === 'PDP' || r.contract_id?.startsWith('S') ? 'PDP' : 'MA',
          JSON.stringify(r)
        );
      }

      const query = `
        INSERT INTO plans (
          contract_id, plan_id, segment_id, plan_year,
          plan_name, plan_type, organization_name,
          county, state, fips_code,
          monthly_premium, drug_premium, drug_deductible,
          snp_type, product_type, raw_data
        ) VALUES ${placeholders.join(', ')}
        ON CONFLICT (contract_id, plan_id, segment_id, plan_year, fips_code)
        DO UPDATE SET
          plan_name = EXCLUDED.plan_name,
          plan_type = EXCLUDED.plan_type,
          organization_name = EXCLUDED.organization_name,
          monthly_premium = EXCLUDED.monthly_premium,
          drug_premium = EXCLUDED.drug_premium,
          drug_deductible = EXCLUDED.drug_deductible,
          snp_type = EXCLUDED.snp_type,
          raw_data = EXCLUDED.raw_data,
          updated_at = NOW()`;

      try {
        await pool.query(query, values);
        totalRows += batch.length;
      } catch (err) {
        console.error(`  Batch error at row ${i}: ${err.message}`);
      }
    }
    console.log(`  Inserted/updated ${rows.length} rows`);
  }

  console.log(`\nTotal landscape rows processed: ${totalRows}`);
}

async function ingestPlanDirectory() {
  console.log('\n=== Ingesting MA Plan Directory ===');
  const files = await findCsvFiles('directory');
  if (files.length === 0) {
    console.log('No directory CSV files found.');
    return;
  }

  for (const file of files) {
    console.log(`Processing: ${path.basename(file)}`);
    const rows = [];
    await new Promise((resolve, reject) => {
      fs.createReadStream(file)
        .pipe(csv())
        .on('data', (raw) => rows.push(parseRow(raw)))
        .on('end', resolve)
        .on('error', reject);
    });

    for (let i = 0; i < rows.length; i += BATCH_SIZE) {
      const batch = rows.slice(i, i + BATCH_SIZE);
      for (const r of batch) {
        try {
          await pool.query(`
            UPDATE plans SET
              organization_name = COALESCE($1, organization_name),
              raw_data = raw_data || $2::jsonb
            WHERE contract_id = $3 AND plan_id = $4 AND plan_year = $5
          `, [
            r.organization_name || r.org_name,
            JSON.stringify(r),
            r.contract_id || r.contract_number,
            r.plan_id || r.pbp_id,
            parseInt(r.plan_year || '2026')
          ]);
        } catch (err) {
          // skip individual row errors
        }
      }
    }
    console.log(`  Processed ${rows.length} directory entries`);
  }
}

async function main() {
  try {
    await ingestLandscape();
    await ingestPlanDirectory();
  } finally {
    await pool.end();
  }
}

main().catch(console.error);
