#!/usr/bin/env node
/**
 * ingest-benefits.js - Ingest PBP Benefits data into benefits table
 * Also handles enrollment CPSC and star ratings
 */
const fs = require('fs');
const path = require('path');
const csv = require('csv-parser');
const { Pool } = require('pg');
require('dotenv').config({ path: path.join(__dirname, '..', '..', '.env') });

const pool = new Pool({ connectionString: process.env.DATABASE_URL });
const DATA_DIR = path.join(__dirname, '..', '..', 'data');
const BATCH_SIZE = 500;

function findCsvFiles(category) {
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

function norm(row) {
  const o = {};
  for (const [k, v] of Object.entries(row)) {
    o[k.trim().toLowerCase().replace(/[\s-]+/g, '_')] = v?.trim() || null;
  }
  return o;
}

async function ingestBenefits() {
  console.log('=== Ingesting PBP Benefits ===');
  const files = findCsvFiles('benefits');
  if (!files.length) { console.log('No benefits files found.'); return; }

  let total = 0;
  for (const file of files) {
    console.log(`Processing: ${path.basename(file)}`);
    const rows = [];
    await new Promise((res, rej) => {
      fs.createReadStream(file).pipe(csv())
        .on('data', (r) => rows.push(norm(r)))
        .on('end', res).on('error', rej);
    });

    for (let i = 0; i < rows.length; i += BATCH_SIZE) {
      const batch = rows.slice(i, i + BATCH_SIZE);
      for (const r of batch) {
        try {
          await pool.query(`
            INSERT INTO benefits (contract_id, plan_id, segment_id, plan_year,
              benefit_category, benefit_name, cost_sharing, authorization_required,
              referral_required, benefit_limit, raw_data)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
            ON CONFLICT (contract_id, plan_id, segment_id, plan_year, benefit_category)
            DO UPDATE SET benefit_name=EXCLUDED.benefit_name,
              cost_sharing=EXCLUDED.cost_sharing, raw_data=EXCLUDED.raw_data,
              updated_at=NOW()
          `, [
            r.contract_id || r.bid_id?.substring(0,5),
            r.plan_id || r.pbp_id || r.bid_id?.substring(5,8),
            r.segment_id || r.segment || '000',
            parseInt(r.plan_year || '2026'),
            r.benefit_category || r.category_description || r.pbp_a_ben_category,
            r.benefit_name || r.benefit_description || r.covered_benefit,
            r.cost_sharing || r.copay_coinsurance || r.ehc_cost_sharing,
            r.authorization_required === 'Y' || r.prior_auth === 'Yes',
            r.referral_required === 'Y' || r.referral === 'Yes',
            r.benefit_limit || r.quantity_limit || null,
            JSON.stringify(r)
          ]);
          total++;
        } catch (err) { /* skip row errors */ }
      }
    }
    console.log(`  Processed ${rows.length} benefit rows`);
  }
  console.log(`Total benefits ingested: ${total}`);
}

async function ingestEnrollment() {
  console.log('\n=== Ingesting Enrollment CPSC ===');
  const files = findCsvFiles('enrollment');
  if (!files.length) { console.log('No enrollment files found.'); return; }

  let total = 0;
  for (const file of files) {
    console.log(`Processing: ${path.basename(file)}`);
    const rows = [];
    await new Promise((res, rej) => {
      fs.createReadStream(file).pipe(csv())
        .on('data', (r) => rows.push(norm(r)))
        .on('end', res).on('error', rej);
    });

    for (let i = 0; i < rows.length; i += BATCH_SIZE) {
      const batch = rows.slice(i, i + BATCH_SIZE);
      for (const r of batch) {
        try {
          await pool.query(`
            INSERT INTO enrollment (contract_id, plan_id, plan_year, fips_code,
              state, county, enrollment_count, data_month, raw_data)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
            ON CONFLICT (contract_id, plan_id, plan_year, fips_code, data_month)
            DO UPDATE SET enrollment_count=EXCLUDED.enrollment_count,
              raw_data=EXCLUDED.raw_data, updated_at=NOW()
          `, [
            r.contract_number || r.contract_id,
            r.plan_id || r.pbp_id,
            parseInt(r.year || r.plan_year || '2026'),
            r.fips_state_county_code || r.fips_cnty || r.fips,
            r.state || r.state_code,
            r.county || r.county_name,
            parseInt(r.enrollment || r.total_enrollment || '0') || 0,
            r.month || path.basename(file).match(/(january|february|march|april|may|june|july|august|september|october|november|december)/i)?.[0] || 'unknown',
            JSON.stringify(r)
          ]);
          total++;
        } catch (err) { /* skip */ }
      }
    }
    console.log(`  Processed ${rows.length} enrollment rows`);
  }
  console.log(`Total enrollment rows: ${total}`);
}

async function ingestRatings() {
  console.log('\n=== Ingesting Star Ratings ===');
  const files = findCsvFiles('ratings');
  if (!files.length) { console.log('No ratings files found.'); return; }

  for (const file of files) {
    console.log(`Processing: ${path.basename(file)}`);
    const rows = [];
    await new Promise((res, rej) => {
      fs.createReadStream(file).pipe(csv())
        .on('data', (r) => rows.push(norm(r)))
        .on('end', res).on('error', rej);
    });

    for (const r of rows) {
      try {
        await pool.query(`
          INSERT INTO ratings (contract_id, plan_year, overall_rating, health_rating,
            drug_rating, organization_name, raw_data)
          VALUES ($1,$2,$3,$4,$5,$6,$7)
          ON CONFLICT (contract_id, plan_year)
          DO UPDATE SET overall_rating=EXCLUDED.overall_rating,
            health_rating=EXCLUDED.health_rating, drug_rating=EXCLUDED.drug_rating,
            raw_data=EXCLUDED.raw_data, updated_at=NOW()
        `, [
          r.contract_id || r.contract_number,
          parseInt(r.plan_year || r.year || '2026'),
          parseFloat(r.overall_star_rating || r.overall_rating || '0') || null,
          parseFloat(r.health_plan_star_rating || r.part_c_summary || '0') || null,
          parseFloat(r.drug_plan_star_rating || r.part_d_summary || '0') || null,
          r.organization_name || r.org_name,
          JSON.stringify(r)
        ]);
      } catch (err) { /* skip */ }
    }
    console.log(`  Processed ${rows.length} rating rows`);
  }
}

async function ingestFormulary() {
  console.log('\n=== Ingesting Formulary Data ===');
  const files = findCsvFiles('formulary');
  if (!files.length) { console.log('No formulary files found.'); return; }

  let total = 0;
  for (const file of files) {
    console.log(`Processing: ${path.basename(file)}`);
    const rows = [];
    await new Promise((res, rej) => {
      fs.createReadStream(file).pipe(csv())
        .on('data', (r) => rows.push(norm(r)))
        .on('end', res).on('error', rej);
    });

    for (let i = 0; i < rows.length; i += BATCH_SIZE) {
      const batch = rows.slice(i, i + BATCH_SIZE);
      for (const r of batch) {
        try {
          await pool.query(`
            INSERT INTO formulary (contract_id, plan_id, plan_year, rxcui,
              drug_name, tier, quantity_limit, step_therapy, prior_auth, raw_data)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
            ON CONFLICT (contract_id, plan_id, plan_year, rxcui)
            DO UPDATE SET drug_name=EXCLUDED.drug_name, tier=EXCLUDED.tier,
              quantity_limit=EXCLUDED.quantity_limit, raw_data=EXCLUDED.raw_data,
              updated_at=NOW()
          `, [
            r.contract_id || r.formulary_id?.substring(0,5),
            r.plan_id || r.pbp_id || r.formulary_id?.substring(5,8),
            parseInt(r.plan_year || '2026'),
            r.rxcui || r.rxnorm_id || r.ndc,
            r.drug_name || r.proprietary_name || r.nonproprietary_name,
            parseInt(r.tier_level_value || r.tier || '0') || 0,
            r.quantity_limit_yn === 'Y' || r.quantity_limit === 'Yes',
            r.step_therapy_yn === 'Y' || r.step_therapy === 'Yes',
            r.prior_authorization_yn === 'Y' || r.prior_auth === 'Yes',
            JSON.stringify(r)
          ]);
          total++;
        } catch (err) { /* skip */ }
      }
    }
    console.log(`  Processed ${rows.length} formulary rows`);
  }
  console.log(`Total formulary rows: ${total}`);
}

async function ingestServiceAreas() {
  console.log('\n=== Ingesting Service Areas ===');
  const files = findCsvFiles('service_area');
  if (!files.length) { console.log('No service area files found.'); return; }

  for (const file of files) {
    console.log(`Processing: ${path.basename(file)}`);
    const rows = [];
    await new Promise((res, rej) => {
      fs.createReadStream(file).pipe(csv())
        .on('data', (r) => rows.push(norm(r)))
        .on('end', res).on('error', rej);
    });

    for (const r of rows) {
      try {
        await pool.query(`
          INSERT INTO service_areas (contract_id, plan_year, fips_code, state, county,
            partial_county, raw_data)
          VALUES ($1,$2,$3,$4,$5,$6,$7)
          ON CONFLICT (contract_id, plan_year, fips_code)
          DO UPDATE SET partial_county=EXCLUDED.partial_county,
            raw_data=EXCLUDED.raw_data, updated_at=NOW()
        `, [
          r.contract_number || r.contract_id,
          parseInt(r.year || r.plan_year || '2026'),
          r.fips_state_county_code || r.fips || r.county_fips,
          r.state || r.state_code,
          r.county || r.county_name,
          r.partial_county === 'Y' || r.partial === 'Yes',
          JSON.stringify(r)
        ]);
      } catch (err) { /* skip */ }
    }
    console.log(`  Processed ${rows.length} service area rows`);
  }
}

async function main() {
  try {
    await ingestBenefits();
    await ingestEnrollment();
    await ingestRatings();
    await ingestFormulary();
    await ingestServiceAreas();
    console.log('\n=== All ingestion complete ===');
  } finally {
    await pool.end();
  }
}

main().catch(console.error);
