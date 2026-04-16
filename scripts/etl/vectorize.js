#!/usr/bin/env node
/**
 * vectorize.js - Generate embeddings from plan data for semantic search
 * Creates text chunks from structured data and embeds via OpenAI
 */
const { Pool } = require('pg');
const OpenAI = require('openai');
const path = require('path');
require('dotenv').config({ path: path.join(__dirname, '..', '..', '.env') });

const pool = new Pool({ connectionString: process.env.DATABASE_URL });
const openai = new OpenAI({ apiKey: process.env.OPENAI_API_KEY });

const EMBED_MODEL = process.env.EMBED_MODEL || 'text-embedding-3-small';
const BATCH_SIZE = 50;
const CHUNK_SIZE = 1500;

function buildPlanChunk(plan, benefits, rating) {
  let text = `Plan: ${plan.plan_name}\n`;
  text += `Organization: ${plan.organization_name}\n`;
  text += `Type: ${plan.plan_type} (${plan.product_type})\n`;
  text += `Contract: ${plan.contract_id}-${plan.plan_id}-${plan.segment_id}\n`;
  text += `County: ${plan.county}, ${plan.state} (FIPS: ${plan.fips_code})\n`;
  text += `Monthly Premium: $${plan.monthly_premium}\n`;
  text += `Drug Premium: $${plan.drug_premium}\n`;
  text += `Drug Deductible: $${plan.drug_deductible}\n`;
  if (plan.snp_type) text += `SNP Type: ${plan.snp_type}\n`;
  if (rating) {
    text += `Overall Star Rating: ${rating.overall_rating}/5\n`;
    text += `Health Rating: ${rating.health_rating}/5\n`;
    text += `Drug Rating: ${rating.drug_rating}/5\n`;
  }
  if (benefits && benefits.length > 0) {
    text += `\nKey Benefits:\n`;
    for (const b of benefits.slice(0, 20)) {
      text += `- ${b.benefit_category}: ${b.benefit_name || ''} (${b.cost_sharing || 'N/A'})\n`;
    }
  }
  return text;
}

async function getEmbedding(text) {
  const resp = await openai.embeddings.create({
    model: EMBED_MODEL,
    input: text.substring(0, 8000)
  });
  return resp.data[0].embedding;
}

async function vectorizePlans() {
  console.log('=== Vectorizing Plan Data ===');

  const { rows: plans } = await pool.query(`
    SELECT p.*, r.overall_rating, r.health_rating, r.drug_rating
    FROM plans p
    LEFT JOIN ratings r ON p.contract_id = r.contract_id AND p.plan_year = r.plan_year
    WHERE p.id NOT IN (SELECT DISTINCT (metadata->>'plan_id')::int FROM vector_chunks WHERE metadata->>'plan_id' IS NOT NULL)
    ORDER BY p.id
  `);

  console.log(`Found ${plans.length} plans to vectorize`);

  for (let i = 0; i < plans.length; i += BATCH_SIZE) {
    const batch = plans.slice(i, i + BATCH_SIZE);
    const chunks = [];

    for (const plan of batch) {
      const { rows: benefits } = await pool.query(
        'SELECT * FROM benefits WHERE contract_id=$1 AND plan_id=$2 AND plan_year=$3',
        [plan.contract_id, plan.plan_id, plan.plan_year]
      );

      const text = buildPlanChunk(plan, benefits, plan);
      chunks.push({ plan, text });
    }

    // Batch embed
    try {
      const texts = chunks.map(c => c.text.substring(0, 8000));
      const resp = await openai.embeddings.create({
        model: EMBED_MODEL,
        input: texts
      });

      for (let j = 0; j < chunks.length; j++) {
        const embedding = resp.data[j].embedding;
        const { plan, text } = chunks[j];
        const vecStr = `[${embedding.join(',')}]`;

        await pool.query(`
          INSERT INTO vector_chunks (content, embedding, source_table, source_id, metadata)
          VALUES ($1, $2::vector, $3, $4, $5)
        `, [
          text,
          vecStr,
          'plans',
          plan.id,
          JSON.stringify({
            plan_id: plan.id,
            contract_id: plan.contract_id,
            plan_name: plan.plan_name,
            county: plan.county,
            state: plan.state,
            fips_code: plan.fips_code,
            plan_type: plan.plan_type,
            premium: plan.monthly_premium
          })
        ]);
      }

      console.log(`  Vectorized batch ${Math.floor(i/BATCH_SIZE)+1}/${Math.ceil(plans.length/BATCH_SIZE)} (${chunks.length} plans)`);
    } catch (err) {
      console.error(`  Embedding error at batch ${i}: ${err.message}`);
      // Retry individually
      for (const { plan, text } of chunks) {
        try {
          const embedding = await getEmbedding(text);
          const vecStr = `[${embedding.join(',')}]`;
          await pool.query(`
            INSERT INTO vector_chunks (content, embedding, source_table, source_id, metadata)
            VALUES ($1, $2::vector, $3, $4, $5)
          `, [text, vecStr, 'plans', plan.id, JSON.stringify({ plan_id: plan.id, contract_id: plan.contract_id })]);
        } catch (e) {
          console.error(`  Failed plan ${plan.contract_id}-${plan.plan_id}: ${e.message}`);
        }
      }
    }

    // Rate limit
    if (i + BATCH_SIZE < plans.length) {
      await new Promise(r => setTimeout(r, 200));
    }
  }
}

async function vectorizeFormulary() {
  console.log('\n=== Vectorizing Formulary Data ===');

  const { rows } = await pool.query(`
    SELECT contract_id, plan_id, plan_year,
      array_agg(drug_name ORDER BY drug_name) as drugs,
      count(*) as drug_count
    FROM formulary
    GROUP BY contract_id, plan_id, plan_year
    HAVING count(*) > 0
  `);

  console.log(`Found ${rows.length} plan formularies to vectorize`);

  for (let i = 0; i < rows.length; i += BATCH_SIZE) {
    const batch = rows.slice(i, i + BATCH_SIZE);
    const texts = batch.map(r => {
      const drugs = r.drugs.slice(0, 100).join(', ');
      return `Formulary for ${r.contract_id}-${r.plan_id} (${r.plan_year}): ${r.drug_count} drugs covered including: ${drugs}`;
    });

    try {
      const resp = await openai.embeddings.create({ model: EMBED_MODEL, input: texts });
      for (let j = 0; j < batch.length; j++) {
        const vecStr = `[${resp.data[j].embedding.join(',')}]`;
        await pool.query(`
          INSERT INTO vector_chunks (content, embedding, source_table, source_id, metadata)
          VALUES ($1, $2::vector, $3, $4, $5)
        `, [
          texts[j],
          vecStr,
          'formulary',
          0,
          JSON.stringify({ contract_id: batch[j].contract_id, plan_id: batch[j].plan_id, drug_count: batch[j].drug_count })
        ]);
      }
      console.log(`  Vectorized formulary batch ${Math.floor(i/BATCH_SIZE)+1}`);
    } catch (err) {
      console.error(`  Formulary batch error: ${err.message}`);
    }
    await new Promise(r => setTimeout(r, 200));
  }
}

async function main() {
  try {
    await vectorizePlans();
    await vectorizeFormulary();
    
    const { rows } = await pool.query('SELECT count(*) FROM vector_chunks');
    console.log(`\nTotal vector chunks: ${rows[0].count}`);
    console.log('=== Vectorization complete ===');
  } finally {
    await pool.end();
  }
}

main().catch(console.error);
