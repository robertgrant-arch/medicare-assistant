const express = require('express');
const cors = require('cors');
const path = require('path');
const { Pool } = require('pg');
const OpenAI = require('openai');
require('dotenv').config();

const app = express();
const pool = new Pool({ connectionString: process.env.DATABASE_URL });
const openai = new OpenAI({ apiKey: process.env.OPENAI_API_KEY });
const EMBED_MODEL = process.env.EMBED_MODEL || 'text-embedding-3-small';
const CHAT_MODEL = process.env.CHAT_MODEL || 'gpt-4o';
const PORT = process.env.PORT || 3000;

app.use(cors());
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

// Health check
app.get('/api/health', async (req, res) => {
  try {
    const { rows } = await pool.query('SELECT count(*) as plans FROM plans');
    const { rows: vecs } = await pool.query('SELECT count(*) as chunks FROM vector_chunks');
    res.json({ status: 'ok', plans: rows[0].plans, vectors: vecs[0].chunks });
  } catch (err) {
    res.status(500).json({ status: 'error', message: err.message });
  }
});

// Semantic search endpoint
async function semanticSearch(query, filters = {}, limit = 10) {
  const resp = await openai.embeddings.create({ model: EMBED_MODEL, input: query });
  const embedding = resp.data[0].embedding;
  const vecStr = `[${embedding.join(',')}]`;

  let where = '';
  const params = [vecStr, limit];
  let idx = 3;

  if (filters.state) {
    where += ` AND metadata->>'state' = $${idx++}`;
    params.push(filters.state);
  }
  if (filters.county) {
    where += ` AND metadata->>'county' ILIKE $${idx++}`;
    params.push(`%${filters.county}%`);
  }
  if (filters.fips) {
    where += ` AND metadata->>'fips_code' = $${idx++}`;
    params.push(filters.fips);
  }
  if (filters.plan_type) {
    where += ` AND metadata->>'plan_type' ILIKE $${idx++}`;
    params.push(`%${filters.plan_type}%`);
  }

  const { rows } = await pool.query(`
    SELECT content, metadata,
      1 - (embedding <=> $1::vector) as similarity
    FROM vector_chunks
    WHERE 1=1 ${where}
    ORDER BY embedding <=> $1::vector
    LIMIT $2
  `, params);

  return rows;
}

// Structured plan search (for quote engine)
app.get('/api/plans', async (req, res) => {
  try {
    const { state, county, fips, zip, plan_type, max_premium, drug_name, limit = 50 } = req.query;
    let where = 'WHERE 1=1';
    const params = [];
    let idx = 1;

    if (state) { where += ` AND p.state = $${idx++}`; params.push(state); }
    if (county) { where += ` AND p.county ILIKE $${idx++}`; params.push(`%${county}%`); }
    if (fips) { where += ` AND p.fips_code = $${idx++}`; params.push(fips); }
    if (plan_type) { where += ` AND p.plan_type ILIKE $${idx++}`; params.push(`%${plan_type}%`); }
    if (max_premium) { where += ` AND p.monthly_premium <= $${idx++}`; params.push(parseFloat(max_premium)); }

    let joinFormulary = '';
    if (drug_name) {
      joinFormulary = `INNER JOIN formulary f ON p.contract_id = f.contract_id AND p.plan_id = f.plan_id AND p.plan_year = f.plan_year AND f.drug_name ILIKE $${idx++}`;
      params.push(`%${drug_name}%`);
    }

    params.push(parseInt(limit));

    const { rows } = await pool.query(`
      SELECT DISTINCT p.*, r.overall_rating, r.health_rating, r.drug_rating
      FROM plans p
      LEFT JOIN ratings r ON p.contract_id = r.contract_id AND p.plan_year = r.plan_year
      ${joinFormulary}
      ${where}
      ORDER BY p.monthly_premium ASC
      LIMIT $${idx}
    `, params);

    res.json({ plans: rows, total: rows.length });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Plan details with benefits
app.get('/api/plans/:contractId/:planId', async (req, res) => {
  try {
    const { contractId, planId } = req.params;
    const year = req.query.year || 2026;

    const { rows: plans } = await pool.query(
      'SELECT p.*, r.overall_rating, r.health_rating, r.drug_rating FROM plans p LEFT JOIN ratings r ON p.contract_id=r.contract_id AND p.plan_year=r.plan_year WHERE p.contract_id=$1 AND p.plan_id=$2 AND p.plan_year=$3',
      [contractId, planId, year]
    );
    const { rows: benefits } = await pool.query(
      'SELECT * FROM benefits WHERE contract_id=$1 AND plan_id=$2 AND plan_year=$3 ORDER BY benefit_category',
      [contractId, planId, year]
    );
    const { rows: formulary } = await pool.query(
      'SELECT * FROM formulary WHERE contract_id=$1 AND plan_id=$2 AND plan_year=$3 ORDER BY drug_name LIMIT 500',
      [contractId, planId, year]
    );
    const { rows: enrollment } = await pool.query(
      'SELECT * FROM enrollment WHERE contract_id=$1 AND plan_id=$2 AND plan_year=$3',
      [contractId, planId, year]
    );

    res.json({ plan: plans[0] || null, benefits, formulary, enrollment });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Drug search
app.get('/api/drugs', async (req, res) => {
  try {
    const { name, limit = 50 } = req.query;
    if (!name) return res.json({ drugs: [] });
    const { rows } = await pool.query(
      'SELECT DISTINCT drug_name, tier, prior_auth, step_therapy, quantity_limit, contract_id, plan_id FROM formulary WHERE drug_name ILIKE $1 LIMIT $2',
      [`%${name}%`, parseInt(limit)]
    );
    res.json({ drugs: rows });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Chat endpoint with RAG
app.post('/api/chat', async (req, res) => {
  try {
    const { message, history = [], filters = {} } = req.body;
    if (!message) return res.status(400).json({ error: 'Message required' });

    // Semantic search for context
    const results = await semanticSearch(message, filters, 8);
    const context = results.map(r => r.content).join('\n---\n');

    const systemPrompt = `You are a Medicare Assistant AI. You help users understand Medicare Advantage (MA), Medicare Part D (PDP), and Medigap plans. You have access to real CMS data for plan year 2026.

Use the following plan data context to answer questions accurately. If the data doesn't contain the answer, say so. Always cite specific plan names, premiums, and ratings when available.

For quote requests, provide specific plan recommendations with premiums, star ratings, and key benefits. Format currency values properly.

Context from CMS data:
${context}`;

    const messages = [
      { role: 'system', content: systemPrompt },
      ...history.slice(-10).map(h => ({ role: h.role, content: h.content })),
      { role: 'user', content: message }
    ];

    // Stream response
    res.setHeader('Content-Type', 'text/event-stream');
    res.setHeader('Cache-Control', 'no-cache');
    res.setHeader('Connection', 'keep-alive');

    const stream = await openai.chat.completions.create({
      model: CHAT_MODEL,
      messages,
      stream: true,
      temperature: 0.3,
      max_tokens: 2000
    });

    for await (const chunk of stream) {
      const content = chunk.choices[0]?.delta?.content;
      if (content) {
        res.write(`data: ${JSON.stringify({ content })}\n\n`);
      }
    }

    // Send sources
    const sources = results.map(r => ({
      plan_name: r.metadata?.plan_name,
      contract_id: r.metadata?.contract_id,
      similarity: r.similarity
    }));
    res.write(`data: ${JSON.stringify({ done: true, sources })}\n\n`);
    res.end();
  } catch (err) {
    if (!res.headersSent) {
      res.status(500).json({ error: err.message });
    } else {
      res.write(`data: ${JSON.stringify({ error: err.message })}\n\n`);
      res.end();
    }
  }
});

// Stats
app.get('/api/stats', async (req, res) => {
  try {
    const stats = {};
    for (const table of ['plans', 'benefits', 'enrollment', 'formulary', 'ratings', 'service_areas', 'vector_chunks']) {
      const { rows } = await pool.query(`SELECT count(*) FROM ${table}`);
      stats[table] = parseInt(rows[0].count);
    }
    res.json(stats);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.listen(PORT, () => {
  console.log(`Medicare Assistant API running on port ${PORT}`);
  console.log(`Chat UI: http://localhost:${PORT}`);
});
