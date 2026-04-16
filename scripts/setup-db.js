import pg from 'pg';
import 'dotenv/config';

const { Pool } = pg;
const pool = new Pool({ connectionString: process.env.DATABASE_URL });

const SQL = `
-- Enable pgvector
CREATE EXTENSION IF NOT EXISTS vector;
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- ============================================
-- CORE PLAN TABLES (quote-engine grade)
-- ============================================

CREATE TABLE IF NOT EXISTS medicare_contracts (
  contract_id VARCHAR(5) PRIMARY KEY,
  org_name TEXT,
  org_type VARCHAR(50),
  plan_type_desc TEXT,
  parent_org TEXT,
  created_at TIMESTAMPTZ DEFAULT NOW(),
  updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS medicare_plans (
  id SERIAL PRIMARY KEY,
  contract_id VARCHAR(5) NOT NULL,
  pbp_id VARCHAR(3) NOT NULL,
  segment_id VARCHAR(3) DEFAULT '000',
  plan_year INT NOT NULL,
  plan_name TEXT,
  org_name TEXT,
  org_marketing_name TEXT,
  plan_type VARCHAR(50),
  plan_type_desc TEXT,
  snp_type VARCHAR(20),
  eghp_flag BOOLEAN DEFAULT FALSE,
  national_pdp_flag BOOLEAN DEFAULT FALSE,
  part_d_flag BOOLEAN DEFAULT FALSE,
  premium_part_c DECIMAL(10,2),
  premium_part_d DECIMAL(10,2),
  premium_total DECIMAL(10,2),
  premium_part_d_basic DECIMAL(10,2),
  premium_part_d_supplemental DECIMAL(10,2),
  drug_deductible DECIMAL(10,2),
  part_b_premium_reduction DECIMAL(10,2),
  moop_in_network INT,
  moop_in_out_combined INT,
  star_rating_overall DECIMAL(3,1),
  star_rating_part_c DECIMAL(3,1),
  star_rating_part_d DECIMAL(3,1),
  enrollment_count INT,
  created_at TIMESTAMPTZ DEFAULT NOW(),
  updated_at TIMESTAMPTZ DEFAULT NOW(),
  UNIQUE(contract_id, pbp_id, segment_id, plan_year)
);

CREATE TABLE IF NOT EXISTS medicare_plan_service_areas (
  id SERIAL PRIMARY KEY,
  contract_id VARCHAR(5) NOT NULL,
  pbp_id VARCHAR(3),
  plan_year INT NOT NULL,
  state VARCHAR(2) NOT NULL,
  county VARCHAR(100),
  county_fips VARCHAR(5),
  created_at TIMESTAMPTZ DEFAULT NOW(),
  UNIQUE(contract_id, pbp_id, plan_year, state, county_fips)
);

CREATE TABLE IF NOT EXISTS medicare_plan_enrollment (
  id SERIAL PRIMARY KEY,
  contract_id VARCHAR(5) NOT NULL,
  pbp_id VARCHAR(3) NOT NULL,
  plan_year INT NOT NULL,
  month INT NOT NULL,
  state VARCHAR(2),
  county VARCHAR(100),
  county_fips VARCHAR(5),
  enrollment INT,
  created_at TIMESTAMPTZ DEFAULT NOW(),
  UNIQUE(contract_id, pbp_id, plan_year, month, state, county_fips)
);

-- ============================================
-- BENEFITS TABLES
-- ============================================

CREATE TABLE IF NOT EXISTS medicare_plan_benefits (
  id SERIAL PRIMARY KEY,
  contract_id VARCHAR(5) NOT NULL,
  pbp_id VARCHAR(3) NOT NULL,
  segment_id VARCHAR(3) DEFAULT '000',
  plan_year INT NOT NULL,
  benefit_category VARCHAR(100),
  benefit_name TEXT,
  in_network_copay TEXT,
  in_network_coins TEXT,
  out_network_copay TEXT,
  out_network_coins TEXT,
  auth_required BOOLEAN,
  referral_required BOOLEAN,
  benefit_limit TEXT,
  notes TEXT,
  created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS medicare_plan_costs (
  id SERIAL PRIMARY KEY,
  contract_id VARCHAR(5) NOT NULL,
  pbp_id VARCHAR(3) NOT NULL,
  segment_id VARCHAR(3) DEFAULT '000',
  plan_year INT NOT NULL,
  monthly_premium_total DECIMAL(10,2),
  monthly_premium_medical DECIMAL(10,2),
  monthly_premium_drug DECIMAL(10,2),
  part_b_premium_reduction DECIMAL(10,2),
  deductible_medical INT,
  deductible_drug INT,
  moop_in_network INT,
  moop_in_out_combined INT,
  created_at TIMESTAMPTZ DEFAULT NOW(),
  UNIQUE(contract_id, pbp_id, segment_id, plan_year)
);

-- ============================================
-- STAR RATINGS
-- ============================================

CREATE TABLE IF NOT EXISTS medicare_star_ratings (
  id SERIAL PRIMARY KEY,
  contract_id VARCHAR(5) NOT NULL,
  plan_year INT NOT NULL,
  overall_star DECIMAL(3,1),
  part_c_star DECIMAL(3,1),
  part_d_star DECIMAL(3,1),
  health_plan_quality_star DECIMAL(3,1),
  drug_plan_quality_star DECIMAL(3,1),
  low_performing_icon BOOLEAN DEFAULT FALSE,
  high_performing_icon BOOLEAN DEFAULT FALSE,
  measure_scores JSONB,
  created_at TIMESTAMPTZ DEFAULT NOW(),
  UNIQUE(contract_id, plan_year)
);

-- ============================================
-- DRUG FORMULARY
-- ============================================

CREATE TABLE IF NOT EXISTS medicare_drug_formulary (
  id SERIAL PRIMARY KEY,
  contract_id VARCHAR(5) NOT NULL,
  pbp_id VARCHAR(3) NOT NULL,
  segment_id VARCHAR(3) DEFAULT '000',
  plan_year INT NOT NULL,
  ndc VARCHAR(20),
  drug_name TEXT,
  tier INT,
  prior_auth_flag BOOLEAN DEFAULT FALSE,
  step_therapy_flag BOOLEAN DEFAULT FALSE,
  quantity_limit_flag BOOLEAN DEFAULT FALSE,
  selected_drug_flag BOOLEAN DEFAULT FALSE,
  created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS medicare_drug_costs (
  id SERIAL PRIMARY KEY,
  contract_id VARCHAR(5) NOT NULL,
  pbp_id VARCHAR(3) NOT NULL,
  segment_id VARCHAR(3) DEFAULT '000',
  plan_year INT NOT NULL,
  tier INT,
  days_supply INT,
  pharmacy_type VARCHAR(50),
  copay DECIMAL(10,2),
  coinsurance DECIMAL(5,2),
  created_at TIMESTAMPTZ DEFAULT NOW()
);

-- ============================================
-- PHARMACY NETWORK
-- ============================================

CREATE TABLE IF NOT EXISTS medicare_pharmacy_network (
  id SERIAL PRIMARY KEY,
  contract_id VARCHAR(5) NOT NULL,
  pbp_id VARCHAR(3) NOT NULL,
  plan_year INT NOT NULL,
  npi VARCHAR(20),
  pharmacy_name TEXT,
  address TEXT,
  city VARCHAR(100),
  state VARCHAR(2),
  zip VARCHAR(10),
  preferred_flag BOOLEAN DEFAULT FALSE,
  mail_order_flag BOOLEAN DEFAULT FALSE,
  retail_flag BOOLEAN DEFAULT TRUE,
  created_at TIMESTAMPTZ DEFAULT NOW()
);

-- ============================================
-- SNP DATA
-- ============================================

CREATE TABLE IF NOT EXISTS medicare_snp_plans (
  id SERIAL PRIMARY KEY,
  contract_id VARCHAR(5) NOT NULL,
  pbp_id VARCHAR(3) NOT NULL,
  plan_year INT NOT NULL,
  snp_type VARCHAR(20),
  snp_condition TEXT,
  enrollment INT,
  created_at TIMESTAMPTZ DEFAULT NOW(),
  UNIQUE(contract_id, pbp_id, plan_year)
);

-- ============================================
-- PLAN CROSSWALK
-- ============================================

CREATE TABLE IF NOT EXISTS medicare_plan_crosswalk (
  id SERIAL PRIMARY KEY,
  prior_contract_id VARCHAR(5),
  prior_pbp_id VARCHAR(3),
  prior_segment_id VARCHAR(3),
  prior_plan_year INT,
  current_contract_id VARCHAR(5),
  current_pbp_id VARCHAR(3),
  current_segment_id VARCHAR(3),
  current_plan_year INT,
  crosswalk_type VARCHAR(50),
  created_at TIMESTAMPTZ DEFAULT NOW()
);

-- ============================================
-- VECTOR / DOCUMENT TABLES
-- ============================================

CREATE TABLE IF NOT EXISTS plan_documents (
  id SERIAL PRIMARY KEY,
  contract_id VARCHAR(5),
  pbp_id VARCHAR(3),
  plan_year INT,
  source_type VARCHAR(50),
  source_file TEXT,
  title TEXT,
  raw_text TEXT,
  created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS document_chunks (
  id SERIAL PRIMARY KEY,
  document_id INT REFERENCES plan_documents(id),
  contract_id VARCHAR(5),
  pbp_id VARCHAR(3),
  plan_year INT,
  state VARCHAR(2),
  county_fips VARCHAR(5),
  plan_type VARCHAR(50),
  snp_type VARCHAR(20),
  source_type VARCHAR(50),
  chunk_index INT,
  chunk_text TEXT NOT NULL,
  embedding vector(1536),
  metadata JSONB DEFAULT '{}',
  created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS uploaded_files (
  id SERIAL PRIMARY KEY,
  filename TEXT NOT NULL,
  original_name TEXT,
  file_type VARCHAR(20),
  size_bytes BIGINT,
  status VARCHAR(20) DEFAULT 'pending',
  created_at TIMESTAMPTZ DEFAULT NOW()
);

-- ============================================
-- CMS ETL TRACKING
-- ============================================

CREATE TABLE IF NOT EXISTS cms_etl_log (
  id SERIAL PRIMARY KEY,
  source_category VARCHAR(100) NOT NULL,
  source_file TEXT,
  plan_year INT,
  period VARCHAR(20),
  rows_loaded INT,
  status VARCHAR(20) DEFAULT 'complete',
  started_at TIMESTAMPTZ,
  completed_at TIMESTAMPTZ DEFAULT NOW(),
  error_message TEXT
);

-- ============================================
-- INDEXES
-- ============================================

CREATE INDEX IF NOT EXISTS idx_plans_contract ON medicare_plans(contract_id);
CREATE INDEX IF NOT EXISTS idx_plans_year ON medicare_plans(plan_year);
CREATE INDEX IF NOT EXISTS idx_plans_state ON medicare_plan_service_areas(state);
CREATE INDEX IF NOT EXISTS idx_plans_county ON medicare_plan_service_areas(county_fips);
CREATE INDEX IF NOT EXISTS idx_plans_type ON medicare_plans(plan_type);
CREATE INDEX IF NOT EXISTS idx_plans_star ON medicare_plans(star_rating_overall);
CREATE INDEX IF NOT EXISTS idx_plans_premium ON medicare_plans(premium_total);
CREATE INDEX IF NOT EXISTS idx_enrollment_cpsc ON medicare_plan_enrollment(contract_id, pbp_id, plan_year, month);
CREATE INDEX IF NOT EXISTS idx_enrollment_county ON medicare_plan_enrollment(state, county_fips);
CREATE INDEX IF NOT EXISTS idx_benefits_plan ON medicare_plan_benefits(contract_id, pbp_id, plan_year);
CREATE INDEX IF NOT EXISTS idx_star_contract ON medicare_star_ratings(contract_id, plan_year);
CREATE INDEX IF NOT EXISTS idx_formulary_drug ON medicare_drug_formulary(drug_name);
CREATE INDEX IF NOT EXISTS idx_formulary_ndc ON medicare_drug_formulary(ndc);
CREATE INDEX IF NOT EXISTS idx_formulary_plan ON medicare_drug_formulary(contract_id, pbp_id, plan_year);
CREATE INDEX IF NOT EXISTS idx_pharmacy_npi ON medicare_pharmacy_network(npi);
CREATE INDEX IF NOT EXISTS idx_pharmacy_zip ON medicare_pharmacy_network(zip);
CREATE INDEX IF NOT EXISTS idx_chunks_embedding ON document_chunks USING ivfflat (embedding vector_cosine_ops) WITH (lists = 100);
CREATE INDEX IF NOT EXISTS idx_chunks_contract ON document_chunks(contract_id);
CREATE INDEX IF NOT EXISTS idx_chunks_source ON document_chunks(source_type);
CREATE INDEX IF NOT EXISTS idx_chunks_year ON document_chunks(plan_year);
CREATE INDEX IF NOT EXISTS idx_chunks_state ON document_chunks(state);
`;

async function setup() {
  console.log('Setting up Medicare Assistant database...');
  try {
    await pool.query(SQL);
    const tables = await pool.query(`
      SELECT table_name FROM information_schema.tables
      WHERE table_schema = 'public' ORDER BY table_name
    `);
    console.log('Created tables:');
    tables.rows.forEach(r => console.log('  -', r.table_name));
    console.log('Database setup complete.');
  } catch (err) {
    console.error('Setup failed:', err.message);
    process.exit(1);
  } finally {
    await pool.end();
  }
}

setup();
