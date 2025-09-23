-- Choose the first transaction in a given market to understand a market's lifecycle
-- In this case, we're choosing a NegRisk market which consists of mutually exclusive
-- binary markets unlike the CTF exchange which supports single questions with multiple outcomes
WITH target_transaction AS (
  SELECT '0xacdd6ae651a17ad9b206b00dcc4ad58337af0101a7d925fb80c6890eff15dfed' AS transaction_hash
),

-- Get all logs for the specific target transaction
target_transaction_logs AS (
  SELECT 
    name,
    transaction_hash,
    address,
    params,
    log_index,
    block_timestamp,
    block_number
  FROM polygon.decoded.logs
  WHERE transaction_hash = (SELECT transaction_hash FROM target_transaction)
),

-- Broader date-range logs for historical lookups
date_range_logs AS (
  SELECT 
    name,
    transaction_hash,
    address,
    params,
    log_index,
    block_timestamp,
    block_number
  FROM polygon.decoded.logs
  WHERE block_timestamp >= '2025-08-21'
    AND block_timestamp < '2025-08-23'
),

-- Pick one asset
target_asset AS (
  SELECT 
    params:takerAssetId::STRING AS asset_id
  FROM target_transaction_logs
  WHERE name = 'OrderFilled'
    AND params:takerAssetId::STRING <> '0'
  ORDER BY asset_id DESC
  LIMIT 1
),

-- Find the mint transaction
mint_transaction AS (
  SELECT 
    mint.name,
    mint.transaction_hash,
    mint.params,
    mint.log_index,
    mint.block_timestamp,
    ta.asset_id
  FROM date_range_logs AS mint
  CROSS JOIN target_asset AS ta
  WHERE mint.address = '0x4d97dcd97ec945f40cf65f87097ace5ea0476045' -- ConditionalTokens
    AND mint.name IN ('TransferSingle', 'TransferBatch')
    AND mint.params:from::STRING = '0x0000000000000000000000000000000000000000'
    AND (mint.params:id::STRING = ta.asset_id  -- For TransferSingle events: check the single 'id' field
         OR ARRAY_CONTAINS(ta.asset_id::variant, mint.params:ids))  -- For TransferBatch events: check if asset_id appears in 'ids' array
  ORDER BY mint.block_timestamp ASC, mint.log_index ASC
  LIMIT 1
),


-- Find the PositionSplit event in the same transaction
position_split AS (
  SELECT 
    name,
    transaction_hash,
    params,
    log_index,
    block_timestamp,
    -- params,
    params:collateralToken::STRING as collateral_token,
    params:parentCollectionId::STRING as parent_collection_id,
    params:conditionId::STRING as condition_id,
    params:partition as partition_array,
    params:stakeholder as stakeholder
  FROM target_transaction_logs
  WHERE address = '0x4d97dcd97ec945f40cf65f87097ace5ea0476045' -- ConditionalTokens
    AND name = 'PositionSplit'
  ),

-- Get the ConditionPreparation event for this conditionId
condition_preparation AS (
  SELECT
    cp.name,
    cp.transaction_hash,
    cp.params,
    cp.log_index,
    cp.block_timestamp,
    cp.params:conditionId::STRING as condition_id,
    cp.params:oracle::STRING as oracle,
    cp.params:outcomeSlotCount::NUMBER as outcome_slot_count,
    cp.params:questionId::STRING as question_id
  FROM date_range_logs AS cp
  CROSS JOIN position_split ps
  WHERE cp.block_timestamp <= ps.block_timestamp
    AND cp.address = '0x4d97dcd97ec945f40cf65f87097ace5ea0476045'  -- ConditionalTokens
    AND cp.name = 'ConditionPreparation'
    AND cp.params:conditionId::STRING = ps.condition_id -- Link the condition from the split to its original preparation to get oracle & questionId
),

-- Bridge through NegRisk Adapter to get marketId
negrisk_bridge AS (
  SELECT
    l.name,
    l.transaction_hash,
    l.params,
    l.params:marketId::STRING as market_id,
    l.params:questionId::STRING as question_id,
    l.params:index::NUMBER as question_index,
    l.params:data::STRING as raw_param_data,
    TRY_HEX_DECODE_STRING(
      REGEXP_REPLACE(raw_param_data, '^0x', '')
    ) as market_description,
    cp.oracle,
    cp.condition_id,
    l.block_timestamp
  FROM date_range_logs AS l
  CROSS JOIN condition_preparation cp
  WHERE l.address = cp.oracle  -- Use oracle from condition_preparation
    AND l.name = 'QuestionPrepared'
    AND l.params:questionId::STRING = cp.question_id
)


-- -- Determine outcome
-- SELECT question_id
-- FROM condition_preparation

-- Comment out all lines below and uncomment SELECT and FROM lines above
-- Once you run the code above, comment out the SELECT and FROM lines
-- Since the function creating the condition ID uses elliptic curve math unavailable in SQL, we need to do manual checks 
--    (or automatically read it all values with a script)
-- Math behind condition IDs: https://github.com/Polymarket/neg-risk-ctf-adapter/blob/main/src/libraries/CTHelpers.sol

-- Now paste the question_id and (true|false) outcomes to https://polygonscan.com/address/0xd91E80cF2E7be2e162c6513ceD06f1dD0dA35296#readContract#F13
-- Fill in CASE WHEN below

SELECT 
  nb.market_description,
  ta.asset_id,
  CASE 
    WHEN ta.asset_id = '49130265341313597389493962307790881068415757087231190982843443676191664887923' THEN 'Yes' -- corresponds to true outcome - hardcoded based on Polygonscan
    WHEN ta.asset_id = '26326881126711954259545913168354107903979176375708143815611007728704481070965' THEN 'No' -- corresponds to false outcome - hardcoded based on Polygonscan
    ELSE 'Unknown outcome'
  END as outcome,
  nb.market_id,
  nb.question_id,
  cp.oracle,
  cp.condition_id
FROM target_asset ta
CROSS JOIN negrisk_bridge nb
CROSS JOIN condition_preparation cp


/*
GENERALIZABLE MARKET-TO-ASSET DISCOVERY APPROACH:
=================================================

DISCOVERY: Instead of reverse-engineering individual transactions, we can build a comprehensive
asset catalog by starting with all unique markets and mapping forward to their assets.
Note: This approach maps NEGRISK markets. CTF markets would need to be added separately.

THE FORWARD-MAPPING PATTERN (NEGRISK-FOCUSED):
1. MARKET UNIVERSE DISCOVERY: Find all DISTINCT question_id values from ConditionPreparation events
2. MARKET METADATA EXTRACTION: For each question_id, find QuestionPrepared events to get market_id and descriptions
3. ASSET GENERATION: Use Polygonscan contract calls on each question_id to generate both outcome asset_ids (true/false)
4. COMPREHENSIVE MAPPING: Build long-format table where each asset_id gets its own row with full market context

IMPLEMENTATION STRATEGY:
- Query all ConditionPreparation events for question_id list
- For each question_id, make Polygonscan contract calls to get both asset_ids (true/false outcomes)
- Create two rows per market: one for each asset_id with its outcome label
- Each row contains: asset_id, question_id, market_id, market_type, market_description, condition_id, oracle_address, outcome

LONG-FORMAT TABLE STRUCTURE:
| asset_id | question_id | market_id | market_type | market_description | condition_id | oracle_address | outcome |
|----------|-------------|-----------|-------------|-------------------|--------------|----------------|---------|
| 49130... | 0x123...    | mkt_001   | NEGRISK     | "Will X happen?"  | 0xabc...     | 0xd91E80...    | Yes     |
| 26326... | 0x123...    | mkt_001   | NEGRISK     | "Will X happen?"  | 0xabc...     | 0xd91E80...    | No      |
| 55443... | 0x456...    | mkt_002   | NEGRISK     | "Will Y happen?"  | 0xdef...     | 0xd91E80...    | Yes     |
| 77889... | 0x456...    | mkt_002   | NEGRISK     | "Will Y happen?"  | 0xdef...     | 0xd91E80...    | No      |

RESULT: Universal asset_id lookup table - every Polymarket asset instantly resolves to full market context.

SNOWFLAKE SCHEMA DESIGN:
=======================

For a proper normalized approach, design a two-table structure:

1. QUESTIONS DIMENSION TABLE: Contains one row per question_id (PRIMARY KEY), with market_id, 
   market_type, market_description, condition_id, and oracle_address. This eliminates 
   redundancy since these fields repeat for every asset within the same question.

2. ASSETS FACT TABLE: Contains one row per asset_id (PRIMARY KEY), with question_id as 
   FOREIGN KEY reference, plus outcome field. This maintains the granular trading-level data.

The relationship is one-to-many: each question_id can have multiple asset_id values 
(typically 2 for Yes/No outcomes), but each asset_id belongs to exactly one question_id.
*/