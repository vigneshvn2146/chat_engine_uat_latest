"""
Vespa Search Client Module - UPDATED to return YQL query

Handles Vespa search operations with equipment-aware queries and 3-tier fallback strategy:
- Tier 1: Hard filters (make, model, info_type, equipment) + embedding (Hybrid)
- Tier 2: Soft filters (make + info_type mandatory, others flexible) + embedding  
- Tier 3: Only info_type + embedding (remove all equipment filters)

NOW RETURNS: (search_results, yql_query_used) tuple for debugging visibility
"""

import pandas as pd
import logging
from vespa.application import Vespa
from vespa.io import VespaQueryResponse
from vespa.exceptions import VespaError
from typing import Dict, List, Optional, Tuple, Any
from config import config
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError
import requests
from rapidfuzz import fuzz, process
import traceback
import time
import re
import boto3

logger = logging.getLogger(__name__)

class VespaSearchClient:
    """Vespa search client with equipment-aware search and 3-tier fallback"""
    
    def __init__(self):
        """Initialize Vespa client"""
        self.vespa_config = config.get_vespa_config()
        self.search_limit = config.vespa_search_limit
        self.max_context_results = config.max_context_results
        self.search_timeout = config.search_timeout
        
        # Initialize Vespa application
        self._initialize_vespa()

        print("[DEBUG] Vespa endpoint in use:", self.vespa_config['endpoint'])

        self._load_distinct_values()
        
        # Search statistics
        self.stats = {
            "total_searches": 0,
            "tier1_success": 0,
            "tier2_success": 0,
            "tier3_success": 0,
            "tier4_success": 0,  # 🔥 new
            "tier5_success": 0,  # for info-type
            "tier6_success": 0,  # for embedding-only
            "total_failures": 0,
            "average_results": 0
        }

    def _load_distinct_values(self):
        """Fetch and cache distinct values for key fields (make, detail, engine_type)"""
        self.distinct_fields = ["engine_make", "detail", "engine_type"]
        self.distinct_values = {}

        for field in self.distinct_fields:
            try:
                query = f"yql=select {field} from doc where true | all(group({field}) each(output(count())))"
                url = f"{self.vespa_config['endpoint']}/search/?{query}"
                res = requests.get(
                    url,
                    cert=(
                        self.vespa_config['cert_file_path'],
                        self.vespa_config['key_file_path']
                    )
                )
                res.raise_for_status()
                data = res.json()

                print(data)

                # Safe traversal
                root_children = data.get("root", {}).get("children", [])
                if not root_children:
                    print(f"⚠️ No root children found for {field}")
                    self.distinct_values[field] = []
                    continue

                group_level = root_children[0].get("children", [])
                if not group_level:
                    print(f"⚠️ No group level found for {field}")
                    self.distinct_values[field] = []
                    continue

                subgroup = group_level[0].get("children", [])
                if not subgroup:
                    print(f"⚠️ No subgroup found for {field}")
                    self.distinct_values[field] = []
                    continue

                values = [g.get("value", "").strip() for g in subgroup if g.get("value")]
                self.distinct_values[field] = values
                print(f"✅ Loaded {len(values)} distinct values for {field}: {values}...")

            except Exception as e:
                print(f"⚠️ Failed to fetch distinct values for {field}: {e}")
                self.distinct_values[field] = []
    
    def _initialize_vespa(self):
        """Initialize Vespa application connection"""
        try:
            self.vespa_app = Vespa(
                self.vespa_config['endpoint'], 
                cert=self.vespa_config['cert_file_path'], 
                key=self.vespa_config['key_file_path']
            )
            logger.info("✅ Vespa client initialized successfully")
        except Exception as e:
            logger.error(f"❌ Failed to initialize Vespa client: {str(e)}")
            self.vespa_app = None

    # Sanitize common fields to safe lowercase alphanumeric + spaces
    def _sanitize_field(self, val: Optional[str]) -> str:
        if not val:
            return ""
        v = str(val).strip().lower()
        v = re.sub(r'[^a-z0-9\s-]', ' ', v)   # keep alphanumerics, spaces, and hyphen
        v = re.sub(r'\s+', ' ', v).strip()    # collapse multiple spaces
        return v
    
    def _fuzzy_match(self, field: str, user_value: str, threshold: int = 80) -> str:
        """
        Improved fuzzy match:
        1. If user_value exists in distinct list → return it directly.
        2. Otherwise → fuzzy match with threshold.
        3. If no fuzzy match → return original user_value.
        """

        if not user_value or field not in self.distinct_values:
            return user_value

        choices = self.distinct_values.get(field, [])
        print(f"Choices: {choices}")
        if not choices:
            return user_value

        # Normalize for matching
        user_norm = user_value.strip().lower()
        normalized_choices = {c.lower(): c for c in choices}

        # ----------------------------------------------------
        # 1️⃣ Exact match (case-insensitive)
        # ----------------------------------------------------
        if user_norm in normalized_choices:
            exact = normalized_choices[user_norm]
            print(f"✅ Exact match found for '{user_value}' → '{exact}'")
            return exact

        # ----------------------------------------------------
        # 2️⃣ Fuzzy match as fallback
        # ----------------------------------------------------
        best_match = process.extractOne(user_value, choices, scorer=fuzz.token_sort_ratio)

        if best_match and best_match[1] >= threshold:
            print(f"🔍 Fuzzy matched '{user_value}' → '{best_match[0]}' ({best_match[1]}%)")
            return best_match[0]

        # ----------------------------------------------------
        # 3️⃣ No good match → return original
        # ----------------------------------------------------
        print(f"⚠️ No close match for '{user_value}' in {field} (best={best_match})")
        return user_value

    def search_with_equipment_context(self, query_embedding: List[float], rephrased_query: str,
                                    info_type: str, equipment_context: Dict[str, str]) -> Tuple[pd.DataFrame, str]:
        """
        Execute search with equipment context using 3-tier fallback strategy
        
        Args:
            query_embedding: Embedding vector for the query
            rephrased_query: Rephrased query text for logging
            info_type: Query classification (NON_MANUALS or MANUAL)
            equipment_context: Equipment information (make, model, equipment, vessel, problems)
            
        Returns:
            Tuple[pd.DataFrame, str]: (Search results, YQL query used)
        """
        print("[VESPA DEBUG] Sequential search_with_equipment_context triggered")
        self.stats["total_searches"] += 1
        
        if not self.vespa_app:
            logger.error("❌ Vespa client not initialized")
            return pd.DataFrame(), "ERROR: Vespa client not initialized"
        
        logger.info(f"🔍 Starting 3-tier Vespa search for: '{rephrased_query[:100]}...'")
        logger.info(f"🎯 Info type: {info_type}")

        if equipment_context.get('equipment'):
            equipment_context['equipment'] = self._sanitize_field(equipment_context.get('equipment'))
        if equipment_context.get('make'):
            equipment_context['make'] = self._sanitize_field(equipment_context.get('make'))
        if equipment_context.get('model'):
            equipment_context['model'] = self._sanitize_field(equipment_context.get('model'))

        # NEW FIELDS: sanitize make_type, detail, engine_type
        if equipment_context.get('detail'):
            # detail may be more free-form; keep it sanitized similarly to avoid YQL injection
            equipment_context['detail'] = self._sanitize_field(equipment_context.get('detail'))
        if equipment_context.get('engine_type'):
            equipment_context['engine_type'] = self._sanitize_field(equipment_context.get('engine_type'))
        
        # Tier 1: Hard filters (all equipment filters + embedding)
        results, yql_query = self._tier1_hard_filters(query_embedding, info_type, equipment_context, rephrased_query)
        if not results.empty:
            logger.info(f"✅ Tier 1 SUCCESS: Found {len(results)} results with hard filters")
            self.stats["tier1_success"] += 1
            # enrich the results (adds any missing pages)
            try:
                results = self._enrich_with_missing_pages(results, equipment_context, info_type)
            except Exception as e:
                logger.warning(f"⚠️ enrichment failed for tier: {e}")

            return results, f"TIER1: {yql_query}"
        
        logger.info("⚠️ Tier 1 FAILED: No results with hard filters, trying Tier 2...")
        
        # Tier 2: Hard filters (all equipment filters + embedding)
        results, yql_query = self._tier2_hard_filters(query_embedding, info_type, equipment_context, rephrased_query)
        if not results.empty:
            logger.info(f"✅ Tier 2 SUCCESS: Found {len(results)} results with hard filters")
            self.stats["tier2_success"] += 1
            # enrich the results (adds any missing pages)
            try:
                results = self._enrich_with_missing_pages(results, equipment_context, info_type)
            except Exception as e:
                logger.warning(f"⚠️ enrichment failed for tier: {e}")

            return results, f"TIER2: {yql_query}"
        
        logger.info("⚠️ Tier 2 FAILED: No results with hard filters, trying Tier 3...")
        
        # Tier 3: Soft filters (make + info_type mandatory, others flexible)
        results, yql_query = self._tier3_soft_filters(query_embedding, info_type, equipment_context, rephrased_query)
        if not results.empty:
            logger.info(f"✅ Tier 3 SUCCESS: Found {len(results)} results with soft filters")
            self.stats["tier3_success"] += 1
            # enrich the results (adds any missing pages)
            try:
                results = self._enrich_with_missing_pages(results, equipment_context, info_type)
            except Exception as e:
                logger.warning(f"⚠️ enrichment failed for tier: {e}")

            return results, f"TIER3: {yql_query}"
        
        logger.info("⚠️ Tier 3 FAILED: No results with soft filters, trying Tier 4...")

        # Tier 4: Minimal filters (make + info_type mandatory, others flexible)
        results, yql_query = self._tier4_minimal_filters(query_embedding, info_type, equipment_context, rephrased_query)
        if not results.empty:
            logger.info(f"✅ Tier 4 SUCCESS: Found {len(results)} results with minimal filters")
            self.stats["tier4_success"] += 1
            # enrich the results (adds any missing pages)
            try:
                results = self._enrich_with_missing_pages(results, equipment_context, info_type)
            except Exception as e:
                logger.warning(f"⚠️ enrichment failed for tier: {e}")

            return results, f"TIER4: {yql_query}"

        # Tier 5: Basic Filter
        results, yql_query = self._tier5_basic_search(query_embedding, info_type, rephrased_query)
        if not results.empty:
            logger.info(f"✅ Tier 5 SUCCESS: Found {len(results)} results with basic filters")
            # enrich the results (adds any missing pages)
            try:
                results = self._enrich_with_missing_pages(results, equipment_context, info_type)
            except Exception as e:
                logger.warning(f"⚠️ enrichment failed for tier: {e}")

            return results, f"TIER5: {yql_query}"
        
        logger.info("⚠️ Tier 5 FAILED: No results with info_type-only, trying Tier 6 (embedding-only)...")

        # Tier 6: Embedding only
        results, yql_query = self._tier6_embedding_only(query_embedding, rephrased_query)
        if not results.empty:
            logger.info(f"✅ Tier 6 SUCCESS: Found {len(results)} results (embedding-only)")
            self.stats["tier6_success"] += 1
            # enrich the results (adds any missing pages)
            try:
                results = self._enrich_with_missing_pages(results, equipment_context, info_type)
            except Exception as e:
                logger.warning(f"⚠️ enrichment failed for tier: {e}")

            return results, f"TIER6: {yql_query}"

        logger.info("❌ ALL TIERS FAILED: No results found")
        self.stats["total_failures"] += 1
        
        # Return the last attempted query for debugging
        return pd.DataFrame(), f"ALL_TIERS_FAILED - Last attempted: {yql_query}"

    def search_with_vespa_retrieval(
        self,
        query_embedding,
        rephrased_query,
        info_type,
        equipment_context,
        timeout=12.0
    ):
        """
        Helper wrapper for ChatEngine.

        - If MANUAL → call search_tiers_parallel once.
        - If NON_MANUALS → run two-stage retrieval:
            1. FMECA docs (NON_MANUALS)
            2. Manual docs (MANUAL)
            3. Combine results
        """

        # -------------------------------
        # CASE 1: MANUAL → unchanged path
        # -------------------------------
        if info_type.upper() == "MANUAL":
            return self.search_tiers_parallel(
                query_embedding,
                rephrased_query,
                "MANUAL",
                equipment_context,
                timeout
            )

        # ------------------------------------------
        # CASE 2: NON-MANUALS → TWO-STAGE RETRIEVAL
        # ------------------------------------------

        # Stage 1: Get FMECA docs
        fmeca_results, fmeca_yql = self.search_fmeca_parallel(
            query_embedding,
            rephrased_query,
            equipment_context,
            timeout
        )

        # Stage 2: Get MANUAL docs for same equipment
        manual_results, manual_yql = self.search_tiers_parallel(
            query_embedding,
            rephrased_query,
            "manual",
            equipment_context,
            timeout
        )

        if fmeca_results.empty and manual_results.empty:
            combined = pd.DataFrame()
        elif fmeca_results.empty:
            combined = manual_results
        elif manual_results.empty:
            combined = fmeca_results
        else:
            combined = pd.concat([fmeca_results, manual_results], ignore_index=True)

        return combined, f"FMECA:{fmeca_yql} | MANUAL:{manual_yql}"

    def search_fmeca_parallel(
        self,
        query_embedding: List[float],
        rephrased_query: str,
        equipment_context: Dict[str, str],
        timeout: float = 8.0
    ):
        # ------- Build NEW FMECA-specific YQLs (Tier A + Tier B) -------
        yql_fmeca_1 = self._build_fmeca_tier1_yql(rephrased_query, equipment_context)
        yql_fmeca_2 = self._build_fmeca_tier2_yql(rephrased_query, equipment_context)

        fmeca_yqls = {
            "FMECA_TIER1": yql_fmeca_1,
            "FMECA_TIER2": yql_fmeca_2,
        }

        # ------- Same parallel execution logic as other tiers -------
        def run_safe(name, yql):
            try:
                with self.vespa_app.syncio() as session:
                    print(f"\n[DEBUG] Running FMECA tier: {name}")
                    print(f"YQL → {yql}")   
                    resp = session.query(
                        yql=yql,
                        ranking="fusion-rerank",
                        timeout=self.search_timeout,
                        body={
                            "input.query(q1024)": query_embedding,
                            "query": rephrased_query,
                            "ranking.profile": "fusion-rerank"
                        }
                    )
                    if hasattr(resp, "hits") and resp.hits:
                        df = self._convert_response_to_dataframe(resp)
                        print(f"[DEBUG] DataFrame rows from {name}: {len(df)}")
                        return df, f"{name}:{yql}"
                return pd.DataFrame(), f"{name}:{yql}"
            except:
                return pd.DataFrame(), f"{name}:{yql}"

        # ------- Run both tiers in parallel -------
        with ThreadPoolExecutor(max_workers=2) as ex:
            futs = {ex.submit(run_safe, k, v): k for k, v in fmeca_yqls.items()}
            for f in as_completed(futs, timeout=timeout):
                df, yql_used = f.result()
                if not df.empty:
                    return df, yql_used

        return pd.DataFrame(), "FMECA_FAIL"

    def _build_fmeca_tier1_yql(self, rephrased_query: str, equipment_context: Dict[str, str]) -> str:

        equipment = equipment_context.get('equipment', "")
        make = self._fuzzy_match("engine_make", equipment_context.get("make", ""))
        engine_type = equipment_context.get("engine_type", "")
        
        return f'''select id, page_title, topic, equipment, page_number, person, vessel,
            engine_model, engine_make, make_type, detail, engine_type, 
            source_url, url, info_type, document_type, text from doc
            where info_type contains "fmeca" and engine_make contains "{make}" and engine_type contains "{engine_type}" and text contains "{equipment}" and 
            rank({{targetHits:100, approximate:true}}nearestNeighbor(embedding, q1024)) 
            limit {self.search_limit}'''
    
    def _build_fmeca_tier2_yql(self, rephrased_query: str, equipment_context: Dict[str, str]) -> str:

        equipment = equipment_context.get('equipment', "")
        make = self._fuzzy_match("engine_make", equipment_context.get("make", ""))
        engine_type = equipment_context.get("engine_type", "")
        
        return f'''select id, page_title, topic, equipment, page_number, person, vessel,
            engine_model, engine_make, make_type, detail, engine_type, 
            source_url, url, info_type, document_type, text from doc
            where info_type contains "fmeca" and engine_make contains "{make}" and engine_type contains "{engine_type}" and text contains "{equipment}" and
            rank({{targetHits:100, approximate:true}}nearestNeighbor(embedding, q1024)) 
            limit {self.search_limit}'''
    
    def search_tiers_parallel(
        self,
        query_embedding: List[float],
        rephrased_query: str,
        info_type: str,
        equipment_context: Dict[str, str],
        timeout: float = 12.0,
    ) -> Tuple[pd.DataFrame, str]:
        """
        Stage-based parallelism for 6-tier hybrid Vespa search:
        - Stage 1: Run Tier1 + Tier2 in parallel (very strict). Prefer Tier1 if both succeed.
        - Stage 2: If Stage1 fails, run Tier3 + Tier4 in parallel (strict).
        - Stage 3: If Stage2 fails, run Tier5 + Tier6 in parallel (lenient).
        - Final fallback: sequential search for robustness.
        """
        
        if not self.vespa_app:
            logger.error("❌ Vespa client not initialized")
            return pd.DataFrame(), "ERROR: Vespa client not initialized"
        
        if info_type.lower() in ["manual", "procedure", "specification", "non_manuals"]:
            ranking_profile = "fusion-rerank"   # reranked hybrid precision
        else:
            ranking_profile = "semantic-rerank" # reranked semantic balance

        # --- Helper to safely run any YQL tier ---
        def run_tier_safe(tier_name: str, yql: str):
            """Executes a Vespa tier safely and always returns (DataFrame, YQL)."""
            try:
                with self.vespa_app.syncio() as session:
                    response = session.query(
                        yql=yql,                   # only structural filters
                        ranking=ranking_profile,
                        timeout=self.search_timeout,
                        body={
                            "input.query(q1024)": query_embedding,   # semantic vector
                            "query": rephrased_query,                # text for BM25
                            "ranking.profile": ranking_profile,
                            "presentation.timing": "true",
                        },
                    )
                    if hasattr(response, "hits") and response.hits:
                        df = self._convert_response_to_dataframe(response)
                        df_pages = df
                        page_nums = df_pages["page_number"].tolist() if "page_number" in df else []
                        print(f"Tiers Page Numbers: {page_nums}")
                        #ranked = self._apply_equipment_ranking(df, equipment_context)
                        return df.copy(), f"{tier_name}: {yql}"
                    return pd.DataFrame(), f"{tier_name}: {yql}"
            except Exception as e:
                tb = traceback.format_exc()
                logger.warning(f"⚠️ {tier_name} search error: {e}\n{tb}")
                return pd.DataFrame(), f"ERROR:{tier_name}: {yql}"

        # ======================================================
        # 🧱 Stage 1: Very Strict — Tier1 + Tier2
        # ======================================================
        stage1_yqls = {
            "TIER1": self._build_tier1_yql(info_type, equipment_context, rephrased_query),
            "TIER2": self._build_tier2_yql(info_type, equipment_context, rephrased_query),
        }

        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = {executor.submit(run_tier_safe, t, y): t for t, y in stage1_yqls.items()}
            for future in as_completed(futures, timeout=timeout / 3):
                tier_name = futures[future]
                results, yql_used = future.result()
                if not results.empty:
                    logger.info(f"✅ Stage1 SUCCESS: {tier_name} returned {len(results)} results")
                    print(f"[VESPA DEBUG] 🎯 {tier_name} SUCCESS ({len(results)} results)")
                    # --- enrich missing pages (cluster-aware) before ranking/returning ---
                    try:
                        results = self._enrich_with_missing_pages(results, equipment_context, info_type)
                    except Exception as e:
                        logger.warning(f"⚠️ enrichment failed: {e}")
                    # return top-k (keep same behavior as sequential path)
                    return results, yql_used

        # ======================================================
        # ⚙️ Stage 2: Strict — Tier3 + Tier4 (soft + minimal)
        # ======================================================
        stage2_yqls = {
            "TIER3": self._build_tier3_yql(info_type, equipment_context, rephrased_query),
            "TIER4": self._build_tier4_yql(info_type, equipment_context, rephrased_query),
        }

        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = {executor.submit(run_tier_safe, t, y): t for t, y in stage2_yqls.items()}
            for future in as_completed(futures, timeout=timeout / 3):
                tier_name = futures[future]
                results, yql_used = future.result()
                if not results.empty:
                    logger.info(f"✅ Stage2 SUCCESS: {tier_name} returned {len(results)} results")
                    print(f"[VESPA DEBUG] 🎯 {tier_name} SUCCESS ({len(results)} results)")
                    # --- enrich missing pages (cluster-aware) before ranking/returning ---
                    try:
                        results = self._enrich_with_missing_pages(results, equipment_context, info_type)
                    except Exception as e:
                        logger.warning(f"⚠️ enrichment failed: {e}")
                    # return top-k (keep same behavior as sequential path)
                    return results, yql_used

        # ======================================================
        # 🧭 Stage 3: Lenient — Tier5 + Tier6 (info_type + embedding-only)
        # ======================================================
        stage3_yqls = {
            "TIER5": self._build_tier5_yql(info_type),
            "TIER6": self._build_tier6_yql(),
        }

        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = {executor.submit(run_tier_safe, t, y): t for t, y in stage3_yqls.items()}
            for future in as_completed(futures, timeout=timeout / 3):
                tier_name = futures[future]
                results, yql_used = future.result()
                if not results.empty:
                    logger.info(f"✅ Stage3 SUCCESS: {tier_name} returned {len(results)} results")
                    print(f"[VESPA DEBUG] 🎯 {tier_name} SUCCESS ({len(results)} results)")
                    # --- enrich missing pages (cluster-aware) before ranking/returning ---
                    try:
                        results = self._enrich_with_missing_pages(results, equipment_context, info_type)
                    except Exception as e:
                        logger.warning(f"⚠️ enrichment failed: {e}")
                    # return top-k (keep same behavior as sequential path)
                    return results, yql_used

        # ======================================================
        # 🧩 Final Fallback: Sequential Search (guaranteed return)
        # ======================================================
        logger.warning("❌ All parallel stages failed — Falling back to sequential search flow.")
        print("[VESPA DEBUG] ❌ Stage1, Stage2 & Stage3 FAILED → Falling back sequentially")
        return self.search_with_equipment_context(query_embedding, rephrased_query, info_type, equipment_context)

    def _tier1_hard_filters(self, query_embedding: List[float], info_type: str, 
                           equipment_context: Dict[str, str], rephrased_query:str) -> Tuple[pd.DataFrame, str]:
        """
        Tier 1: Hard filters - All equipment information + embedding (Hybrid)
        
        Returns:
            Tuple[pd.DataFrame, str]: (Results, YQL query used)
        """
        logger.info("🔧 Tier 1: Applying hard filters (make, model, info_type, equipment)")
        
        try:
            yql = self._build_tier1_yql(info_type, equipment_context)

            # Dynamically choose profile
            if info_type.lower() in ["manual", "procedure", "specification"]:
                ranking_profile = "fusion-rerank"   # reranked hybrid precision
            else:
                ranking_profile = "semantic-rerank" # reranked semantic balance

            with self.vespa_app.syncio() as session:
                response = session.query(
                    yql=yql,                   # only structural filters
                    ranking= ranking_profile,
                    timeout=self.search_timeout,
                    body={
                        "input.query(q1024)": query_embedding,   # semantic vector
                        "query": rephrased_query,                # text for BM25
                        "ranking.profile": ranking_profile,
                        "presentation.timing": "true",
                    },
                )
                                
                if hasattr(response, "hits") and response.hits:
                    logger.info(f"🎯 Tier 1: Found {len(response.hits)} hits with hard filters")
                    return self._convert_response_to_dataframe(response), yql
                else:
                    logger.info("❌ Tier 1: No hits with hard filters")
                    return pd.DataFrame(), yql
                    
        except Exception as e:
            logger.error(f"❌ Tier 1 search failed: {str(e)}")
            # Return the query even if it failed for debugging
            yql = self._build_tier1_yql(info_type, equipment_context)
            return pd.DataFrame(), f"ERROR: {yql}"
        
    def _tier2_hard_filters(self, query_embedding: List[float], info_type: str,
                        equipment_context: Dict[str, str], rephrased_query:str) -> Tuple[pd.DataFrame, str]:
        """
        Tier 2: Hard filters - Focused set (make, model, equipment, detail, engine_type)
        """
        logger.info("🔧 Tier 2: Applying hard filters (make, model, equipment, detail, engine_type)")

        try:
            yql = self._build_tier2_yql(info_type, equipment_context)

            # Dynamically choose profile
            if info_type.lower() in ["manual", "procedure", "specification"]:
                ranking_profile = "fusion-rerank"   # reranked hybrid precision
            else:
                ranking_profile = "semantic-rerank" # reranked semantic balance

            with self.vespa_app.syncio() as session:
                response = session.query(
                    yql=yql,                   # only structural filters
                    ranking= ranking_profile,
                    timeout=self.search_timeout,
                    body={
                        "input.query(q1024)": query_embedding,   # semantic vector
                        "query": rephrased_query,                # text for BM25
                        "ranking.profile": ranking_profile,
                        "presentation.timing": "true",
                    },
                )

                if hasattr(response, "hits") and response.hits:
                    logger.info(f"🎯 Tier 2: Found {len(response.hits)} hits with focused hard filters")
                    return self._convert_response_to_dataframe(response), yql
                else:
                    logger.info("❌ Tier 2: No hits with focused hard filters")
                    return pd.DataFrame(), yql

        except Exception as e:
            logger.error(f"❌ Tier 2 search failed: {str(e)}")
            yql = self._build_tier2_yql(info_type, equipment_context)
            return pd.DataFrame(), f"ERROR: {yql}"

    
    def _tier3_soft_filters(self, query_embedding: List[float], info_type: str,
                           equipment_context: Dict[str, str], rephrased_query:str) -> Tuple[pd.DataFrame, str]:
        """
        Tier 3: Soft filters - make + info_type mandatory, others flexible + embedding
        
        Returns:
            Tuple[pd.DataFrame, str]: (Results, YQL query used)
        """
        logger.info("🔧 Tier 3: Applying soft filters (make + info_type mandatory)")
        
        try:
            yql = self._build_tier3_yql(info_type, equipment_context)

            # Dynamically choose profile
            if info_type.lower() in ["manual", "procedure", "specification"]:
                ranking_profile = "fusion-rerank"   # reranked hybrid precision
            else:
                ranking_profile = "semantic-rerank" # reranked semantic balance
            
            with self.vespa_app.syncio() as session:
                response = session.query(
                    yql=yql,                   # only structural filters
                    ranking=ranking_profile,
                    timeout=self.search_timeout,
                    body={
                        "input.query(q1024)": query_embedding,   # semantic vector
                        "query": rephrased_query,                # text for BM25
                        "ranking.profile": ranking_profile,
                        "presentation.timing": "true",
                    },
                )
                
                if hasattr(response, "hits") and response.hits:
                    logger.info(f"🎯 Tier 3: Found {len(response.hits)} hits with soft filters")
                    return self._convert_response_to_dataframe(response), yql
                else:
                    logger.info("❌ Tier 3: No hits with soft filters")
                    return pd.DataFrame(), yql
                    
        except Exception as e:
            logger.error(f"❌ Tier 3 search failed: {str(e)}")
            # Return the query even if it failed for debugging
            yql = self._build_tier3_yql(info_type, equipment_context)
            return pd.DataFrame(), f"ERROR: {yql}"
    
    def _tier4_minimal_filters(self, query_embedding: List[float], info_type: str,
                           equipment_context: Dict[str, str], rephrased_query:str) -> Tuple[pd.DataFrame, str]:
        """
        Tier 4: Minimal filters — make/model only (bridge tier between soft and info-type-only)
        """
        logger.info("🔧 Tier 4: Applying minimal filters (make/model only)")
        try:
            yql = self._build_tier4_yql(info_type, equipment_context)

            if info_type.lower() in ["manual", "procedure", "specification"]:
                ranking_profile = "fusion-rerank"   # reranked hybrid precision
            else:
                ranking_profile = "semantic-rerank" # reranked semantic balance

            with self.vespa_app.syncio() as session:
                response = session.query(
                    yql=yql,                   # only structural filters
                    ranking=ranking_profile,
                    timeout=self.search_timeout,
                    body={
                        "input.query(q1024)": query_embedding,   # semantic vector
                        "query": rephrased_query,                # text for BM25
                        "ranking.profile": ranking_profile,
                        "presentation.timing": "true",
                    },
                )

            if hasattr(response, "hits") and response.hits:
                logger.info(f"🎯 Tier 4: Found {len(response.hits)} hits with minimal filters")
                df = self._convert_response_to_dataframe(response)
                ranked_results = self._apply_equipment_ranking(df, equipment_context)
                return ranked_results, yql
            else:
                logger.info("❌ Tier 4: No hits with minimal filters")
                return pd.DataFrame(), yql

        except Exception as e:
            logger.error(f"❌ Tier 4 minimal search failed: {e}\n{traceback.format_exc()}")
            yql = self._build_tier4_yql(info_type, equipment_context)
            return pd.DataFrame(), f"ERROR: {yql}"

    def _tier5_basic_search(self, query_embedding: List[float], info_type: str, rephrased_query:str) -> Tuple[pd.DataFrame, str]:
        """
        Tier 5: Basic search - Only info_type + embedding (remove all equipment filters)
        
        Returns:
            Tuple[pd.DataFrame, str]: (Results, YQL query used)
        """
        logger.info("🔧 Tier 5: Applying basic search (info_type + embedding only)")
        
        try:
            yql = self._build_tier5_yql(info_type)

            if info_type.lower() in ["manual", "procedure", "specification"]:
                ranking_profile = "fusion-rerank"   # reranked hybrid precision
            else:
                ranking_profile = "semantic-rerank" # reranked semantic balance
            
            with self.vespa_app.syncio() as session:
                response = session.query(
                    yql=yql,                   # only structural filters
                    ranking=ranking_profile,
                    timeout=self.search_timeout,
                    body={
                        "input.query(q1024)": query_embedding,   # semantic vector
                        "query": rephrased_query,                # text for BM25
                        "ranking.profile": ranking_profile,
                        "presentation.timing": "true",
                    },
                )
                
                if hasattr(response, "hits") and response.hits:
                    logger.info(f"🎯 Tier 5: Found {len(response.hits)} hits with basic search")
                    return self._convert_response_to_dataframe(response), yql
                else:
                    logger.info("❌ Tier 5: No hits with basic search")
                    return pd.DataFrame(), yql
                    
        except Exception as e:
            logger.error(f"❌ Tier 5 search failed: {str(e)}")
            # Return the query even if it failed for debugging
            yql = self._build_tier5_yql(info_type)
            return pd.DataFrame(), f"ERROR: {yql}"
        
    def _tier6_embedding_only(self, query_embedding: List[float], rephrased_query:str) -> Tuple[pd.DataFrame, str]:
        """
        Tier 6: Pure embedding-only search — final fallback with no filters.
        Searches the entire Vespa corpus based purely on vector similarity.
        """
        logger.info("🔧 Tier 6: Applying pure embedding-only search (no filters)")
        try:
            yql = self._build_tier6_yql()

            with self.vespa_app.syncio() as session:
                response = session.query(
                    yql=yql,                   # only structural filters
                    ranking="fusion-rerank",
                    timeout=self.search_timeout,
                    body={
                        "input.query(q1024)": query_embedding,   # semantic vector
                        "query": rephrased_query,                # text for BM25
                        "ranking.profile": "fusion-rerank",
                        "presentation.timing": "true",
                    },
                )

            if hasattr(response, "hits") and response.hits:
                logger.info(f"🎯 Tier 6: Found {len(response.hits)} hits (embedding-only search)")
                df = self._convert_response_to_dataframe(response)
                return df, yql
            else:
                logger.info("❌ Tier 6: No hits found (embedding-only search)")
                return pd.DataFrame(), yql

        except Exception as e:
            logger.error(f"❌ Tier 6 embedding-only search failed: {e}\n{traceback.format_exc()}")
            yql = self._build_tier6_yql()
            return pd.DataFrame(), f"ERROR: {yql}"
    
    def _build_tier1_yql(self, info_type: str, equipment_context: Dict[str, str], rephrased_query: str) -> str:
        
        """Build YQL query for Tier 1 (hard filters)"""
        base_yql = f'''select id, page_title, topic, equipment, page_number, person, vessel,
            engine_model, engine_make, make_type, detail, engine_type, 
            source_url, url, info_type, document_type, text from doc
            where info_type contains "{info_type.lower()}"'''

        equipment_filters = []

        if equipment_context.get('equipment'):
            equipment_filters.append(f'page_title contains "{equipment_context["equipment"]}"')

        if equipment_context.get('make'):
            make = self._fuzzy_match('engine_make', equipment_context['make'])
            equipment_filters.append(f'engine_make contains "{make}"')

        if equipment_context.get('detail'):
            detail = self._fuzzy_match('detail', equipment_context['detail'])
            equipment_filters.append(f'detail contains "{detail}"')

        if equipment_context.get('engine_type'):
            engine_type = equipment_context.get("engine_type", "")
            equipment_filters.append(f'engine_type contains "{engine_type}"')

        if equipment_filters:
            filter_string = " and ".join(equipment_filters)
            final_yql = f"""{base_yql} and ({filter_string}) and 
            rank({{targetHits:100, approximate:true}}nearestNeighbor(embedding, q1024)) 
            limit {self.search_limit}"""
        else:
            final_yql = f"""{base_yql} and 
            rank({{targetHits:100, approximate:true}}nearestNeighbor(embedding, q1024)) 
            limit {self.search_limit}"""

        logger.debug(f"🔧 Tier 1 YQL: {final_yql}")
        return final_yql
    
    def _build_tier2_yql(self, info_type: str, equipment_context: Dict[str, str], rephrased_query: str) -> str:
        
        """Build YQL query for Tier 2 (hard filters on make, model, equipment, detail, engine_type)"""
        base_yql = f'''select id, page_title, topic, equipment, page_number, person, vessel,
            engine_model, engine_make, make_type, detail, engine_type,
            source_url, url, info_type, document_type, text from doc
            where info_type contains "{info_type.lower()}"'''

        equipment_filters = []

        if equipment_context.get('equipment'):
            equipment = equipment_context['equipment']
            equipment_filters.append(f'text contains "{equipment}"')

        if equipment_context.get('make'):
            make = self._fuzzy_match('engine_make', equipment_context['make'])
            equipment_filters.append(f'engine_make contains "{make}"')

        if equipment_context.get('detail'):
            detail = self._fuzzy_match('detail', equipment_context['detail'])
            equipment_filters.append(f'detail contains "{detail}"')

        if equipment_context.get('engine_type'):
            engine_type = equipment_context.get("engine_type", "")
            equipment_filters.append(f'engine_type contains "{engine_type}"')

        if equipment_filters:
            filter_string = " and ".join(equipment_filters)
            final_yql = f"""{base_yql} and ({filter_string}) and 
            rank({{targetHits:100, approximate:true}}nearestNeighbor(embedding, q1024)) 
            limit {self.search_limit}"""
        else:
            final_yql = f"""{base_yql} and 
            rank({{targetHits:100, approximate:true}}nearestNeighbor(embedding, q1024)) 
            limit {self.search_limit}"""

        logger.debug(f"🔧 Tier 2 YQL: {final_yql}")
        return final_yql
    
    def _build_tier3_yql(self, info_type: str, equipment_context: Dict[str, str], rephrased_query: str) -> str:
        
        """Build YQL query for Tier 3 (soft filters - make + info_type mandatory)"""
        base_yql = f'''select id, page_title, topic, equipment, page_number, person, vessel,
            engine_model, engine_make, make_type, detail, engine_type,
            source_url, url, info_type, document_type, text from doc
            where info_type contains "{info_type.lower()}"'''

        # Mandatory filters
        mandatory_filters = []
        if equipment_context.get('make'):
            make = self._fuzzy_match('engine_make', equipment_context['make'])
            mandatory_filters.append(f'engine_make contains "{make}"')

        if equipment_context.get('engine_type'):
            engine_type = equipment_context.get("engine_type", "")
            mandatory_filters.append(f'engine_type contains "{engine_type}"')

        if equipment_context.get('detail'):
            detail = self._fuzzy_match('detail', equipment_context['detail'])
            mandatory_filters.append(f'detail contains "{detail}"')

        # Flexible: model, equipment, detail (OR)
        flexible_filters = []
        if equipment_context.get('equipment'):
            equipment_type = equipment_context['equipment']
            flexible_filters.append(f'page_title contains "{equipment_type}"')

        # Build final query
        if mandatory_filters and flexible_filters:
            filter_string = "(" + " and ".join(mandatory_filters) + ") and (" + " or ".join(flexible_filters) + ")"
            final_yql = f"""{base_yql} and ({filter_string}) and 
            rank({{targetHits:100, approximate:true}}nearestNeighbor(embedding, q1024)) 
            limit {self.search_limit}"""
        elif mandatory_filters:
            filter_string = " and ".join(mandatory_filters)
            final_yql = f"""{base_yql} and ({filter_string}) and 
            rank({{targetHits:100, approximate:true}}nearestNeighbor(embedding, q1024)) 
            limit {self.search_limit}"""
        else:
            final_yql = f"""{base_yql} and 
            rank({{targetHits:100, approximate:true}}nearestNeighbor(embedding, q1024)) 
            limit {self.search_limit}"""

        logger.debug(f"🔧 Tier 3 YQL: {final_yql}")
        return final_yql
    
    def _build_tier4_yql(self, info_type: str, equipment_context: Dict[str, str], rephrased_query: str) -> str:
        """
        Tier 4 (New): Minimal filters — make or model only (optional info_type).
        Looser than soft filters but still context-aware.
        """
        base_yql = f'''select id, page_title, topic, equipment, page_number, person, vessel,
            engine_model, engine_make, make_type, detail, engine_type,
            source_url, url, info_type, document_type, text from doc
            where info_type contains "{info_type.lower()}"'''

        filters = []

        make = self._fuzzy_match('engine_make', equipment_context['make'])
        model = equipment_context.get('model')
        detail = self._fuzzy_match('detail', equipment_context['detail'])

        # Only include make/model filters if available
        if make and model:
            filters.append(f'(engine_make contains "{make}" and detail contains "{detail}")')
        elif make:
            filters.append(f'engine_make contains "{make}"')

        if filters:
            filter_string = " and ".join(filters)
            final_yql = f"""{base_yql} and ({filter_string}) and 
            rank({{targetHits:100, approximate:true}}nearestNeighbor(embedding, q1024)) 
            limit {self.search_limit}"""
        else:
            # If no make/model found, just fallback to info_type + embedding
            final_yql = f"""{base_yql} and 
            rank({{targetHits:100, approximate:true}}nearestNeighbor(embedding, q1024)) 
            limit {self.search_limit}"""

        logger.debug(f"🔧 Tier 4 (minimal) YQL: {final_yql}")
        return final_yql

    def _build_tier5_yql(self, info_type: str) -> str:
        """Build YQL query for Tier 5"""
        final_yql = f'''select id, page_title, topic, equipment, page_number, person, vessel,
            engine_model, engine_make, make_type, detail, engine_type,
            source_url, url, info_type, document_type, text from doc where
            rank({{targetHits:100, approximate:true}}nearestNeighbor(embedding, q1024))  
            limit {self.search_limit}'''
        
        logger.debug(f"🔧 Tier 5 YQL: {final_yql}")
        return final_yql
    
    def _build_tier6_yql(self) -> str:
        """
        Tier 6: Pure embedding-only fallback (no filters, full-corpus semantic search)
        """
        final_yql = f'''select id, page_title, topic, equipment, page_number, person, vessel,
            engine_model, engine_make, make_type, detail, engine_type,
            source_url, url, info_type, document_type, text from doc where
            rank({{targetHits:100, approximate:true}}nearestNeighbor(embedding, q1024))  
            limit {self.search_limit}'''

        logger.debug(f"🔧 Tier 6 YQL (embedding-only): {final_yql}")
        return final_yql
    
    def _convert_response_to_dataframe(self, response: VespaQueryResponse) -> pd.DataFrame:
        """Convert Vespa response to pandas DataFrame"""
        try:
            records = []
            fields = [
                "page_title", "topic", "equipment", "page_number", "person", "vessel",
                "engine_model", "engine_make", "source_url", "url",
                "make_type", "detail", "engine_type", "info_type",
                "document_type", "text"
            ]
            
            for hit in response.hits:
                record = {field: hit["fields"].get(field, None) for field in fields}
                matchfeatures = hit["fields"].get("matchfeatures", {})
                record["bm25_score"] = matchfeatures.get("bm25sum", None)
                record["cos_score"] = matchfeatures.get("cos_sim", None)
                record["relevance"] = hit.get("relevance", None)
                records.append(record)
                
            return pd.DataFrame(records)
        
        except Exception as e:
            logger.error(f"❌ convert_response_to_dataframe error: {e}\n{traceback.format_exc()}")
            return pd.DataFrame()
    
    def _apply_equipment_ranking(self, df: pd.DataFrame, 
                               equipment_context: Dict[str, str]) -> pd.DataFrame:
        """Apply equipment-aware ranking boost"""
        if df.empty:
            return df
        
        df = df.copy()  # Avoid modifying original dataframe
        df['equipment_score'] = 0.0
        
        make = equipment_context.get('make', '').lower()
        model = equipment_context.get('model', '').lower()
        equipment_type = equipment_context.get('equipment', '').lower()
        
        # Apply scoring boosts
        if equipment_type:
            equipment_match = df['equipment'].str.lower().str.contains(equipment_type, na=False)
            df.loc[equipment_match, 'equipment_score'] += 0.4
            logger.debug(f"🔧 Equipment type boost applied: {equipment_type}")
        
        if make:
            make_match = (df['equipment'].str.lower().str.contains(make, na=False) |
                         df['engine_make'].str.lower().str.contains(make, na=False))
            df.loc[make_match, 'equipment_score'] += 0.3
            logger.debug(f"🏭 Make boost applied: {make}")
        
        if model:
            model_match = (df['equipment'].str.lower().str.contains(model, na=False) |
                          df['engine_model'].str.lower().str.contains(model, na=False))
            df.loc[model_match, 'equipment_score'] += 0.5
            logger.debug(f"🔧 Model boost applied: {model}")

        if equipment_context.get('make_type'):
            mt = equipment_context.get('make_type').lower()
            # match against both make_type field and engine_make/document_type as fallback
            mt_match = (
                df.get('make_type', pd.Series('', index=df.index)).astype(str).str.lower().str.contains(mt, na=False) |
                df.get('engine_make', pd.Series('', index=df.index)).astype(str).str.lower().str.contains(mt, na=False) |
                df.get('document_type', pd.Series('', index=df.index)).astype(str).str.lower().str.contains(mt, na=False)
            )
            df.loc[mt_match, 'equipment_score'] += 0.2
            logger.debug(f"🏭 make_type boost applied: {mt}")

        if equipment_context.get('detail'):
            dt = equipment_context.get('detail').lower()
            dt_match = df.get('detail', pd.Series('', index=df.index)).astype(str).str.lower().str.contains(dt, na=False)
            df.loc[dt_match, 'equipment_score'] += 0.1
            logger.debug(f"🔎 Detail boost applied: {dt}")

        if equipment_context.get('engine_type'):
            et = equipment_context.get('engine_type').lower()
            et_match = df.get('engine_type', pd.Series('', index=df.index)).astype(str).str.lower().str.contains(et, na=False)
            df.loc[et_match, 'equipment_score'] += 0.2
            logger.debug(f"⚙️ engine_type boost applied: {et}")
        
        # Combine scores and sort
        df['enhanced_relevance'] = df['relevance'] + df['equipment_score']
        df = df.sort_values('enhanced_relevance', ascending=False)
        
        # Update stats
        self.stats["average_results"] = (self.stats["average_results"] + len(df)) / max(1, self.stats["total_searches"])
        
        top_score = df['enhanced_relevance'].max() if not df.empty else 0
        logger.info(f"🏆 Equipment ranking completed, top score: {top_score:.3f}")
        
        # Return limited results for context
        try:
            # existing ranking logic
            return df
        except Exception as e:
            logger.error(f"❌ _apply_equipment_ranking error: {e}\n{traceback.format_exc()}")
            return pd.DataFrame()

    def _fill_missing_page_numbers(self, page_list: List[str], gap_threshold: int = 5) -> List[str]:
        """
        Given a list of page numbers (strings), detect local clusters and fill missing pages
        within each cluster. Returns a sorted list of filled page numbers as strings.
        """
        try:
            nums = sorted(set(int(p) for p in page_list if str(p).isdigit()))
        except Exception:
            return []

        if not nums:
            return []

        clusters = []
        current = [nums[0]]
        for n in nums[1:]:
            if n - current[-1] <= gap_threshold:
                current.append(n)
            else:
                clusters.append(current)
                current = [n]
        clusters.append(current)

        filled = []
        for c in clusters:
            filled.extend(range(min(c), max(c) + 1))

        return [str(x) for x in sorted(set(filled))]

    def _fetch_missing_pages_parallel(
        self,
        missing_pages: List[str],
        equipment_context: Dict[str, str],
        info_type: str,
        max_workers: int = 6,
    ) -> pd.DataFrame:
        """
        Fetch missing pages in parallel by issuing one Vespa query per page_number.
        Returns combined DataFrame (may be empty).
        """
        if not missing_pages:
            return pd.DataFrame()

        # limit workers reasonably
        workers = min(max_workers, len(missing_pages))

        def fetch_one(page_number: str) -> pd.DataFrame:
            try:
                # build YQL targeting single page_number and equipment filters
                yql = f'''select id, page_title, topic, equipment, page_number, person, vessel,
                    engine_model, engine_make, make_type, detail, engine_type,
                    source_url, url, info_type, document_type, text from doc
                    where info_type contains "{info_type.lower()}"'''
                # apply equipment filters (AND)
                filters = []

                if equipment_context.get('make'):
                    make = equipment_context['make']
                    filters.append(f'engine_make contains "{make}"')

                if equipment_context.get('detail'):
                    detail = equipment_context['detail']
                    filters.append(f'detail contains "{detail}"')

                if equipment_context.get('engine_type'):
                    engine_type = equipment_context['engine_type']
                    filters.append(f'engine_type contains "{engine_type}"')

                if filters:
                    yql += " and (" + " and ".join(filters) + ")"

                # small targetHits and limit 1 (we want the specific page)
                yql += f' and page_number contains "{page_number}"'

                with self.vespa_app.syncio() as session:
                    response = session.query(
                        yql=yql,                   # only structural filters
                        ranking="fusion-rerank",
                        timeout=self.search_timeout,
                        body={
                            "ranking.profile": "fusion-rerank",
                            "presentation.timing": "true",
                        },
                    )
                    if hasattr(response, "hits") and response.hits:
                        return self._convert_response_to_dataframe(response)
            except Exception as e:
                print(f"⚠️ _fetch_missing_pages_parallel: failed for page {page_number}: {e}")
            return pd.DataFrame()

        results = []
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {executor.submit(fetch_one, p): p for p in missing_pages}
            for future in as_completed(futures):
                try:
                    df = future.result()
                    if df is not None and not df.empty:
                        results.append(df)
                except Exception as e:
                    print(f"⚠️ Error fetching missing page {futures.get(future)}: {e}")

        if results:
            combined = pd.concat(results, ignore_index=True)
            combined = combined.drop_duplicates(subset=["page_number"])
            print(f"✅ _fetch_missing_pages_parallel: retrieved {len(combined)} pages out of {len(missing_pages)} requested")
            return combined

        print("_fetch_missing_pages_parallel: no pages fetched")
        return pd.DataFrame()

    def _enrich_with_missing_pages(
        self, df: pd.DataFrame, equipment_context: Dict[str, str], info_type: str
    ) -> pd.DataFrame:
        """
        Enrich the given DataFrame of Vespa results by:
        - Finding local clusters of consecutive page numbers.
        - Filling missing pages in those clusters.
        - Extending the largest cluster by ± buffer pages.
        - Fetching all missing pages (from cluster and buffer) in parallel.
        """
        if df is None or df.empty:
            print("⚠️ [DEBUG] No data provided for enrichment.")
            return df
        if "page_number" not in df.columns:
            print("⚠️ [DEBUG] No 'page_number' column found in DataFrame.")
            return df

        try:
            page_list = df["page_number"].dropna().astype(str).tolist()
        except Exception:
            print("⚠️ [DEBUG] Could not extract page numbers safely.")
            return df

        # ================================================================
        # STEP 1: Fill missing pages within detected clusters
        # ================================================================
        filled = self._fill_missing_page_numbers(page_list)
        missing = sorted(set(filled) - set(page_list))

        print(f"\n🟨 [DEBUG] Pages found in top results: {sorted(page_list)}")
        print(f"🟩 [DEBUG] Cluster-filled range: {filled}")
        print(f"🟥 [DEBUG] Missing pages identified for fetch: {missing}\n")

        # ================================================================
        # STEP 2: Fetch missing pages from those internal gaps
        # ================================================================
        extra = pd.DataFrame()
        if missing:
            logger.info(f"📘 _enrich_with_missing_pages: fetching internal missing pages {missing}")
            extra = self._fetch_missing_pages_parallel(missing, equipment_context, info_type)

        # Merge fetched results if any
        if extra is not None and not extra.empty:
            print(f"✅ [DEBUG] Internal missing pages fetched: {extra['page_number'].tolist()}")
            combined = pd.concat([df, extra], ignore_index=True).drop_duplicates(subset=["page_number"])
        else:
            combined = df.copy()
            print("_enrich_with_missing_pages: no extra pages fetched — proceeding with existing cluster")

        # ================================================================
        # STEP 3: Find largest cluster and extend it with ± buffer pages
        # ================================================================
        def find_largest_cluster(nums, gap_threshold=1, buffer_size=5):
            nums = sorted(set(int(x) for x in nums if str(x).isdigit()))
            if not nums:
                print("⚠️ [DEBUG] No valid numeric page numbers for clustering.")
                return []

            clusters = []
            current_cluster = [nums[0]]
            for i in range(1, len(nums)):
                if nums[i] - nums[i - 1] <= gap_threshold:
                    current_cluster.append(nums[i])
                else:
                    clusters.append(current_cluster)
                    current_cluster = [nums[i]]
            clusters.append(current_cluster)

            valid_clusters = [c for c in clusters if len(c) >= 2]
            if not valid_clusters:
                valid_clusters = [max(clusters, key=len)]

            extended_cluster = []
            for cluster in valid_clusters:
                min_page = max(min(cluster) - buffer_size, 0)
                max_page = max(cluster) + buffer_size
                extended_cluster.extend(range(min_page, max_page + 1))

            print(f"🧩 [DEBUG] Largest cluster range: {min(extended_cluster)} → {max(extended_cluster)}")
            print(f"🧭 [DEBUG] Extended with buffer ±{buffer_size}: {min_page} → {max_page}")

            return [str(x) for x in extended_cluster]

        # Compute the extended cluster range
        page_nums = combined["page_number"].astype(str).tolist()
        extended_cluster = find_largest_cluster(page_nums, gap_threshold=1, buffer_size=2)

        # ================================================================
        # STEP 4: Fetch missing pages in the extended buffer range
        # ================================================================
        existing_pages = set(str(p) for p in combined["page_number"])
        buffer_missing = sorted(set(extended_cluster) - existing_pages)

        if buffer_missing:
            print(f"🟠 [DEBUG] Additional buffer pages missing: {buffer_missing}")
            buffer_extra = self._fetch_missing_pages_parallel(buffer_missing, equipment_context, info_type)
            if buffer_extra is not None and not buffer_extra.empty:
                print(f"🟢 [DEBUG] Buffer pages fetched successfully: {buffer_extra['page_number'].tolist()}")
                combined = pd.concat([combined, buffer_extra], ignore_index=True).drop_duplicates(subset=["page_number"])
            else:
                print("⚪ [DEBUG] No buffer pages retrieved from Vespa.")
        else:
            print("⚪ [DEBUG] No additional buffer pages to fetch.")

        # ================================================================
        # STEP 5: Final filter and sort
        # ================================================================
        if extended_cluster:
            combined = combined[combined["page_number"].astype(str).isin(extended_cluster)].copy()
            combined["page_number"] = pd.to_numeric(combined["page_number"], errors="coerce")
            combined = combined.sort_values("page_number", ascending=True).reset_index(drop=True)
            print(f"✅ [DEBUG] Filtered with extended cluster — kept {len(combined)} pages and pages list {combined["page_number"].to_list()}")
        else:
            print("⚠️ [DEBUG] No cluster found — returning unmodified DataFrame.")

        return combined

    def prepare_context(self, search_results: pd.DataFrame) -> Tuple[str, Dict[str, str]]:
        """Prepare context string and URL mapping from search results"""
        if search_results.empty:
            return "", {}

        try:
            # Select relevant columns for context
            context_df = search_results[[
                "page_title", "topic", "text", "url", "page_number",
                "engine_make", "engine_model", "make_type", "detail", "engine_type",
                "info_type", "document_type", "person", "vessel"
            ]].fillna("")

            context_parts = []
            url_mapping = {}

            for idx, row in context_df.iterrows():
                context_part = f"Title: {row['page_title']}\n"
                context_part += f"Topic: {row['topic']}\n"
                if row.get('make_type'):
                    context_part += f"make_type: {row['make_type']}\n"
                if row.get('detail'):
                    context_part += f"Detail: {row['detail']}\n"
                if row.get('engine_type'):
                    context_part += f"engine_type: {row['engine_type']}\n"
                if row.get('engine_make') or row.get('engine_model'):
                    context_part += f"Engine: {row.get('engine_make','')} {row.get('engine_model','')}\n"
                context_part += f"InfoType: {row.get('info_type','')}\n"
                context_part += f"Content: {row['text']}\n"

                raw_url = row['url']
                raw_url_1 = row['url'] 
                if isinstance(raw_url, list):      # unwrap Vespa’s list
                    raw_url = raw_url[0] if raw_url else ""
                if isinstance(raw_url_1, list):      # unwrap Vespa’s list
                    raw_url_1 = raw_url_1[0] if raw_url_1 else ""
                raw_url = str(raw_url).strip()
                if raw_url:
                    if "prodprod" in raw_url:
                        raw_url = raw_url.replace("prodprod", "prod")  # fix prodprod → prod
                        raw_url = re.sub(r'/silver/pdf/oem/([^/]+)/page_\d+\.json$', r'/source_data/pdf/oem/\1.pdf', raw_url)
                    context_part += f"Source: {raw_url}"
                    if row['page_number']:
                        context_part += f" (Page: {row['page_number']})"
                    url_mapping[f"{raw_url_1} (Page: {row['page_number']})"] = raw_url

                context_part += "\n" + "-"*50 + "\n"
                context_parts.append(context_part)

            context = "\n".join(context_parts)
            logger.info(f"📋 Context prepared from {len(search_results)} results ({len(context)} characters)")
            return context, url_mapping

        except Exception as e:
            logger.error(f"❌ Error preparing context: {e}")
            return "", {}

    # def prepare_context(self, search_results: pd.DataFrame) -> Tuple[str, Dict[str, str]]:
    #     """
    #     Prepare context string and URL mapping from Vespa search results.
    #     ✅ Includes image placeholders (not presigned)
    #     ✅ Keeps lightweight s3:// references for later signing
    #     """

    #     if search_results.empty:
    #         print("⚠️ No search results provided to prepare_context.")
    #         return "", {}

    #     try:
    #         context_df = search_results[[
    #             "page_title", "topic", "text", "url", "page_number",
    #             "engine_make", "engine_model", "make_type", "detail", "engine_type",
    #             "info_type", "document_type", "person", "vessel"
    #         ]].fillna("")

    #         context_parts = []
    #         url_mapping = {}
    #         bucket = "synergy-oe-propulsionpro-prod"

    #         print(f"🚀 Preparing context for {len(context_df)} Vespa results (non-presigned mode)...")

    #         for idx, row in context_df.iterrows():
    #             context_part = f"Title: {row['page_title']}\n"
    #             context_part += f"Topic: {row['topic']}\n"
    #             if row.get('make_type'):
    #                 context_part += f"make_type: {row['make_type']}\n"
    #             if row.get('detail'):
    #                 context_part += f"Detail: {row['detail']}\n"
    #             if row.get('engine_type'):
    #                 context_part += f"engine_type: {row['engine_type']}\n"
    #             if row.get('engine_make') or row.get('engine_model'):
    #                 context_part += f"Engine: {row.get('engine_make','')} {row.get('engine_model','')}\n"
    #             context_part += f"InfoType: {row.get('info_type','')}\n"
    #             context_part += f"Content: {row['text']}\n"

    #             raw_url = row['url']
    #             raw_url_1 = row['url']
    #             if isinstance(raw_url, list):
    #                 raw_url = raw_url[0] if raw_url else ""
    #             if isinstance(raw_url_1, list):
    #                 raw_url_1 = raw_url_1[0] if raw_url_1 else ""
    #             raw_url = str(raw_url).strip()

    #             if not raw_url:
    #                 print(f"⚠️ Row {idx}: No valid URL found, skipping.")
    #                 continue

    #             # Normalize URL
    #             new_url = raw_url.strip()
    #             if "prodprod" in new_url:
    #                 new_url = new_url.replace("prodprod", "prod")

    #             if "/silver/pdf/oem/" in new_url and new_url.endswith(".json"):
    #                 match = re.search(r"/silver/pdf/oem/([^/]+)/page_\d+\.json$", new_url)
    #                 if match:
    #                     manual = match.group(1)
    #                     new_url = f"s3://{bucket}/source_data/pdf/oem/{manual}.pdf"
    #                     print(f"🔄 URL normalized → {new_url}")
    #                 else:
    #                     print(f"⚠️ URL normalization skipped (no match pattern): {new_url}")
    #             else:
    #                 print(f"🔁 URL already normalized or not Vespa JSON: {new_url}")

    #             raw_url = new_url

    #             # Extract manual and page
    #             match = re.search(r"/oem/([^/]+)(?:/|\.pdf)", raw_url)
    #             manual = match.group(1) if match else None

    #             # Try to get page number from dataframe first
    #             pn = str(row.get("page_number", "")).strip()
    #             if not pn or pn.lower() == "nan":
    #                 m = re.search(r'page[_-]?(\d+)', raw_url, re.IGNORECASE)
    #                 page_num = m.group(1) if m else "?"
    #             else:
    #                 page_num = re.sub(r'\D', '', pn) or "?"

    #             if manual:
    #                 # Add lightweight PDF reference
    #                 pdf_key = f"source_data/pdf/oem/{manual}.pdf"
    #                 pdf_s3_uri = f"s3://{bucket}/{pdf_key}"
    #                 context_part += f"📘 Source PDF: {pdf_s3_uri}\n"
    #                 url_mapping[pdf_s3_uri] = pdf_s3_uri

    #                 # Add image placeholders
    #                 image_found = False
    #                 s3_client = boto3.client('s3', **config.get_aws_credentials())
    #                 for i in range(1, 10):
    #                     for variant in [f"silver/pdf/oem/{manual}/page{page_num}_image{i}.png",
    #                                     f"silver/pdf/oem/{manual}/page{page_num}_img{i}.png"]:
    #                         try:
    #                             s3_client.head_object(Bucket=bucket, Key=variant)
    #                             img_s3_path = f"s3://{bucket}/{variant}"
    #                             context_part += f"![Page {page_num} Image {i}]({img_s3_path})\n"
    #                             url_mapping[img_s3_path] = img_s3_path
    #                             break
    #                         except s3_client.exceptions.ClientError:
    #                             continue
    #                 if not image_found:
    #                     print(f"⚠️ No image references added for manual {manual} page {page_num}")
    #             else:
    #                 print(f"⚠️ Could not extract manual from URL: {raw_url}")

    #             # Add original Vespa source reference
    #             context_part += f"Source: {raw_url}"
    #             if row['page_number']:
    #                 context_part += f" (Page: {row['page_number']})"
    #             url_mapping[raw_url] = raw_url

    #             context_part += "\n" + "-" * 50 + "\n"
    #             context_parts.append(context_part)

    #         context = "\n".join(context_parts)
    #         print(f"\n✅ Context preparation complete. Total records processed: {len(context_df)}")
    #         print(f"🔑 Total s3:// references: {len(url_mapping)}")
    #         logger.info(f"📋 Context prepared from {len(search_results)} results ({len(context)} characters)")
    #         return context, url_mapping

    #     except Exception as e:
    #         logger.error(f"❌ Error preparing context: {e}")
    #         print(f"❌ Exception in prepare_context: {e}")
    #         return "", {}
    
    def get_search_stats(self) -> Dict[str, Any]:
        """Get search statistics"""
        return self.stats.copy()
    
    def reset_stats(self):
        """Reset search statistics"""
        self.stats = {
            "total_searches": 0,
            "tier1_success": 0,
            "tier2_success": 0,
            "tier3_success": 0,
            "total_failures": 0,
            "average_results": 0
        }
        logger.info("📊 Vespa search statistics reset")

# Global Vespa client instance
vespa_client = VespaSearchClient()
