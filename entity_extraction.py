"""
Maritime Entity Extraction Module - Updated to Follow Flowchart Logic
Implements exact flowchart decision tree with 90% vessel match threshold
Combines enhanced LLM entity extraction with database lookup functionality

Response format: {
    make: str, model: str, equipment: str, vessel: str,
    make_type: str, detail: str, engine_type: str,
    info_type: str, response: str
}
"""

import pandas as pd
import numpy as np
from rapidfuzz import fuzz
import logging
import json
import re
from typing import Dict, Any, List, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
from config import config
from llm_client import llm_client
from difflib import get_close_matches

logger = logging.getLogger(__name__)

def normalize(text: str) -> str: 
    """Normalize text: lowercase, remove special chars, strip spaces.""" 
    return re.sub(r"[^a-z0-9\s&/-]", "", str(text).lower()).strip()

class MaritimeEntityExtractor:
    """
    Production-ready maritime entity extractor following flowchart logic
    """
    
    def __init__(self):
        """Initialize entity extractor with configuration"""
        self.entity_config = config.get_entity_config()
        self.db_df = None
        self.stats = {
            "queries_processed": 0,
            "direct_make_model": 0,
            "vessel_db_lookup_success": 0,
            "vessel_db_lookup_failed": 0,
            "vessel_below_threshold": 0,
            "imo_db_lookup_success": 0,
            "imo_db_lookup_failed": 0,
            "entity_requests": 0,
            "errors": 0
        }
        
        self.vessel_match_threshold = 90.0
        
        # Normalized dictionaries (rebuilt after DB load)
        self.norm_makes = {}
        self.norm_models = {}
        self.norm_equipment = {}
        self.norm_vessels = {}

        self._load_database()
    
    def _load_database(self) -> bool:
        """Load the database with robust error handling"""
        try:
            path = self.entity_config['csv_path']
            if path.endswith('.csv'):
                self.db_df = pd.read_csv(path, encoding='latin1')
            elif path.endswith(('.xlsx', '.xls')):
                self.db_df = pd.read_excel(path, engine='openpyxl')
            else:
                logger.error(f"❌ Unsupported file format: {path}")
                return False
            
            if self.db_df.empty:
                logger.error("❌ Database file is empty")
                return False
            
            self._prepare_database()
            self._build_dictionaries()
            
            logger.info(f"✅ Database loaded successfully with {len(self.db_df)} records")
            return True
            
        except FileNotFoundError:
            logger.error(f"❌ Database file not found: {self.entity_config['csv_path']}")
            self.db_df = pd.DataFrame()
            return False
        except Exception as e:
            logger.error(f"❌ Failed to load database: {str(e)}")
            self.db_df = pd.DataFrame()
            return False
    
    def _prepare_database(self) -> None:
        """Prepare and clean database for efficient lookups"""
        try:
            for col in ['MAKE', 'MODEL', 'equipment_type', 'Vessel Name', 'IMO Number']:
                if col in self.db_df.columns:
                    self.db_df[col] = self.db_df[col].fillna('').astype(str)
            
            if 'Vessel Name' in self.db_df.columns:
                self.db_df['vessel_name_lower'] = self.db_df['Vessel Name'].str.lower().str.strip()
            if 'IMO Number' in self.db_df.columns:
                self.db_df['imo_searchable'] = self.db_df['IMO Number'].astype(str).str.strip()
            
            logger.debug("✅ Database preparation completed")
            
        except Exception as e:
            logger.error(f"❌ Error preparing database: {str(e)}")

    def _build_dictionaries(self) -> None:
        """Build normalized lookup dictionaries directly from DB"""
        def build_norm_map(series):
            return {normalize(v): v.strip() for v in series.dropna().unique() if normalize(v)}
        
        if 'MAKE' in self.db_df.columns:
            self.norm_makes = build_norm_map(self.db_df['MAKE'])
        if 'MODEL' in self.db_df.columns:
            self.norm_models = build_norm_map(self.db_df['MODEL'])
        if 'equipment_type' in self.db_df.columns:
            self.norm_equipment = build_norm_map(self.db_df['equipment_type'])
        if 'Vessel Name' in self.db_df.columns:
            self.norm_vessels = build_norm_map(self.db_df['Vessel Name'])

        logger.info("✅ Lookup dictionaries rebuilt from DB")

    def _clean_value(self, v) -> str:
        """Normalize and treat common 'junk' values as empty."""
        if v is None:
            return ""
        s = str(v).strip()
        if not s:
            return ""
        if s.lower() in {"-", "n/a", "na", "none", "null"}:
            return ""
        return s

    def process_query(self, query: str) -> Dict[str, Any]:
        """
        Main processing method following the EXACT flowchart logic
        Returns format: {make: str, model: str, equipment: str, vessel: str, response: str}
        """
        self.stats["queries_processed"] += 1
        
        if not query.strip():
            result = self._create_response("", "", "", "", "Please provide some information about your vessel or equipment.")
            print(result)
            return result
        try:
            
            missing_fields = ["MAKE", "MODEL", "EQUIPMENT", "VESSELNAME", "IMO", "MAKETYPE", "DETAIL", "ENGINETYPE"]
            print(f"Missing Fields; {missing_fields}")
            entities = {}
            if missing_fields:
                try:
                    entities = llm_client.extract_entities_with_llm(query, fields=missing_fields)
                    print(f"LLM Extraction Result: {entities}")
                except Exception as e:
                    logger.warning(f"⚠️ LLM extraction failed: {e}")

            # Merge results: fast_result gets priority (keeps what it found)
            make_type = entities.get("MAKETYPE", {}).get("value", "").strip()
            detail = entities.get("DETAIL", {}).get("value", "").strip()
            engine_type = entities.get("ENGINETYPE", {}).get("value", "").strip()
            make_value = entities.get("MAKE", {}).get("value", "").strip()
            model_value = entities.get("MODEL", {}).get("value", "").strip()
            equipment_value = entities.get("EQUIPMENT", {}).get("value", "").strip()
            vessel_value = entities.get("VESSELNAME", {}).get("value", "").strip()
            imo_value = entities.get("IMO", {}).get("value", "").strip()

            print(f"{make_type} {detail} {engine_type} {make_value} {model_value} {equipment_value} {vessel_value} {imo_value}")

            # Step 3: Follow EXACT flowchart decision tree
            if make_value and model_value:
                # FLOWCHART PATH: MAKE + MODEL → Direct Use
                self.stats["direct_make_model"] += 1
                result = self._create_response(
                    make_value, model_value, equipment_value, vessel_value, response="True",
                    info_type="", make_type=make_type, detail=detail, engine_type=engine_type
                )
                print(result)
                return result

            elif vessel_value:
                # FLOWCHART PATH: VESSELNAME → Database Lookup with 90% threshold
                logger.info(f"🚢 FLOWCHART PATH: VESSELNAME lookup for '{vessel_value}'")
                return self._handle_vessel_lookup(
                    vessel_value,
                    equipment_value,
                    make_type=make_type,
                    detail=detail,
                    engine_type=engine_type,
                    fast_make=make_value,
                    fast_model=model_value
                )

            elif imo_value:
                # FLOWCHART PATH: IMO → Database Lookup
                logger.info(f"🆔 FLOWCHART PATH: IMO lookup for '{imo_value}'")
                return self._handle_imo_lookup(
                    imo_value, equipment_value, vessel_value,
                    info_type="", make_type=make_type, detail=detail, engine_type=engine_type
                )

            else:
                # FLOWCHART PATH: No Required Entities → Request
                logger.info("❌ FLOWCHART PATH: No required entities found - requesting")
                self.stats["entity_requests"] += 1
                result = self._create_response(
                    "", "", equipment_value, vessel_value,"Please provide the equipment make and model, or vessel name, or IMO number to help me assist you better."
                )
                print(result)
                return result
                
        except Exception as e:
            logger.error(f"❌ Error processing query: {str(e)}")
            self.stats["errors"] += 1
            reuslt = self._create_response("", "", "", "", "System error occurred. Please try again.")
            print(result)
            return result

    def _handle_vessel_lookup(
            self,
            vessel_name: str,
            equipment_value: str,
            info_type: str = "",
            make_type: str = "",
            detail: str = "",
            engine_type: str = "",
            fast_make: str = "",
            fast_model: str = ""
        ) -> Dict[str, str]:
        """
        Handle vessel lookup following flowchart logic with 90% threshold.
        DB fills only missing fields and we try to pick a DB row that matches
        the fast_make (preferred) or equipment_value (secondary).
        """
        logger.info(f"🚢 Starting vessel DB lookup for: '{vessel_name}'")
        db_results = self.get_the_db_vessel(vessel_name)

        if not db_results:
            logger.info(f"❌ FLOWCHART: Vessel '{vessel_name}' not found in database")
            self.stats["vessel_db_lookup_failed"] += 1
            return self._create_response(
                "", "", equipment_value, vessel_name, response="True"
                f"Unfortunately, we couldn't find vessel '{vessel_name}' in our database. Please provide the equipment make and model directly."
            )

        best_match = db_results[0]
        match_score = best_match.get('_match_score', 0.0)
        logger.info(f"🎯 Best vessel match score: {match_score:.1f}%")

        if match_score < self.vessel_match_threshold:
            logger.info(f"❌ FLOWCHART: Vessel match <90% ({match_score:.1f}%) - requesting make/model")
            self.stats["vessel_below_threshold"] += 1
            return self._create_response(
                "", "", equipment_value, vessel_name, response="True"
                f"We found a vessel '{best_match.get('Vessel Name', 'Unknown')}' but the match confidence is only {match_score:.1f}%. Please provide the equipment make and model directly."
            )

        # match_score >= threshold: pick a DB row that best matches fast_make (if provided)
        fm_clean = self._clean_value(fast_make)
        fm_norm = normalize(fm_clean) if fm_clean else ""
        equip_norm = normalize(equipment_value) if equipment_value else ""

        candidate = None

        # 1) If fast_make exists, prefer DB rows whose MAKE matches it (normalized equality)
        if fm_norm:
            make_matches = [r for r in db_results if normalize(r.get('MAKE', '')) == fm_norm]
            if make_matches:
                candidate = max(make_matches, key=lambda r: r.get('_match_score', 0.0))
                logger.info(
                    f"🔎 Chosen candidate by MAKE match: '{candidate.get('Vessel Name')}' "
                    f"MAKE='{candidate.get('MAKE')}' MODEL='{candidate.get('MODEL')}' "
                    f"(score {candidate.get('_match_score')})"
                )

        # 2) If we don't have a candidate yet, try equipment_type match (Auxiliary Engine, Purifier, etc.)
        if candidate is None and equip_norm:
            eq_matches = []
            for r in db_results:
                rec_equip = r.get('equipment_type') or r.get('equipment') or ""
                rec_equip_norm = normalize(rec_equip)
                if rec_equip_norm and (equip_norm in rec_equip_norm or rec_equip_norm in equip_norm):
                    eq_matches.append(r)
            if eq_matches:
                candidate = max(eq_matches, key=lambda r: r.get('_match_score', 0.0))
                logger.info(
                    f"🔎 Chosen candidate by EQUIPMENT match: '{candidate.get('Vessel Name')}' "
                    f"MAKE='{candidate.get('MAKE')}' MODEL='{candidate.get('MODEL')}' "
                    f"(score {candidate.get('_match_score')})"
                )

        # 3) fallback to best_match
        if candidate is None:
            candidate = best_match
            logger.info(
                f"🔎 No candidate matched MAKE/equipment; falling back to best_match: "
                f"'{candidate.get('Vessel Name')}' MAKE='{candidate.get('MAKE')}' MODEL='{candidate.get('MODEL')}' "
                f"(score {candidate.get('_match_score')})"
            )

        # extract cleaned DB fields from the selected candidate
        make_from_db = self._clean_value(candidate.get('MAKE', ''))
        model_from_db = self._clean_value(candidate.get('MODEL', ''))
        vessel_matched = str(candidate.get('Vessel Name', '')).strip()

        # Final selection rules:
        # ✅ Always keep fast_make if available, else DB
        final_make = fm_clean or make_from_db

        # ✅ For model, prefer fast_model ONLY if it looks like a real model (contains digits)
        fm_clean_model = self._clean_value(fast_model)
        if fm_clean_model and any(ch.isdigit() for ch in fm_clean_model):
            final_model = fm_clean_model
        else:
            final_model = model_from_db

        logger.info(
            f"✅ FLOWCHART: Vessel match ≥90% - FINAL_MAKE: '{final_make}', FINAL_MODEL: '{final_model}'"
        )

        self.stats["vessel_db_lookup_success"] += 1

        return self._create_response(
            final_make, final_model, equipment_value, vessel_matched, response="True",
            info_type=info_type, make_type=make_type, detail=detail, engine_type=engine_type
        )
    
    def _handle_imo_lookup(
        self,
        imo: str,
        equipment_value: str,
        vessel_value: str,
        info_type: str = "",
        make_type: str = "",
        detail: str = "",
        engine_type: str = ""
    ) -> Dict[str, str]:
        """
        Handle IMO lookup following flowchart logic
        """
        logger.info(f"🆔 Starting IMO DB lookup for: '{imo}'")
        
        # Call database lookup
        db_results = self.get_the_db_imo(imo)
        
        if db_results:
            # FLOWCHART PATH: IMO Found → Extract Make/Model from DB
            record = db_results[0]
            make = str(record.get('MAKE', '')).strip()
            model = str(record.get('MODEL', '')).strip()
            vessel_from_db = str(record.get('Vessel Name', '')).strip()
            
            logger.info(
                f"✅ FLOWCHART: IMO found - MAKE: '{make}', MODEL: '{model}', VESSEL: '{vessel_from_db}'"
            )
            self.stats["imo_db_lookup_success"] += 1
            
            return self._create_response(
                make,
                model,
                equipment_value,
                vessel_from_db or vessel_value,
                response="True",
                info_type=info_type,
                make_type=make_type,
                detail=detail,
                engine_type=engine_type
            )
        else:
            # FLOWCHART PATH: IMO Not Found → Request Make/Model
            logger.info(f"❌ FLOWCHART: IMO '{imo}' not found in database")
            self.stats["imo_db_lookup_failed"] += 1
            
            return self._create_response(
                "",
                "",
                equipment_value,
                vessel_value,
                "False",
                info_type=info_type,
                make_type=make_type,
                detail=detail,
                engine_type=engine_type
            )

    
    def get_the_db_vessel(self, vessel_name: str) -> List[Dict[str, Any]]:
        """
        Get database records by vessel name using fuzzy matching with score calculation
        """
        if not self._is_database_ready() or not vessel_name.strip():
            return []
        
        logger.info(f"🚢 Looking up vessel: '{vessel_name}' using {self.entity_config['fuzzy_algorithm']}")
        
        if 'Vessel Name' not in self.db_df.columns:
            logger.error("❌ 'Vessel Name' column not found in database")
            return []
        
        try:
            # Get fuzzy matching function
            fuzzy_func = self._get_fuzzy_matching_func()
            
            # Calculate similarity scores
            vessel_names = self.db_df['Vessel Name'].astype(str)
            scores = []
            
            for vessel_db_name in vessel_names:
                try:
                    score = fuzzy_func(vessel_name.lower(), vessel_db_name.lower())
                    scores.append(score)
                except Exception:
                    scores.append(0.0)
            
            scores = np.array(scores)
            
            if len(scores) == 0:
                logger.warning("⚠️ No vessel names found in database")
                return []
            
            # Get top candidates (sort by score)
            max_candidates = 10
            top_indices = np.argsort(scores)[-max_candidates:][::-1]
            top_scores = scores[top_indices]
            
            # Create results with scores (don't filter by threshold here - let caller decide)
            results = []
            for idx, score in zip(top_indices, top_scores):
                if score > 0:  # Only include non-zero scores
                    matched_record = self.db_df.iloc[idx].to_dict()
                    matched_record['_match_score'] = float(score)  # Ensure float
                    results.append(matched_record)
                    
                    # Log match details
                    vessel_name_matched = matched_record.get('Vessel Name', 'Unknown')
                    make = matched_record.get('MAKE', 'Not found')
                    model = matched_record.get('MODEL', 'Not found')
                    
                    logger.info(f"🎯 Vessel candidate: '{vessel_name_matched}' (score: {score:.1f}%)")
                    logger.debug(f"🔧 DB record - MAKE: '{make}', MODEL: '{model}'")
            
            # Sort by score (highest first)
            results.sort(key=lambda x: x['_match_score'], reverse=True)
            
            if results:
                logger.info(f"✅ Found {len(results)} vessel candidates, best score: {results[0]['_match_score']:.1f}%")
            else:
                logger.info("❌ No vessel matches found")
            
            return results
                
        except Exception as e:
            logger.error(f"❌ Error in vessel fuzzy matching: {str(e)}")
            return []
    
    def get_the_db_imo(self, imo: str) -> List[Dict[str, Any]]:
        """
        Get database records by IMO number using exact and partial matching
        """
        if not self._is_database_ready() or not imo.strip():
            return []
        
        logger.info(f"🆔 Looking up IMO: '{imo}'")
        
        if 'IMO Number' not in self.db_df.columns:
            logger.error("❌ 'IMO Number' column not found in database")
            return []
        
        try:
            results = []
            
            # First try exact match
            exact_matches = self.db_df[
                self.db_df['IMO Number'].astype(str).str.strip() == imo
            ]
            
            if not exact_matches.empty:
                logger.info(f"✅ Exact IMO match found")
                results = exact_matches.head(self.entity_config['top_results']).to_dict(orient='records')
            else:
                # Try partial match
                partial_matches = self.db_df[
                    self.db_df['IMO Number'].astype(str).str.contains(
                        imo, case=False, na=False, regex=False
                    )
                ]
                
                if not partial_matches.empty:
                    logger.info(f"✅ Partial IMO match found")
                    results = partial_matches.head(self.entity_config['top_results']).to_dict(orient='records')
                else:
                    logger.info(f"❌ No IMO match found")
                    return []
            
            # Log extracted information for first result
            if results:
                make = results[0].get('MAKE', 'Not found')
                model = results[0].get('MODEL', 'Not found')
                vessel = results[0].get('Vessel Name', 'Not found')
                logger.info(f"🔧 Extracted from DB - MAKE: '{make}', MODEL: '{model}', VESSEL: '{vessel}'")
            
            return results
            
        except Exception as e:
            logger.error(f"❌ Error in IMO lookup: {str(e)}")
            return []
    
    def _is_database_ready(self) -> bool:
        """Check if database is loaded and ready"""
        if self.db_df is None or self.db_df.empty:
            logger.error("❌ Database not loaded or empty")
            return False
        return True
    
    def _get_fuzzy_matching_func(self):
        """Get the fuzzy matching function based on configuration"""
        algorithm_map = {
            "ratio": fuzz.ratio,
            "partial_ratio": fuzz.partial_ratio,
            "token_sort_ratio": fuzz.token_sort_ratio,
            "WRatio": fuzz.WRatio
        }
        return algorithm_map.get(self.entity_config['fuzzy_algorithm'], fuzz.WRatio)
    
    def _create_response(
        self,
        make: str,
        model: str,
        equipment: str,
        vessel: str,
        response: str,
        info_type: str = "",
        make_type: str = "",
        detail: str = "",
        engine_type: str = ""
    ) -> Dict[str, Any]:
        """Create response in the required format, aligned with Vespa schema (make_type, detail, engine_type). Info_type is injected later by classify_query_type."""
        response_dict = {
            'make': make,
            'model': model,
            'equipment': equipment,
            'vessel': vessel,
            'response': response,
            'info_type': info_type,
            'make_type': make_type,
            'detail': detail,
            'engine_type': engine_type
        }
        print(f"Entity Response: {response_dict}")
        return {
            'make': make,
            'model': model,
            'equipment': equipment,
            'vessel': vessel,
            'response': response,
            'info_type': info_type,
            'make_type': make_type,
            'detail': detail,
            'engine_type': engine_type
        }

    def get_stats(self) -> Dict[str, Any]:
        """Get processing statistics with flowchart path breakdown"""
        total = self.stats["queries_processed"]
        enhanced_stats = self.stats.copy()
        
        if total > 0:
            enhanced_stats["direct_make_model_rate"] = self.stats["direct_make_model"] / total
            enhanced_stats["vessel_success_rate"] = self.stats["vessel_db_lookup_success"] / total
            enhanced_stats["imo_success_rate"] = self.stats["imo_db_lookup_success"] / total
            enhanced_stats["entity_request_rate"] = self.stats["entity_requests"] / total
            
            # Calculate vessel lookup effectiveness
            total_vessel_lookups = (
                self.stats["vessel_db_lookup_success"] + 
                self.stats["vessel_db_lookup_failed"] + 
                self.stats["vessel_below_threshold"]
            )
            if total_vessel_lookups > 0:
                enhanced_stats["vessel_90_threshold_success"] = (
                    self.stats["vessel_db_lookup_success"] / total_vessel_lookups
                )
        
        return enhanced_stats
    
    def get_database_info(self) -> Dict[str, Any]:
        """Get database information"""
        try:
            if self.db_df is None or self.db_df.empty:
                return {
                    "status": "empty",
                    "records": 0,
                    "columns": [],
                    "error": "Database not loaded or empty"
                }
            
            info = {
                "status": "loaded",
                "records": len(self.db_df),
                "columns": list(self.db_df.columns),
                "config": self.entity_config,
                "vessel_match_threshold": self.vessel_match_threshold,
                "stats": self.get_stats()
            }
            
            # Add sample data if available
            for col_name in ['Vessel Name', 'MAKE', 'MODEL', 'IMO Number']:
                if col_name in self.db_df.columns:
                    unique_values = self.db_df[col_name].dropna().unique()
                    info[f"sample_{col_name.lower().replace(' ', '_')}"] = unique_values[:5].tolist()
                    info[f"total_{col_name.lower().replace(' ', '_')}"] = len(unique_values)
            
            return info
            
        except Exception as e:
            logger.error(f"❌ Error getting database info: {str(e)}")
            return {
                "status": "error",
                "records": 0,
                "columns": [],
                "error": str(e)
            }

# Global entity extractor instance
entity_extractor = MaritimeEntityExtractor()

if __name__ == "__main__":
    # Test the flowchart logic
    test_queries = [
        "Wartsil 6L4 engine problems"
        #"Wartsila 6L46 engine problems"
        #"How to do MAN B&W 6S50MC engine maintenance and operation"          # Should request entities
    ]
        #     "Wartsila 6L46 engine problems",  # Should go direct (MAKE+MODEL)
        # "MV Atlantic engine issues",      # Should go vessel lookup
        # "IMO 1234567 maintenance",        # Should go IMO lookup  
        # "Engine overheating", 
    
    print("🧪 Testing Flowchart Logic")
    print("=" * 50)
    
    for query in test_queries:
        print(f"\nQuery: '{query}'")
        result = entity_extractor.process_query(query)
        print(f"Result: {result}")
        print("-" * 30)
    
    print(f"\n📊 Statistics:")
    stats = entity_extractor.get_stats()
    for key, value in stats.items():
        print(f"  {key}: {value}")