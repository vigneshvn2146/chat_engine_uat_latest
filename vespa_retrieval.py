# # -----------------------------------------------------------------------
# # Fetching Documents with empty fields
# import requests
# import logging
# import pandas as pd
# import os
# import time
# from config import config  # Ensure this defines get_vespa_config()

# logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# vespa_cfg = config.get_vespa_config()
# endpoint = vespa_cfg["endpoint"].rstrip("/")
# cert = vespa_cfg["cert_file_path"]
# key = vespa_cfg["key_file_path"]
# NAMESPACE = "doc"
# DOCUMENT_TYPE = "doc"

# OUTPUT_FILE = "vespa_docs_missing_field.csv"


# def safe_append_to_csv(record, output_file):
#     """Append a single record to CSV safely."""
#     df = pd.DataFrame([record])
#     write_header = not os.path.exists(output_file)
#     df.to_csv(output_file, mode="a", header=write_header, index=False)


# def fetch_all_docs_missing_field(field_name):
#     continuation = None
#     processed_count = 0
#     missing_count = 0

#     while True:
#         url = f"{endpoint}/document/v1/{NAMESPACE}/{DOCUMENT_TYPE}/docid"
#         if continuation:
#             url += f"?continuation={continuation}"

#         logging.info(f"📡 Fetching batch from Vespa: {url}")
#         try:
#             response = requests.get(url, cert=(cert, key), timeout=30)
#             if response.status_code != 200:
#                 logging.error(f"❌ Failed to fetch docs: {response.status_code}")
#                 break
#         except Exception as e:
#             logging.error(f"⚠️ Request error: {e}, retrying in 10 seconds...")
#             time.sleep(10)
#             continue

#         data = response.json()
#         documents = data.get("documents", [])
#         logging.info(f"✅ Retrieved {len(documents)} documents in this batch")

#         for doc in documents:
#             try:
#                 docid = doc.get("id")
#                 vespa_id = docid.split("::")[-1]
#                 doc_url = f"{endpoint}/document/v1/{NAMESPACE}/{DOCUMENT_TYPE}/docid/{vespa_id}"

#                 doc_resp = requests.get(doc_url, cert=(cert, key), timeout=30)
#                 if doc_resp.status_code != 200:
#                     logging.warning(f"⚠️ Could not fetch {vespa_id}: {doc_resp.status_code}")
#                     continue

#                 doc_data = doc_resp.json()
#                 fields = doc_data.get("fields", {})
#                 doc_name = fields.get("document_name")

#                 if field_name not in fields:
#                     record = {"id": docid, "doc_name": doc_name, "status": "missing_field"}
#                     safe_append_to_csv(record, OUTPUT_FILE)
#                     missing_count += 1

#                 processed_count += 1
#                 if processed_count % 50 == 0:
#                     logging.info(f"📈 Processed {processed_count} docs, Missing so far: {missing_count}")

#             except Exception as e:
#                 logging.error(f"❌ Error processing doc {docid}: {e}")

#         continuation = data.get("continuation")
#         if not continuation:
#             logging.info("✅ Completed pagination.")
#             break

#     logging.info(f"🏁 Finished. Processed: {processed_count}, Missing: {missing_count}")
#     return


# if __name__ == "__main__":
#     fetch_all_docs_missing_field("engine_type")


# -----------------------------------------------------------------------
# Fetching ALL Documents with ALL Fields
# import requests
# import logging
# import pandas as pd
# import os
# import time
# from config import config  # Ensure this defines get_vespa_config()

# logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# vespa_cfg = config.get_vespa_config()
# endpoint = vespa_cfg["endpoint"].rstrip("/")
# cert = vespa_cfg["cert_file_path"]
# key = vespa_cfg["key_file_path"]
# NAMESPACE = "doc"
# DOCUMENT_TYPE = "doc"

# OUTPUT_FILE = "vespa_all_docs_fields_new.csv"


# def safe_append_to_csv(record, output_file):
#     """Append a single record to CSV safely."""
#     df = pd.DataFrame([record])
#     write_header = not os.path.exists(output_file)
#     df.to_csv(output_file, mode="a", header=write_header, index=False)


# def fetch_all_docs_all_fields():
#     continuation = None
#     processed_count = 0

#     while True:
#         url = f"{endpoint}/document/v1/{NAMESPACE}/{DOCUMENT_TYPE}/docid"
#         if continuation:
#             url += f"?continuation={continuation}"

#         logging.info(f"📡 Fetching batch from Vespa: {url}")
#         try:
#             response = requests.get(url, cert=(cert, key), timeout=30)
#             if response.status_code != 200:
#                 logging.error(f"❌ Failed to fetch docs: {response.status_code}")
#                 break
#         except Exception as e:
#             logging.error(f"⚠️ Request error: {e}, retrying in 10 seconds...")
#             time.sleep(10)
#             continue

#         data = response.json()
#         documents = data.get("documents", [])
#         logging.info(f"✅ Retrieved {len(documents)} documents in this batch")

#         for doc in documents:
#             try:
#                 docid = doc.get("id")
#                 vespa_id = docid.split("::")[-1]
#                 doc_url = f"{endpoint}/document/v1/{NAMESPACE}/{DOCUMENT_TYPE}/docid/{vespa_id}"

#                 doc_resp = requests.get(doc_url, cert=(cert, key), timeout=30)
#                 if doc_resp.status_code != 200:
#                     logging.warning(f"⚠️ Could not fetch {vespa_id}: {doc_resp.status_code}")
#                     continue

#                 doc_data = doc_resp.json()
#                 fields = doc_data.get("fields", {})

#                 # Add docid as a field for clarity
#                 record = {"id": docid}
#                 record.update(fields)

#                 safe_append_to_csv(record, OUTPUT_FILE)

#                 processed_count += 1
#                 if processed_count % 50 == 0:
#                     logging.info(f"📈 Processed {processed_count} docs so far...")

#             except Exception as e:
#                 logging.error(f"❌ Error processing doc {docid}: {e}")

#         continuation = data.get("continuation")
#         if not continuation:
#             logging.info("✅ Completed pagination.")
#             break

#     logging.info(f"🏁 Finished. Total processed: {processed_count}")
#     return


# if __name__ == "__main__":
#     fetch_all_docs_all_fields()

# # ----------------------------------------------------
# # Main Code to Update Values
# import logging
# import time
# import pandas as pd
# import requests
# from vespa.application import Vespa
# from config import config  # Ensure this defines get_vespa_config()

# # ======================
# # 🔧 LOGGING CONFIG
# # ======================
# logging.basicConfig(
#     level=logging.INFO,
#     format="%(asctime)s - %(levelname)s - %(message)s"
# )

# # ======================
# # 📜 DOCUMENT SEARCH LIST
# # ======================
# DOCUMENT_NAMES = "S50MC_01"

# # ======================
# # 📂 CSV CONFIG
# # ======================
# CSV_PATH = "vespa_all_docs_fields_new.csv"  # <- Replace with your actual path


# # ======================
# # 🧠 FETCH AND UPDATE DOCS
# # ======================
# def fetch_and_update_docs_from_csv():
#     vespa_cfg = config.get_vespa_config()
#     endpoint = vespa_cfg["endpoint"].rstrip("/")
#     cert = vespa_cfg["cert_file_path"]
#     key = vespa_cfg["key_file_path"]

#     # Load Vespa session
#     app = Vespa(url=endpoint, cert=cert, key=key)

#     # Read CSV
#     try:
#         df = pd.read_csv(CSV_PATH, on_bad_lines='skip')
#         logging.info(f"📄 Loaded CSV with {len(df)} rows.")
#     except Exception as e:
#         logging.error(f"❌ Failed to read CSV: {e}")
#         return []

#     # Validation
#     if df.empty or df.shape[1] < 2:
#         logging.error("🚫 CSV must have at least two columns (vespa_id, document_name).")
#         return []

#     # Filter CSV based on DOCUMENT_NAMES
#     filtered_df = df[df['info_type']=='fmeca']
#     logging.info(f"🔍 Found {len(filtered_df)} matching rows for target document(s).")

#     if filtered_df.empty:
#         logging.warning("⚠️ No matching documents found in CSV.")
#         return []

#     results = []

#     with app.syncio() as session:
#         for i, row in filtered_df.iterrows():
#             doc_id = str(row.iloc[0]).strip()
#             doc_name = str(row.iloc[1]).strip()

#             if not doc_id:
#                 logging.warning(f"⚠️ Missing vespa_id for row {i}. Skipping.")
#                 continue

#             vespa_id = doc_id.split('::')[-1]
#             print(doc_id, vespa_id)
#             get_url = f"{endpoint}/document/v1/doc/doc/docid/{vespa_id}"
#             logging.info(f"🌐 [{i}] Updating Vespa doc: {vespa_id} ({doc_name})")

#             # Example: Updating a field (customize this as per your needs)
#             update_data = {
#                 "update": doc_id,
#                 "fields": {
#                     "engine_make": {"assign": "man b w"}
#                 }
#             }

#             try:
#                 put_response = requests.put(
#                     get_url,
#                     json=update_data,
#                     cert=(cert, key),
#                     timeout=30
#                 )

#                 if put_response.status_code == 200:
#                     logging.info(f"✅ Successfully updated {vespa_id}")
#                     results.append({"vespa_id": vespa_id, "document_name": doc_name, "status": "updated"})
#                 else:
#                     logging.warning(f"❌ Failed to update {vespa_id} ({put_response.status_code})")
#                     logging.warning(put_response.text[:200])
#                     results.append({
#                         "vespa_id": vespa_id,
#                         "document_name": doc_name,
#                         "status": f"failed ({put_response.status_code})"
#                     })

#             except requests.exceptions.RequestException as e:
#                 logging.error(f"🚨 Request error for {vespa_id}: {e}")
#                 results.append({"vespa_id": vespa_id, "document_name": doc_name, "status": "error"})

#             # Avoid hammering the server
#             time.sleep(0.2)

#     # 💾 Save summary results
#     if results:
#         result_df = pd.DataFrame(results)
#         result_df.to_csv("vespa_update_results.csv", index=False)
#         logging.info(f"💾 Update summary saved to vespa_update_results.csv ({len(result_df)} rows)")
#     else:
#         logging.warning("⚠️ No updates performed.")

#     return results


# # ======================
# # 🚀 MAIN ENTRY POINT
# # ======================
# if __name__ == "__main__":
#     fetch_and_update_docs_from_csv()


# # Vespa Updation of Title
# import logging
# import time
# import os
# import pandas as pd
# import requests
# from vespa.application import Vespa
# from config import config
# from sentence_transformers import SentenceTransformer, util

# # ======================
# # 🔧 LOGGING CONFIG
# # ======================
# logging.basicConfig(
#     level=logging.INFO,
#     format="%(asctime)s - %(levelname)s - %(message)s"
# )

# # ======================
# # 📜 DOCUMENT SEARCH LIST
# # ======================
# DOCUMENT_NAMES = "S50ME-B_01"
# CSV_PATH = "vespa_all_docs_fields.csv"
# RESULT_CSV = "vespa_update_results.csv"

# # ======================
# # ⚙️ SIMILARITY CONFIG
# # ======================
# SIM_THRESHOLD = 0.8  # adjust if needed
# MODEL = SentenceTransformer('all-MiniLM-L6-v2')

# def combine_similar_titles(df):
#     titles = df["page_title"].tolist()
#     embeddings = MODEL.encode(titles, convert_to_tensor=True)
#     combined_titles = []

#     for i in range(len(titles)):
#         current = titles[i]
#         neighbors = []

#         if i > 0:
#             sim_prev = util.cos_sim(embeddings[i], embeddings[i-1]).item()
#             if sim_prev >= SIM_THRESHOLD:
#                 neighbors.append(titles[i-1])

#         if i < len(titles) - 1:
#             sim_next = util.cos_sim(embeddings[i], embeddings[i+1]).item()
#             if sim_next >= SIM_THRESHOLD:
#                 neighbors.append(titles[i+1])

#         if neighbors:
#             merged = " | ".join(sorted(set([current] + neighbors)))
#         else:
#             merged = current

#         combined_titles.append(merged)

#     df["combined_title"] = combined_titles
#     return df


# def append_result_to_csv(result_dict):
#     """Append a single result row to the CSV file in real time."""
#     df_result = pd.DataFrame([result_dict])

#     if not os.path.exists(RESULT_CSV):
#         df_result.to_csv(RESULT_CSV, mode='w', index=False, header=True)
#     else:
#         df_result.to_csv(RESULT_CSV, mode='a', index=False, header=False)


# def fetch_and_update_docs_from_csv():
#     vespa_cfg = config.get_vespa_config()
#     endpoint = vespa_cfg["endpoint"].rstrip("/")
#     cert = vespa_cfg["cert_file_path"]
#     key = vespa_cfg["key_file_path"]

#     app = Vespa(url=endpoint, cert=cert, key=key)

#     try:
#         df = pd.read_csv(CSV_PATH)
#         logging.info(f"📄 Loaded CSV with {len(df)} rows.")
#     except Exception as e:
#         logging.error(f"❌ Failed to read CSV: {e}")
#         return []

#     if "page_title" not in df.columns or "text" not in df.columns:
#         logging.error("🚫 CSV must contain 'page_title' and 'text' columns.")
#         return []

#     filtered_df = df[df['document_name'] == DOCUMENT_NAMES].reset_index(drop=True)
#     logging.info(f"🔍 Found {len(filtered_df)} matching rows for target document(s).")

#     if filtered_df.empty:
#         logging.warning("⚠️ No matching documents found in CSV.")
#         return []

#     # Ensure sorting by page number if present
#     if "page_number" in filtered_df.columns:
#         filtered_df = filtered_df.sort_values(by="page_number").reset_index(drop=True)
#         logging.info(f"🔍 Found {len(filtered_df)} matching rows for target document(s) after filtering")
#     else:
#         filtered_df = filtered_df.reset_index(drop=True)
#         filtered_df["page_number"] = filtered_df.index + 1
#         logging.info("ℹ️ No page_number column found. Using row index as order.")

#     filtered_df = combine_similar_titles(filtered_df)

#     with app.syncio() as session:
#         for i, row in filtered_df.iterrows():
#             doc_id = str(row["id"]).strip()
#             page_title = str(row['page_title']).strip()
#             text = str(row['text']).strip()
#             combined_title = row["combined_title"]
#             updated_text = f"{combined_title} - {row['text']}"

#             if not doc_id:
#                 msg = f"⚠️ Missing vespa_id for row {i}. Skipping."
#                 logging.warning(msg)
#                 append_result_to_csv({
#                     "row_index": i,
#                     "vespa_id": None,
#                     "status": "missing vespa_id",
#                     "message": msg
#                 })
#                 continue

#             vespa_id = doc_id.split('::')[-1]
#             get_url = f"{endpoint}/document/v1/doc/doc/docid/{vespa_id}"

#             update_data = {
#                 "fields": {
#                     "page_title": {"assign": combined_title},
#                     "text": {"assign": updated_text}
#                 }
#             }

#             logging.info(f"🌐 [{i}] Updating Vespa doc: {vespa_id} → {combined_title}")

#             try:
#                 put_response = requests.put(
#                     get_url,
#                     json=update_data,
#                     cert=(cert, key),
#                     timeout=30
#                 )

#                 if put_response.status_code == 200:
#                     status = "updated"
#                     logging.info(f"✅ Successfully updated {vespa_id}")
#                 else:
#                     status = f"failed ({put_response.status_code})"
#                     logging.warning(f"❌ Failed to update {vespa_id}: {put_response.status_code}")
#                     logging.warning(put_response.text[:200])

#                 result = {
#                     "row_index": i,
#                     "vespa_id": vespa_id,
#                     "page_number": row.get("page_number", None),
#                     "page_title": row["page_title"],
#                     "combined_title": combined_title,
#                     "status": status,
#                     "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
#                 }

#                 append_result_to_csv(result)

#             except requests.exceptions.RequestException as e:
#                 msg = f"🚨 Request error for {vespa_id}: {e}"
#                 logging.error(msg)
#                 append_result_to_csv({
#                     "row_index": i,
#                     "vespa_id": vespa_id,
#                     "status": "error",
#                     "message": str(e),
#                     "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
#                 })

#             time.sleep(0.2)

#     logging.info("🏁 All documents processed. Results are being written live to CSV.")


# if __name__ == "__main__":
#     fetch_and_update_docs_from_csv()


#!/usr/bin/env python3
# """
# Bulk-delete Vespa documents where info_type contains 'fmeca'.

# Requirements:
#  - pip install requests pandas
#  - config.get_vespa_config() must return dict with keys:
#      - endpoint (example "https://<vespa-host>:4443")
#      - cert_file_path
#      - key_file_path

# Behavior:
#  - Searches Vespa via YQL for document ids matching info_type contains 'fmeca'
#  - Deletes each document by its docid using HTTP DELETE
#  - Writes a CSV summary (vespa_delete_results.csv) with status for each id
# """

# #!/usr/bin/env python3
# """
# Bulk-delete Vespa documents where info_type = "fmeca".

# Compatible with Vespa Cloud:
#  - Uses POST /search/ (GET with YQL will fail)
#  - Uses TLS cert + private key (same as CLI)
#  - Produces CSV summary of all deletions
# """

# import logging
# import time
# import requests
# import pandas as pd
# from config import config  # Your existing config provider

# # ======================
# # 🔧 LOGGING
# # ======================
# logging.basicConfig(
#     level=logging.INFO,
#     format="%(asctime)s - %(levelname)s - %(message)s"
# )

# # ======================
# # 🔧 CONFIG
# # ======================
# vespa_cfg = config.get_vespa_config()
# ENDPOINT = vespa_cfg["endpoint"].rstrip("/")
# CERT = vespa_cfg["cert_file_path"]
# KEY = vespa_cfg["key_file_path"]

# NAMESPACE = "doc"
# DOCUMENT_TYPE = "doc"

# PAGE_SIZE = 200
# DELETE_SLEEP = 0.1
# SUMMARY_CSV = "vespa_delete_results.csv"


# # ======================
# # 🔍 SEARCH DOC IDS
# # ======================
# def search_fmeca_docids():
#     """
#     Returns all document IDs where info_type = 'fmeca'
#     using POST /search/.
#     """
#     yql = "select documentid from doc where info_type contains 'fmeca'"

#     offset = 0
#     all_ids = []

#     logging.info("🔎 Starting Vespa Cloud search for FMECA documents...")

#     while True:
#         payload = {
#             "yql": yql,
#             "hits": PAGE_SIZE,
#             "offset": offset
#         }

#         url = f"{ENDPOINT}/search/"

#         resp = requests.post(
#             url,
#             json=payload,
#             cert=(CERT, KEY),
#             timeout=30
#         )

#         if resp.status_code != 200:
#             logging.error(f"❌ Search failed: {resp.status_code}")
#             logging.error(resp.text)
#             raise RuntimeError("Vespa query failed")

#         data = resp.json()
#         children = data.get("root", {}).get("children", []) or []

#         logging.info(f"➡️  Received {len(children)} docs at offset {offset}")

#         if not children:
#             break

#         for doc in children:
#             fields = doc.get("fields", {})
#             docid = fields.get("documentid")
#             if docid:
#                 all_ids.append(docid)

#         offset += PAGE_SIZE

#     logging.info(f"✅ Total documents found: {len(all_ids)}")
#     return all_ids


# # ======================
# # 🗑️ DELETE DOC BY ID
# # ======================
# def delete_doc(docid):
#     """
#     Delete a document in Vespa Cloud.
#     """
#     vespa_id = docid.split("::")[-1]

#     delete_url = f"{ENDPOINT}/document/v1/{NAMESPACE}/{DOCUMENT_TYPE}/docid/{vespa_id}"

#     try:
#         r = requests.delete(delete_url, cert=(CERT, KEY), timeout=30)
#         if r.status_code in (200, 202, 204):
#             return True, f"deleted ({r.status_code})"
#         else:
#             return False, f"failed ({r.status_code}) {r.text[:500]}"
#     except requests.RequestException as e:
#         return False, f"error: {str(e)}"


# # ======================
# # 🚀 MAIN FLOW
# # ======================
# def delete_fmeca_docs():
#     docids = search_fmeca_docids()
#     results = []

#     logging.info("🔥 Starting bulk delete...")

#     for docid in docids:
#         logging.info(f"🗑️ Deleting {docid}")

#         success, message = delete_doc(docid)
#         results.append({
#             "documentid": docid,
#             "success": success,
#             "message": message,
#             "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
#         })

#         time.sleep(DELETE_SLEEP)

#     # Save summary
#     df = pd.DataFrame(results)
#     df.to_csv(SUMMARY_CSV, index=False)

#     logging.info(f"📄 Summary written to {SUMMARY_CSV}")

#     logging.info(f"🎉 Finished. Deleted={df['success'].sum()}, Failed={len(df) - df['success'].sum()}")

#     return results


# # ======================
# # 🏁 ENTRY POINT
# # ======================
# if __name__ == "__main__":
#     delete_fmeca_docs()


#!/usr/bin/env python3
"""
Bulk-delete Vespa documents where info_type == 'fmeca', reading IDs directly from CSV.

This version is intentionally simple and matches the style of the code you already use.
No dry-runs, no advanced CLI, no auto-detection logic — just:
    1. Read CSV
    2. Filter rows where info_type == 'fmeca'
    3. Extract vespa_id
    4. Delete the doc from Vespa
    5. Save results to vespa_delete_results.csv

Requirements:
 - pip install pandas requests
 - config.get_vespa_config() must provide endpoint, cert_file_path, key_file_path
"""

# import logging
# import time
# import pandas as pd
# import requests
# from config import config

# # -------------------------------
# # LOGGING
# # -------------------------------
# logging.basicConfig(
#     level=logging.INFO,
#     format="%(asctime)s - %(levelname)s - %(message)s"
# )

# # -------------------------------
# # CONFIG
# # -------------------------------
# vespa_cfg = config.get_vespa_config()
# ENDPOINT = vespa_cfg["endpoint"].rstrip("/")
# CERT = vespa_cfg["cert_file_path"]
# KEY = vespa_cfg["key_file_path"]

# NAMESPACE = "doc"
# DOCUMENT_TYPE = "doc"

# CSV_PATH = "vespa_all_docs_fields_new.csv"   # <-- update if needed
# RESULT_CSV = "vespa_delete_results.csv"
# SLEEP = 0.15

# # -------------------------------
# # MAIN DELETE LOGIC
# # -------------------------------

# def delete_fmeca_docs():
#     try:
#         df = pd.read_csv(CSV_PATH, on_bad_lines='skip')
#     except Exception as e:
#         logging.error(f"Failed to read CSV: {e}")
#         return

#     if "info_type" not in df.columns:
#         logging.error("CSV must have an 'info_type' column.")
#         return

#     if "id" not in df.columns:
#         logging.error("CSV must contain a column named 'id' (full Vespa document id).")
#         return

#     # Filter FMECA docs
#     fmeca_df = df[df['info_type'].astype(str).str.lower().str.contains('fmeca')]
#     logging.info(f"Found {len(fmeca_df)} FMECA documents to delete.")

#     if fmeca_df.empty:
#         logging.info("No FMECA documents found. Nothing to delete.")
#         return

#     results = []

#     for idx, row in fmeca_df.iterrows():
#         raw_id = str(row['id']).strip()
#         if not raw_id:
#             logging.warning(f"Row {idx}: Missing id value. Skipping.")
#             continue

#         vespa_id = raw_id.split("::")[-1]

#         delete_url = f"{ENDPOINT}/document/v1/{NAMESPACE}/{DOCUMENT_TYPE}/docid/{vespa_id}"
#         logging.info(f"Deleting {vespa_id}")

#         try:
#             resp = requests.delete(delete_url, cert=(CERT, KEY), timeout=30)

#             if resp.status_code in (200, 202, 204):
#                 status = "deleted"
#                 logging.info(f"✔ Deleted {vespa_id}")
#             else:
#                 status = f"failed ({resp.status_code})"
#                 logging.warning(f"❌ Failed to delete {vespa_id}: {resp.status_code}")

#             results.append({
#                 "vespa_id": vespa_id,
#                 "documentid": raw_id,
#                 "status": status,
#                 "response": resp.text[:300]
#             })

#         except Exception as e:
#             logging.error(f"Error deleting {vespa_id}: {e}")
#             results.append({
#                 "vespa_id": vespa_id,
#                 "documentid": raw_id,
#                 "status": "error",
#                 "response": str(e)
#             })

#         time.sleep(SLEEP)

#     pd.DataFrame(results).to_csv(RESULT_CSV, index=False)
#     logging.info(f"Summary saved to {RESULT_CSV}")
#     logging.info("All deletions completed.")


# if __name__ == "__main__":
#     delete_fmeca_docs()


# Update Single Vespa Doc
# import requests
# import logging
# from vespa.application import Vespa
# from config import config

# logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# def update_single_doc(vespa_doc_id, field_name, new_value):
#     """
#     Update a single Vespa document by its doc ID.

#     Args:
#         vespa_doc_id (str): The full Vespa doc ID (e.g., "id:doc:doc::12345")
#         field_name (str): The field you want to update
#         new_value (str): The new value to assign
#     """
#     vespa_cfg = config.get_vespa_config()
#     endpoint = vespa_cfg["endpoint"].rstrip("/")
#     cert = vespa_cfg["cert_file_path"]
#     key = vespa_cfg["key_file_path"]

#     # Extract only the ID part after ::
#     doc_id_clean = vespa_doc_id.split("::")[-1]

#     update_url = f"{endpoint}/document/v1/doc/doc/docid/{doc_id_clean}"
#     logging.info(f"Updating Vespa document: {vespa_doc_id}")

#     update_payload = {
#         "update": vespa_doc_id,
#         "fields": {
#             field_name: {"assign": new_value}
#         }
#     }

#     try:
#         response = requests.put(
#             update_url,
#             json=update_payload,
#             cert=(cert, key),
#             timeout=30
#         )

#         if response.status_code == 200:
#             logging.info(f"✅ Successfully updated document: {doc_id_clean}")
#             return {"vespa_id": doc_id_clean, "status": "updated"}
#         else:
#             logging.error(f"❌ Update failed ({response.status_code}): {response.text}")
#             return {"vespa_id": doc_id_clean, "status": "failed"}

#     except requests.exceptions.RequestException as e:
#         logging.error(f"🚨 Request error: {e}")
#         return {"vespa_id": doc_id_clean, "status": "error"}


# if __name__ == "__main__":
#     # Example usage
#     vespa_doc_id = "id:doc:doc::139482e0d40cc0c72ce1be60aae2967e3309f69d148e23b81a408a32f82ec9d0"   # <-- replace with your ID
#     field_to_update = "engine_make"
#     new_value = "man b w"

#     update_single_doc(vespa_doc_id, field_to_update, new_value)

#!/usr/bin/env python3
import requests
import logging
import time
import pandas as pd
from config import config

# ======================
# 🔧 LOGGING
# ======================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# ======================
# 🔧 CONFIG
# ======================
vespa_cfg = config.get_vespa_config()
ENDPOINT = vespa_cfg["endpoint"].rstrip("/")
CERT = vespa_cfg["cert_file_path"]
KEY = vespa_cfg["key_file_path"]

NAMESPACE = "doc"
DOCUMENT_TYPE = "doc"
PAGE_SIZE = 200
RESULT_CSV = "vespa_engine_type_update_results.csv"

# ============================================================
# 🔍 STEP 1: Fetch ALL docs (pagination)
# ============================================================
def fetch_and_update_in_one_pass():
    continuation = None
    results = []

    while True:
        # Fetch a batch of doc IDs
        url = f"{ENDPOINT}/document/v1/{NAMESPACE}/{DOCUMENT_TYPE}/docid"
        if continuation:
            url += f"?continuation={continuation}"

        resp = requests.get(url, cert=(CERT, KEY), timeout=30)
        if resp.status_code != 200:
            logging.error(f"❌ Fetch error: {resp.status_code}")
            break

        data = resp.json()
        docs = data.get("documents", [])

        if not docs:
            break

        for d in docs:
            full_id = d.get("id")
            if not full_id:
                continue

            vespa_id = full_id.split("::")[-1]

            # Fetch full doc data
            doc_url = f"{ENDPOINT}/document/v1/{NAMESPACE}/{DOCUMENT_TYPE}/docid/{vespa_id}"
            doc_resp = requests.get(doc_url, cert=(CERT, KEY), timeout=30)

            if doc_resp.status_code != 200:
                logging.warning(f"⚠️ Could not fetch {vespa_id}")
                continue

            fields = doc_resp.json().get("fields", {})

            info_type = str(fields.get("info_type", "")).lower()
            engine_type = str(fields.get("engine_type", "")).lower()

            # --- Condition check ---
            if info_type == "fmeca":
                new_value = "me_b" if engine_type == "meb" else ("me_c" if engine_type == "mec" else engine_type)

                # Only update when needed
                if new_value != engine_type:
                    logging.info(f"🔧 Updating {vespa_id}: {engine_type} → {new_value}")

                    update_payload = {
                        "update": full_id,
                        "fields": {
                            "engine_type": {"assign": new_value}
                        }
                    }

                    update_resp = requests.put(
                        doc_url,
                        json=update_payload,
                        cert=(CERT, KEY),
                        timeout=30
                    )

                    status = "updated" if update_resp.status_code == 200 else f"failed ({update_resp.status_code})"
                else:
                    status = "no_change"
            else:
                status = "skipped"

            results.append({
                "vespa_id": vespa_id,
                "old": engine_type,
                "new": new_value if info_type == "fmeca" else engine_type,
                "status": status
            })

            time.sleep(0.15)

        continuation = data.get("continuation")
        if not continuation:
            break

    pd.DataFrame(results).to_csv("vespa_engine_type_updates.csv", index=False)
    logging.info("Done.")


if __name__ == "__main__":
    fetch_and_update_in_one_pass()


