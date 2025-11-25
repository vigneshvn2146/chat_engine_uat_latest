# import pandas as pd
# import numpy as np
# from utils.StringDistance import ngram_similarity

# def get_the_db(vessel_name=None, imo=None):
#     # Load CSV with correct encoding
#     df = pd.read_csv('./utils/MainEngine.csv', encoding='latin1')

#     # Process vessel_name
#     if vessel_name:
#         df['Vessel Name'] = df['Vessel Name'].astype(str)  # Ensure column is string
#         scores = np.asarray([ngram_similarity(vessel_name.lower(), str(i).lower()) for i in df['Vessel Name']])
#         best_match_idx = np.argmax(scores)
#         print(f"string distance : {scores.max()}")

#         if scores.max() >= 0.6:
#             df = df.iloc[[best_match_idx]]  # Keep it as DataFrame
#         else:
#             return "No Closest Match Found"

#     # Process IMO
#     if imo:
#         df = df[df['IMO Number'].fillna('').astype(str).str.contains(str(imo), case=False, na=False)]

#     # Convert to structured dictionary output
#     return df.to_dict(orient='records')  # Returns list of dicts

# if __name__ == "__main__":
#     print(f"Given vessel is preludge : {get_the_db(vessel_name="preludge", imo=None)}")
#     print(f"Given vessel is ACE ETERNITY : {get_the_db(vessel_name="ACE ETERNITY", imo=None)}")

import pandas as pd
import numpy as np
from rapidfuzz import fuzz

def get_the_db(vessel_name=None, imo=None):
    # Load CSV with correct encoding
    df = pd.read_csv('./utils/MainEngine.csv', encoding='latin1')

    # Process vessel_name with RapidFuzz WRatio
    if vessel_name:
        df['Vessel Name'] = df['Vessel Name'].astype(str)  # Ensure column is string
        
        # Compute similarity scores using fuzz.WRatio
        scores = np.array([fuzz.WRatio(vessel_name.lower(), str(i).lower()) for i in df['Vessel Name']])
        best_match_idx = np.argmax(scores)  # Get index of best match
        max_score = scores[best_match_idx]

        print(f"String similarity score: {max_score}")

        # Apply threshold for match acceptance
        if max_score >= 80:  
            df = df.iloc[[best_match_idx]]  # Keep best match row as DataFrame
        else:
            return "No Closest Match Found"

    # Process IMO
    if imo:
        df = df[df['IMO Number'].fillna('').astype(str).str.contains(str(imo), case=False, na=False)]

    # Convert to structured dictionary output
    return df.to_dict(orient='records')  # Returns list of dicts

if __name__ == "__main__":
    print(f"Given vessel is preludge : {get_the_db(vessel_name='preludge', imo=None)}")
    print(f"Given vessel is ACE ETERNITY : {get_the_db(vessel_name='ACE ETERNITY', imo=None)}")