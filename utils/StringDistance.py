import numpy as np
from nltk.util import ngrams
from collections import Counter

#### String distance
def ngram_similarity(s1, s2, n=3):
    # Convert strings into character-level n-grams
    ngrams1 = Counter(ngrams(s1, n))
    ngrams2 = Counter(ngrams(s2, n))

    # Compute intersection and union of n-grams
    intersection = sum((ngrams1 & ngrams2).values())
    union = sum((ngrams1 | ngrams2).values())

    return intersection / union if union != 0 else 0  # Jaccard similarity

def lcs(s1, s2):
    m, n = len(s1), len(s2)
    dp = np.zeros((m+1, n+1))

    for i in range(1, m+1):
        for j in range(1, n+1):
            if s1[i-1] == s2[j-1]:
                dp[i][j] = dp[i-1][j-1] + 1
            else:
                dp[i][j] = max(dp[i-1][j], dp[i][j-1])

    return dp[m][n] / max(m, n)  # Normalized between 0 and 1

if __name__ == "__main__":
    str1 = "NCC DALIA".lower()
    str2 = "dalia".lower()

    print(f"string distance b/w {str1} & {str2} is : {ngram_similarity(str1, str2, n=2)}")