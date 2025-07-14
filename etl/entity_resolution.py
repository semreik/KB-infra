"""
Simple entity resolution for supplier names using Jaccard similarity.
"""
from typing import List, Tuple
import re
from collections import defaultdict
import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

def normalize_name(name: str) -> str:
    """Normalize company name for comparison."""
    # Remove legal entities
    name = re.sub(r'\b(LLC|Inc\.|Ltd\.|Corp\.|Limited)\b', '', name)
    # Convert to lowercase and remove punctuation
    name = re.sub(r'[^\w\s]', '', name.lower())
    return name.strip()

def jaccard_similarity(set1: set, set2: set) -> float:
    """Calculate Jaccard similarity between two sets."""
    intersection = len(set1.intersection(set2))
    union = len(set1.union(set2))
    return intersection / union if union > 0 else 0

def find_similar_names(names: List[str], threshold: float = 0.8) -> List[Tuple[str, str, float]]:
    """Find similar company names using both Jaccard and cosine similarity."""
    normalized_names = [normalize_name(name) for name in names]
    
    # TF-IDF for cosine similarity
    vectorizer = TfidfVectorizer(analyzer='char', ngram_range=(2,3))
    tfidf_matrix = vectorizer.fit_transform(normalized_names)
    
    # Calculate similarities
    matches = []
    for i in range(len(names)):
        for j in range(i+1, len(names)):
            # Jaccard similarity on word sets
            words1 = set(normalized_names[i].split())
            words2 = set(normalized_names[j].split())
            jaccard = jaccard_similarity(words1, words2)
            
            # Cosine similarity on character n-grams
            cosine = cosine_similarity(tfidf_matrix[i:i+1], tfidf_matrix[j:j+1])[0][0]
            
            # Combined score (average of both)
            score = (jaccard + cosine) / 2
            
            if score >= threshold:
                matches.append((names[i], names[j], score))
    
    return sorted(matches, key=lambda x: x[2], reverse=True)

def cluster_suppliers(names: List[str], threshold: float = 0.8) -> dict:
    """Cluster supplier names into groups of likely matches."""
    clusters = defaultdict(set)
    processed = set()
    
    matches = find_similar_names(names, threshold)
    
    # Build clusters
    for name1, name2, score in matches:
        if name1 not in processed or name2 not in processed:
            # Find or create cluster
            cluster_id = None
            for cid, cluster in clusters.items():
                if name1 in cluster or name2 in cluster:
                    cluster_id = cid
                    break
            
            if cluster_id is None:
                cluster_id = len(clusters)
            
            clusters[cluster_id].add(name1)
            clusters[cluster_id].add(name2)
            processed.add(name1)
            processed.add(name2)
    
    # Add singletons
    for name in names:
        if name not in processed:
            clusters[len(clusters)] = {name}
    
    return dict(clusters)

if __name__ == "__main__":
    # Example usage
    test_names = [
        "Acme Corp",
        "ACME Corporation",
        "Globex Inc",
        "Globex Corporation",
        "Totally Different Ltd"
    ]
    
    clusters = cluster_suppliers(test_names)
    for cluster_id, names in clusters.items():
        print(f"Cluster {cluster_id}: {names}")
