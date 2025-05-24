import spacy
from collections import Counter
import os

# --- Configuration ---
# File containing 'TICKER: Company Name Ltd.' one per line
TICKER_AND_COMPANY_INPUT_FILE = "tivker.txt" 

# Minimal set of always-excluded lemmas (common suffixes, articles, conjunctions)
# These are words we don't want to count as part of the company's core identity words.
MANUAL_EXCLUDED_LEMMAS = {
    "ltd", "limited", "inc", "incorporated", "corp", "corporation", 
    "pvt", "private", "group", "fund", "etf", "index", "company", 
    "holding", "holdings", "trust", "plc", "llp", "llc", "co", 
    "&", "of", "the", "and", "in", "for", "on", "at", "to", "with", "by",
    # Common verbs if they appear as part of names sometimes, though less likely for core identity
    "is", "are", "be", "was", "were", 
    # Numbers as words if they are not significant (e.g. "one" in "Company One Ltd")
    # "one", "two", "three" # Be cautious with these
}

# Thresholds for identifying potentially generic terms
# A term is considered "potentially generic" if it appears in more than this percentage of company names.
GENERIC_TERM_COMPANY_PERCENT_THRESHOLD = 0.03  # e.g., term appears in > 3% of all company names
# Or, a term appears at least this many times in absolute terms across all company names (raw frequency)
GENERIC_TERM_MIN_ABSOLUTE_COUNT_THRESHOLD = 5 # e.g., term appears at least 5 times (adjust based on dataset size)
# Minimum length for a word to be considered for generic analysis (to avoid very short, common letter sequences)
MIN_WORD_LENGTH_FOR_ANALYSIS = 3


def load_company_names_from_file(filepath=TICKER_AND_COMPANY_INPUT_FILE):
    """
    Reads 'TICKER: Company Name' from each line of the specified file.
    Returns a list of company names.
    """
    company_names = []
    if not os.path.exists(filepath):
        print(f"Error: Input file '{filepath}' not found.")
        return company_names

    print(f"Loading company names from '{filepath}'...")
    try:
        with open(filepath, mode='r', encoding='utf-8-sig') as infile:
            for line_num, line in enumerate(infile, 1):
                line = line.strip()
                if not line or line.startswith('#'): # Skip empty lines or comments
                    continue
                
                parts = line.split(':', 1) # Split only on the first colon
                if len(parts) == 2:
                    # ticker = parts[0].strip().upper() # We don't need the ticker for this script
                    company_name = parts[1].strip()
                    if company_name:
                        company_names.append(company_name)
                    else:
                        print(f"Warning in '{filepath}' (line {line_num}): Empty company name for line: '{line}'")
                else:
                    print(f"Warning in '{filepath}' (line {line_num}): Could not parse line: '{line}'. Expected 'TICKER: Company Name'")
        print(f"Loaded {len(company_names)} company names for analysis.")
    except Exception as e:
        print(f"Error loading data from '{filepath}': {e}")
    return company_names

def analyze_names(company_names_list, nlp_instance):
    """
    Analyzes company names to find frequent lemmas.
    Returns a list of lemmas sorted by their document frequency (how many company names they appear in).
    """
    if not company_names_list:
        print("No company names provided for analysis.")
        return []

    print(f"Analyzing {len(company_names_list)} company names with spaCy...")

    # lemma_raw_frequency: counts every occurrence of a lemma
    lemma_raw_frequency = Counter()
    # lemma_document_frequency: counts how many unique company names a lemma appears in
    lemma_document_frequency = Counter() 
    
    processed_docs_count = 0
    # Use nlp.pipe for efficient processing of many texts
    # Disable components not strictly needed for lemmatization to speed it up.
    # NER might be useful to identify ORGs if you want to treat multi-word org names differently,
    # but for simple frequency, just lemmatizing is fine.
    for doc in nlp_instance.pipe(company_names_list, disable=["parser", "ner"]): 
        unique_lemmas_in_this_company = set()
        for token in doc:
            lemma_lower = token.lemma_.lower()
            # Filter conditions: not a stop word, not punctuation, not in manual exclusion, and meets min length
            if not token.is_stop and \
               not token.is_punct and \
               lemma_lower not in MANUAL_EXCLUDED_LEMMAS and \
               len(lemma_lower) >= MIN_WORD_LENGTH_FOR_ANALYSIS:
                
                lemma_raw_frequency[lemma_lower] += 1
                unique_lemmas_in_this_company.add(lemma_lower)
        
        for unique_lemma in unique_lemmas_in_this_company:
            lemma_document_frequency[unique_lemma] += 1
        
        processed_docs_count += 1
        if processed_docs_count % 100 == 0:
            print(f"  Processed {processed_docs_count}/{len(company_names_list)} company names...")
    
    print("Analysis complete.")
    
    # --- Identify Potentially Generic Terms ---
    num_companies = len(company_names_list)
    data_driven_generic_terms = set()

    print(f"\n--- Identifying Potentially Generic Terms (threshold: > {GENERIC_TERM_COMPANY_PERCENT_THRESHOLD*100:.1f}% of companies OR min count {GENERIC_TERM_MIN_ABSOLUTE_COUNT_THRESHOLD}) ---")
    # Sort by document frequency (most common across companies first)
    sorted_by_doc_freq = sorted(lemma_document_frequency.items(), key=lambda item: item[1], reverse=True)

    for lemma, doc_freq in sorted_by_doc_freq:
        raw_freq = lemma_raw_frequency[lemma] # Get the raw frequency too
        percentage_companies = (doc_freq / num_companies * 100) if num_companies > 0 else 0
        
        is_generic_by_percentage = num_companies > 0 and (doc_freq / num_companies) > GENERIC_TERM_COMPANY_PERCENT_THRESHOLD
        is_generic_by_absolute_count = raw_freq >= GENERIC_TERM_MIN_ABSOLUTE_COUNT_THRESHOLD
        
        # A term is considered generic if it meets EITHER the percentage threshold OR the absolute count threshold
        if is_generic_by_percentage or is_generic_by_absolute_count:
            data_driven_generic_terms.add(lemma)
            print(f"  '{lemma}': Appears in {doc_freq} companies ({percentage_companies:.1f}%), Raw Freq: {raw_freq} -> Marked as POTENTIALLY GENERIC")
        # else:
            # Optionally print terms that are frequent but didn't cross the "generic" threshold
            # if doc_freq > 1: # Print if it appears in more than one company
            #     print(f"  '{lemma}': Appears in {doc_freq} companies ({percentage_companies:.1f}%), Raw Freq: {raw_freq}")


    print(f"\n--- Top 20 Most Frequent Lemmas (Document Frequency) ---")
    for i, (lemma, count) in enumerate(sorted_by_doc_freq[:20]):
        percentage_companies = (count / num_companies * 100) if num_companies > 0 else 0
        print(f"{i+1}. '{lemma}': in {count} companies ({percentage_companies:.1f}%) (Raw Freq: {lemma_raw_frequency[lemma]})")

    print(f"\n--- Top 20 Most Frequent Lemmas (Raw Frequency) ---")
    sorted_by_raw_freq = sorted(lemma_raw_frequency.items(), key=lambda item: item[1], reverse=True)
    for i, (lemma, count) in enumerate(sorted_by_raw_freq[:20]):
        doc_freq_for_lemma = lemma_document_frequency[lemma]
        percentage_companies = (doc_freq_for_lemma / num_companies * 100) if num_companies > 0 else 0
        print(f"{i+1}. '{lemma}': {count} times (in {doc_freq_for_lemma} companies - {percentage_companies:.1f}%)")
        
    return sorted(list(data_driven_generic_terms))


def main():
    print("Loading spaCy model (en_core_web_sm)...")
    try:
        # Load spaCy model once for the analysis
        nlp = spacy.load("en_core_web_sm")
    except OSError:
        print("spaCy model 'en_core_web_sm' not found. Please run: python -m spacy download en_core_web_sm")
        return
    except Exception as e:
        print(f"An error occurred while loading the spaCy model: {e}")
        return
    print("spaCy model loaded.")

    company_names = load_company_names_from_file()
    if not company_names:
        return

    identified_generic_terms = analyze_names(company_names, nlp)
    
    print("\n\n====================================================================")
    print("Data-Driven Potentially Generic Terms Identified:")
    print("====================================================================")
    if identified_generic_terms:
        for term in identified_generic_terms:
            print(f"- {term}")
        print(f"\nTotal {len(identified_generic_terms)} potentially generic terms found based on thresholds.")
        print("Consider adding these (or a subset) to the HIGHLY_GENERIC_TERMS list in your main mapping script.")
    else:
        print("No terms met the criteria to be marked as data-driven generic.")
    
    print("\nReview the 'Top 20 Most Frequent Lemmas' lists above for other common terms.")

if __name__ == "__main__":
    main()