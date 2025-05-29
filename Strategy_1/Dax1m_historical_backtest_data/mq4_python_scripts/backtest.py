# Remove or comment out the outdated global declaration
# global allowSession1  # ← REMOVE this line

# Use the updated strategyState dictionary instead
strategyState["allowSession1"]
strategyState["allowSession2"]

# Example usage in the script:
if strategyState["allowSession1"]:
    # Logic for session1
    ...
if strategyState["allowSession2"]:
    # Logic for session2
    ...