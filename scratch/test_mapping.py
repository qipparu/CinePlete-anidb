import sys
import os

# Add app directory to path
sys.path.append(os.getcwd())

from app.anidb_mapping import get_mapper

def test_mapping():
    mapper = get_mapper()
    # Re:Zero has AniDB 11370 and should map to TMDB TV 65129
    # In the XML it has tmdbtv="65129"
    aid = 11370
    entry = mapper.lookup(aid)
    print(f"AniDB {aid}: {entry}")
    if entry and entry.tmdb_id:
        print(f"SUCCESS: Found TMDB ID {entry.tmdb_id}")
    else:
        print("FAILURE: TMDB ID not found")

if __name__ == "__main__":
    test_mapping()
