import redis

IS_INITIAL_IMPORT = False


# INDEXED_FRBR_CLUSTERS_BY_UUID = {}
# INDEXED_FRBR_CLUSTERS_BY_TITLES = {}
# INDEXED_FRBR_CLUSTERS_BY_RAW_RECORD_ID = {}
# INDEXED_MANIFESTATIONS_BY_UUID = {}

INDEXED_FRBR_CLUSTERS_BY_UUID = redis.Redis(db=0)
INDEXED_FRBR_CLUSTERS_BY_TITLES = redis.Redis(db=1)
INDEXED_FRBR_CLUSTERS_BY_RAW_RECORD_ID = redis.Redis(db=2)
INDEXED_MANIFESTATIONS_BY_UUID = redis.Redis(db=3)

# Redis connection pools for final converter (multiple instances of FinalConverters are allowed)
