from dogpile.cache import make_region 
from dogpile.cache.util import kwarg_function_key_generator

pomscache = make_region(
   function_key_generator = kwarg_function_key_generator
).configure(
   "dogpile.cache.dbm",
   arguments = {
      "filename": "/tmp/poms_dogpile_cache",
      "rw_lockfile": False,
      "dogpile_lockfile": False,
   }
)

pomscache_10 = make_region(
   function_key_generator = kwarg_function_key_generator
).configure(
   "dogpile.cache.dbm",
   expiration_time = 10,
   arguments = {
      "filename": "/tmp/poms_dogpile_cache_10",
      "rw_lockfile": False,
      "dogpile_lockfile": False, # note this only coordinates within processes
   }
)
