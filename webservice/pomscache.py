from dogpile.cache import make_region 
from dogpile.cache.util import kwarg_function_key_generator

pomscache = make_region(
   function_key_generator = kwarg_function_key_generator
).configure(
   "dogpile.cache.dbm",
   expiration_time = 300,
   arguments = {
      "filename": "/tmp/poms_dogpile_cache"
   }
)

pomscache_10 = make_region(
   function_key_generator = kwarg_function_key_generator
).configure(
   "dogpile.cache.dbm",
   expiration_time = 10,
   arguments = {
      "filename": "/tmp/poms_dogpile_cache_10"
   }
)
