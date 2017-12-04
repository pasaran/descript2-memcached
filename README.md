# descript2-memcached

## Usage

```
const de = require('descript2');
const deMemcached = require('descript2-memcached');

const context = new de.Context(req, res, {
    cache: new deMemcached(myCacheConfig)  
});
```

## Config

```
{
    defaultKeyTTL: 60 * 60 * 24, // key ttl in seconds
    generation: 1, // increment generation to invalidate all key across breaking changes releases
    readTimeout: 100, // read timeout in milliseconds,
    memcachedOptions: {}, // @see https://github.com/3rd-Eden/memcached#options
}
```
