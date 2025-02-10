# EthKV


```bash
--cache 0 # Disable the cache (set size to 0)
--snapshot # Enable KV snapshot support (by default is enabled)
--cache.noprefetch # Disable the cache prefetching to avoid the sequence interference 
export GOMAXPROCS=1 # Set the number of threads to 1 (default is the number of CPU cores)
./geth --cache 0 --cache.noprefetch --snapshot --mainnet --datadir ./data --syncmode full --http --http.api eth,net,engine,admin --authrpc.jwtsecret ../jwt.hex
```