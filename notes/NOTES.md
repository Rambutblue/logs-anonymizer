## Thought process/poznámky
### Assumptions
 - IP adresa v log entries je x.x.x.x, jinak je log invalid a zahodim ho (?) - ověřit log producerem

### Initial poznámky/návrh
- velikost batchů - ? - počítat z provozu ?
- rozdělit na 2 části - anonymizace a pushování logů do Clickhousu
- batche ukládat do souborů (DB?) pro persistance + startup recovery

### Persistance
 - Kafka
 - commit až po tom co jsou logy zapsané v Clickhousu
 - řeší scalability (Kafka partitions)
 - nepotřebuju DB/Soubor zápis

#### Atomicita
- Clickhouse commit + Kafka commit - dupes jenom pokud se stane Clickhouse insert bez Kafka commitu
- ukládat logy tak aby se neuložil stejnej 2x (hash nějakých atributů, ?, zpomalí)
  - pro případ že anonymizer spadne uprostřed téhle operace
  - Click house [ReplacingMergeTree](https://clickhouse.com/docs/reference/engines/table-engines/mergetree-family/replacingmergetree) 
  - duplicates které by se mohly objevit (ne moc často - jen při nešťastném pádu) - nebudou vidět 
  - klíč (order by) - resourceId a timestamp (a ip?)

### Batche
- buď velikost nebo čas - co se naplní dřív
- nastavitelné z configu

### Log input formát
```log
2026-10-24 14:16:00 2026/10/24 12:16:00 Decoded: timestampEpochMilli=1792841824754 resourceId=420575753 bytesSent=883321 requestTimeMilli=3313 responseStatus=403 cacheStatus="STALE" method="HEAD" remoteAddr="177.33.54.104" url="/images/preview/media/fetch/b12ia4grz7lgrjq9ul45w.json"
```
- -> stačí přepsat adresu

### Paralelizace
- goroutines pro anonymizaci pokud by byla složitější
- jinak více instancí lepší (?)
- pokud by byl bottleneck přepisování, šel by udělat worker pooling

## Report

### Clickhouse model
 - `http_log` tabulka drží raw data 
 - `http_log_totals_mv` - MV držící agregované statistky po minutách, resource_id, response_status, cache_status 
  - tyhle 3 statistiky by neměly mít velkou kardinalitu, tak by měl být dobrý poměr řádků které zagregují 
 - pro agregaci ip adres je třeba agregovat přes raw tabulku, nebo případně přidat další MV
 - `http_log` má `ReplacingMergeTree` engine - pro případ, že je záznam vložen vícekrát (je vložen, ale není commitnut do Kafky - při restartu se vloží znova)
 - pro stejné batche vložené vícekrát: `SETTINGS non_replicated_deduplication_window = 1000`
 - `"insert_deduplicate": 1, "deduplicate_blocks_in_dependent_materialized_views": 1` - propisuje dropnuté batche i pro MV
 - **limitace/problém** - záznamy deduplikované pomocí `ReplacingMergeTree` enginu se už nepropíšou do MV
  - řešením by mohl být rebuild MV po pádu, nebo duplikáty ignorovat, s tím že pády by neměly být časté a neměly by výrazně ovlivnit statistiky (realný počet logs <= počet logs v mv) a pro přesné logy lze udělat query nad `http_log` tabulkou s FINAL keywordem

### Aplikace
- `main` rozběhne 2 goroutines
- `fetch` tahá logy z Kafky a předává je přes channel `Run`, který v loopu anonymizuje a insertuje do Clickhousu pokud je batch plný nebo uplynulo dost času
- nekonzistence nastává v případě že je insertnut batch ale není commited do Kafky (další spuštění insertne změny znovu) - tohle řeší `ReplacingMergeTree` v Clickhousus - anonymizer to dále neřeší
- paralelizace lze pomocí vícero instancí a nastavení více partitions Kafky

### Testování
- Unit testy - testují main loop v `processor.go` a samotnou anonymizační logiku
- E2E test script v `./test`
  - testuje celý proces, vkládá předem určené logy python scriptem
  - přepínač `--chaos` náhodně vypíná Kafku/Clickhouse/Anonymizer pro testování konzistence při výpadku

### Performance

| `LOG_BATCH_SIZE` | 1 instance (logs/s) | 2 instances (logs/s) | zrychlení (2 vs 1 instance) |
|---|---|---|---|
| 500 | 54 600 | 102 600 | 1.88× |
| 1000 | 89 500 | 170 600 | 1.91× |
| 2000 | 124 600 | 236 800 | 1.90× |
| 5000 | 177 500 | 325 300 | 1.83× |
| 10000 | 198 000 | 381 000 | 1.92× |
| 20000 | 216 000 | 421 000 | 1.95× |
| 50000 | 233 800 | 458 700 | 1.96× |
| 100000 | 233 300 | 483 600 | 2.07× |

- ve všech runech měla Kafka 2 partitions

#### Čas (zjednodušený hrubý odhad)

- `T_batch = T_fixed + N · T_row`, kde `N` = `LOG_BATCH_SIZE`
  - `T_fixed ≈ 7.9 ms` – režie jednoho batche (insert + commit round trip do Clickhousu a Kafky), nezávisí na `N`
  - `T_row ≈ 4.14 µs` – cena jednoho řádku (decode, anonymizace, `Append`, komprese, přenos přes channel)
- `logs/s = N / T_batch`, pro velké `N` jde k limitu `1 / T_row ≈ 241 000 logs/s` na instanci
- odhady jsou vypočítány z výše uvedené tabulky pro 1 instanci (lineární regrese z mých pár běhů)

#### Závěr
- 1 instance má strop asi 240k logs/s
- 10k je okay default batch size
- u vyšších batch sizes je bottleneckem zpracovávání logů (očekávané)

### Improvements/Pitfalls

#### Latence
- log je zapsán do DB nejpozději po `LOG_BATCH_MAX_TIME` + insert time (za předpokladu že nenastane výpadek)
- batch size/time je třeba nastavit ručně

#### Data loss
- Anonymizer nebo Clickhouse spadne na více než je `KAFKA_LOG_RETENTION_MS`
- neanonymizovatelné logy jsou dropnuty

#### Duplikáty
- popsáno v Clickhouse a Paralelizace sekci

#### Paralelizace
- při rebalancování také dochází k vytváření duplicit, které se nepríšou do `http_log` (mergene je), ale do MV ano, stejně jako nešťastný restart 
- při 2 instancích a 2 Kafka partitions je zrychlení blízko lineárnímu (+ ~90%)
- viz performance sekce

#### Možná rozšíření
- fix duplikace při přerozdělování partitions
- adaptive batch sizing
- paralelní anonymizování v rámci jedné instance
- fetchování dalšího batche zatímco se předchozí insertuje
- backfill a korekce MV v DB nebo jiné řešení MV

#### Ostatní
- aplikace obecně moc neřeší errorhandling 
  - Clickhouse errory jsou logovány jako warnings a insert je zkoušen dokola, chyba při čtení/anonymizování znamená přeskočení logu
  - Stejné s Kafkou - fetch je zkoušen v loopu, nezávisle na erroru co vrací
- chybí integrační testy

### Čas
- projekt mi zabral zhruba 14-18h s tím, že  rozdělení stráveného času mohlo být zhruba 45-35-20 (research-implementace-testování/debug)
  

