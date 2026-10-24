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
- Clickhouse Kafka Sink tohle řeší ale neumí moc upravovat data (-> irelevantní)
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
- goroutines pokud by byla anonymizace složitější
- jinak více instancí lepší (?)
- pokud by byl bottleneck přepisování, šel by udělat worker pooling