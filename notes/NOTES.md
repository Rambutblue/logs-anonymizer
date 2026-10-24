### Assumptions
 - IP adresa v log entries je x.x.x.x, jinak je log invalid a zahodim ho (?) - ověřit log producerem

### Initial poznámky/návrh
- velikost batchů - ? - počítat z provozu ?
- rozdělit na 2 části - anonymizace a pushování logů do Clickhousu
- batche ukládat do souborů (DB?) pro persistance + startup recovery
- insert do Clickhouse pod 1mil rows (max_insert_block_size) - + další podmínky? - je atomic

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
- nastavitelné z konfigu

### Atomicity Clickhousu
- ??

### Log input formát
- ??

### Paralelizace
- ??