### Assumptions
 - IP adresa v log entries je x.x.x.x, jinak je log invalid a zahodim ho (?) - ověřit log producerem

### Initial poznámky/návrh
- velikost batchů - ? - počítat z provozu ?
- rozdělit na 2 části - anonymizace a pushování logů do Clickhousu
- batche ukládat do souborů (DB?) pro persistance + startup recovery
- insert do Clickhouse pod 1mil rows (max_insert_block_size) - + další podmínky? - je atomic

### Persistance
 - Kafka :)
 - commit až po tom co jsou logy zapsané v Clickhousu
 - řeší scalability (Kafka partitions)
 - nepotřebuju DB/Soubor zápis)

#### Atomicita
- Kafka commit + Clickhouse commit
- ukládat logy tak aby se neuložil stejnej 2x (hash nějakých atributů, ?, zpomalí)
- uložit offset kafky do db společně s logama (to snad bude atomic?)

### Batche
- buď velikost nebo čas - co se naplní dřív
- nastavitelné z konfigu

### Atomicity Clickhousu
- ??

### Log input formát
- ??

### Paralelizace
- ??