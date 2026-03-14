/*QUERY*/

/* 1) Determinare i clienti più “costosi” per la piattaforma in base al numero di contenuti visualizzati 
nell’ultimo anno (il costo di un contenuto corrisponde a quello indicato nel contratto di cessione).
*/
SELECT utente, SUM(costo) AS costo
FROM cronologia, costoperstream
WHERE cronologia.contenuto=costoperstream.contenuto AND terminato=true
GROUP BY utente
HAVING SUM(costo)>1000
ORDER BY SUM(costo) DESC;

/* 2) Elencare i titoli prodotti interamente in un solo paese, 
ovvero con regia e casa di produzione provenienti dallo stesso paese.
*/
SELECT titolo, registi.nome, registi.cognome, case_produzione.nome
FROM contenuti, registi, contratti_cessione, case_produzione
WHERE contenuti.regia=registi.piva AND contenuti.titolo=contratti_cessione.contenuto 
	AND contratti_cessione.produzione=case_produzione.nome
	AND registi.nazionalità='GB' AND case_produzione.nazionalità='GB'
GROUP BY titolo, registi.nome, registi.cognome, case_produzione.nome
ORDER BY titolo ASC;

/* 3) Determinare i 10 utenti con più ore di visualizzazione nel mese di gennaio 
(vengono considerati i contenuti visualizzati dall’inizio alla fine).
*/
SELECT utente, CAST(SUM(durata) / 60 AS numeric(5,0)) AS ore
FROM cronologia, contenuti
WHERE cronologia.contenuto=contenuti.titolo AND terminato=true 
	AND cronologia.data>='2023-01-01' AND cronologia.data<='2023-01-31'
GROUP BY utente
ORDER BY ore DESC
LIMIT 10;

/* 4) Individuare il documentario più visualizzato della categoria “Biography”
*/
SELECT contenuto, COUNT
from (	SELECT contenuto, COUNT(*)
		FROM cronologia_contenuti, contenuti
		WHERE cronologia_contenuti.contenuto=contenuti.titolo AND categoria='Biography' AND terminato=true
	  	GROUP BY contenuto
	 ) AS piuvisto
WHERE COUNT = (	SELECT MAX(most)
				FROM (	SELECT COUNT(*) AS most
						FROM cronologia_contenuti, contenuti
						WHERE cronologia_contenuti.contenuto=contenuti.titolo AND categoria='Biography' AND terminato=true
					  	GROUP BY contenuto
	 				) AS piuvisto
			)
GROUP BY contenuto, COUNT;

/* 5) Calcolare la media dei film di maggior successo per ogni regista che rispettano le seguenti condizioni:
	- I registi devono aver creato un numero di film superiore a 7
	- Per la media devono essere usati solo i film con valutazione media >3.5
*/
SELECT registi.nome,registi.cognome, CAST(AVG(contenuti.valutazione_media) AS numeric(3,2)) AS MediaRegista
FROM registi
JOIN contenuti ON registi.piva=contenuti.regia
WHERE contenuti.valutazione_media>3.5
GROUP BY registi.nome,registi.cognome
HAVING COUNT(contenuti.titolo)>7
ORDER BY registi.nome ASC;

/* 6) Contenuti con costo di cessione inferiore a 25000, con rating >3, per tutte le fasce d'età 
e con almeno 100 visualizzazioni. Questo per ottenere i film per tutte le fasce d'età che sono costati "poco" 
e che hanno avuto un certo successo (per il nostro database 100 visual sono tante)
*/
SELECT titolo, costo_cessione,valutazione_media
FROM contenuti
	INNER JOIN contratti_cessione ON contenuti.titolo=contratti_cessione.contenuto
	INNER JOIN cronologia_contenuti on contenuti.titolo=cronologia_contenuti.contenuto
WHERE contratti_cessione.costo_cessione<25000 AND rating='T' AND valutazione_media>3
GROUP BY titolo, costo_cessione, valutazione_media
HAVING COUNT(titolo)>100
ORDER BY titolo ASC;

/* 7) Guadagno totale degli attori da film di successo (valutazione >4.5)
*/
SELECT nome, cognome, SUM(compenso) AS compenso_totale
FROM attori
	JOIN casting ON cf=attore
	JOIN contenuti ON contenuto=titolo
WHERE valutazione_media>4.5
GROUP BY nome,cognome;

/* 8) Paesi delle case di produzione più costosi + costo medio per contratto in ordine decrescente
*/
SELECT nazionalità, SUM(costo_cessione) AS costo_totale, CAST(AVG(costo_cessione) AS numeric(10,2)) AS costo_medio
FROM case_produzione
	JOIN contratti_cessione ON produzione=nome
GROUP BY nazionalità
ORDER BY costo_totale DESC;


/*INDICE*/
CREATE INDEX indice_cronologia ON cronologia(contenuto);