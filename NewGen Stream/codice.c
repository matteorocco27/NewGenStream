#define PORT 5432

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "dependencies/include/libpq-fe.h"
#include <ctype.h>


void checkResults(PGresult *res, const PGconn *conn)
{
    if (PQresultStatus(res) != PGRES_TUPLES_OK)
    {
        printf("Risultati inconsistenti %s\n", PQerrorMessage(conn));
        PQclear(res);
        exit(1);
    }
}

void printquery(PGresult *res)
{
    int tuple = PQntuples(res);
    int campi = PQnfields(res);

    for (int i = 0; i < campi; i++)
    {
        printf("%s\t\t", PQfname(res, i));
    }
    printf("\n");

    for (int i = 0; i < tuple; i++)
    {
        for (int j = 0; j < campi; j++)
        {
            printf("%s\t\t", PQgetvalue(res, i, j));
        }
        printf("\n");
    }
}

int main(int argc, char **argv)
{
    char conninfo[250];
    char user[50];
    char password[50];
    char dbname[50];
    char hostaddr[50];
    char port[50];

    printf("\nInserire utente per connessione:");
    scanf("%s", user);
    printf("\nInserire password per connessione:");
    scanf("%s", password);
    printf("\nInserire nome database per connessione:");
    scanf("%s", dbname);
    printf("\nInserire host address per connessione:");
    scanf("%s", hostaddr);

    sprintf(conninfo, "user=%s password=%s dbname=%s hostaddr=%s port=%d", user, password, dbname, hostaddr, PORT);

    PGconn *conn;
    conn = PQconnectdb(conninfo);

    if (PQstatus(conn) != CONNECTION_OK)
    {
        printf("Errore di connessione: %s\n", PQerrorMessage(conn));
        PQfinish(conn);
        exit(1);
    }
    else
    {
        printf("Connessione avvenuta correttamente\n");
        int operazione = 0;
        do
        {
            printf("\n\nBenvenut* nel database di NewGen Streaming, selezionare l'operazione che si vuole eseguire:\n");
            printf("1) Determinare i 'x' clienti piu' costosi per la piattaforma in base al numero di contenuti visualizzati nel ultimo anno \n(il costo di un contenuto corrisponde a quello indicato nel contratto di cessione).\n");
            printf("2) Elencare i titoli prodotti interamente in un solo paese, ovvero con regia e casa di produzione provenienti dallo stesso paese.\n");
            printf("3) Determinare gli 'x' utenti con piu' ore di visualizzazione nel mese selezionato del 2023 (vengono considerati i contenuti visualizzati dal inizio alla fine)\n");
            printf("4) Individuare il documentario piu' visualizzato del genere selezionato\n");
            printf("5) Calcolare la media delle valutazioni dei film di maggior successo per ogni regista che ha creato piu' di TOT film e che ha tutti i film con una valutazione media superiore a 3.5. \n");
            printf("6) Contenuti con costo di cessione inferiore a X, con rating >Y, per tutte le fasce d'eta' e con almeno N visualizzazioni.\n");
            printf("7) Denaro totale guadagnato da attori che hanno avuto un ruolo in films con una certa valutazione media 0-5.\n");
            printf("8) Determinare il costo totale e il costo medio dei contratti cessione delle case di produzione appartenenenti ad un determinato paese\n");
            printf("Inserire 0 per uscire\n\n");
            scanf("%d", &operazione);
            if (operazione == 1)
            {
                char query[550];
                int numero=0;
                printf("\nInserire il numero di utenti da visualizzare:");
                scanf("%d", &numero);
                sprintf(query, "select utente, sum(costo) from cronologia, costoperstream where cronologia.contenuto=costoperstream.contenuto and terminato=true group by utente having sum(costo)>1000 order by sum(costo) desc limit %d;", numero);
                
                PGresult *res;
                res = PQexec(conn, query);
                checkResults(res, conn);
                printquery(res);
                PQclear(res);
            }
            if (operazione == 2)
            {
                char query[550];
                char paese[2];
                printf("\nInserire il paese scelto (ad esempio GB, US, JP, IT) (case sensitive):");
                scanf("%s", paese);
                sprintf(query, "select titolo, registi.nome, registi.cognome, case_produzione.nome from contenuti, registi, contratti_cessione, case_produzione where contenuti.regia=registi.piva and contenuti.titolo=contratti_cessione.contenuto and contratti_cessione.produzione=case_produzione.nome and registi.nazionalità='%s' and case_produzione.nazionalità='%s' group by titolo, registi.nome, registi.cognome, case_produzione.nome order by titolo asc;", paese, paese);
                
                PGresult *res;
                res = PQexec(conn, query);
                checkResults(res, conn);
                printquery(res);
                PQclear(res);
            }
            if (operazione == 3)
            {
                char query[550];
                char mese[2];
                int giorno=0;
                int numero=0;
                printf("\nInserire il numero di utenti da visualizzare:");
                scanf("%d", &numero);
                printf("\nInserire il mese (gennaio='01', febbraio='02', ecc...):\n");
                scanf("%s", mese);
                printf("\nMese: %s\n", mese);
                if(strcmp("02", mese)==0)
                    giorno = 28;
                else if(strcmp("01", mese)==0 || strcmp("03", mese)==0 || strcmp("05", mese)==0 || strcmp("07", mese)==0 || strcmp("08", mese)==0 || strcmp("10", mese)==0 || strcmp("12", mese)==0)
                    giorno = 31;
                else if(strcmp("04", mese)==0 || strcmp("06", mese)==0 || strcmp("09", mese)==0 || strcmp("11", mese)==0)
                    giorno = 30;
                
                
                sprintf(query, "select utente, cast(sum(durata) / 60 as numeric(5,0)) as ore from cronologia, contenuti where cronologia.contenuto=contenuti.titolo and terminato=true and cronologia.data>='2023-%s-01' and cronologia.data<='2023-%s-%d' group by utente order by ore desc limit %d;", mese, mese, giorno, numero);
                PGresult *res;
                res = PQexec(conn, query);
                checkResults(res, conn);
                printquery(res);
                PQclear(res);
            }
            if (operazione == 4)
            {
                char query[550];
                char genere[20];
                printf("\nInserire il genere ('History', 'Biography', 'Music', 'Politics', 'Economics', 'Art'):\n");
                scanf("%s", genere);
                sprintf(query, "select contenuto, count from (	select contenuto, count(*) from cronologia_contenuti, contenuti where cronologia_contenuti.contenuto=contenuti.titolo and categoria='%s' and terminato=true group by contenuto ) as piuvisto where count= (	select max(most) from (	select count(*) as most from cronologia_contenuti, contenuti where cronologia_contenuti.contenuto=contenuti.titolo and categoria='%s' and terminato=true group by contenuto ) as piuvisto ) group by contenuto, count;", genere, genere);

                PGresult *res;
                res = PQexec(conn, query);
                checkResults(res, conn);
                printquery(res);
                PQclear(res);
            }
            if (operazione == 5)
            {
                int nFilm;
                printf("Inserire il numero minimo di film che un regista deve aver creato: ");
                scanf("%d", &nFilm);

                char query[500];
                sprintf(query, "SELECT registi.nome,registi.cognome, cast(avg(contenuti.valutazione_media) as numeric(3,2)) as MediaRegista FROM registi JOIN contenuti ON registi.piva=contenuti.regia WHERE contenuti.valutazione_media > 3.5 GROUP BY registi.nome,registi.cognome HAVING count(contenuti.titolo) > %d ORDER BY registi.nome ASC;", nFilm);

                PGresult *res;
                res = PQexec(conn, query);
                checkResults(res, conn);

                if(PQntuples(res)==0){
                    printf("La query non ha restituito nessun risultato\n");
                }
                else{
                     printquery(res);
                    PQclear(res);

                }
            }
            if(operazione==6){
                float costoCessione;
                float rating;
                int visualizzazioni;

                printf("Inserire costo massimo cessione (usare il punto invece che la virgola) \n");
                scanf("%f",&costoCessione);
                printf("Inserire rating minimo (usare il punto)\n");
                scanf("%f",&rating);
                printf("Inserire numero di visualizzazioni\n\n");
                scanf("%d",&visualizzazioni);

                char query[500];
                sprintf(query,"select titolo, costo_cessione,valutazione_media "
                                "from contenuti "
                                "inner join contratti_cessione on contenuti.titolo=contratti_cessione.contenuto "
                                "inner join cronologia_contenuti on contenuti.titolo=cronologia_contenuti.contenuto "
                                "where contratti_cessione.costo_cessione<%f and rating='T' and valutazione_media>%f "
                                "group by titolo, costo_cessione, valutazione_media "
                                "having count(titolo)>%d "
                                "order by titolo asc ",costoCessione,rating,visualizzazioni);
                
                PGresult *res;
                res = PQexec(conn, query);
                checkResults(res, conn);
                
                if(PQntuples(res)==0){
                    printf("La query non ha restituito nessun risultato\n");
                }
                else{
                     printquery(res);
                    PQclear(res);

                }
               
            }
            if(operazione==7){
                float valutazione_media;

                printf("Inserire valutazione media (usare punto invece che la virgola)\n");
                scanf("%f",&valutazione_media);

                char query[500];
                sprintf(query,"select nome,cognome,sum(compenso) as compenso_totale "
                                "from attori "
                                "join casting on cf=attore "
                                "join contenuti on contenuto=titolo "
                                "where valutazione_media>%f "
                                "group by nome,cognome",valutazione_media);

                PGresult *res;
                res = PQexec(conn, query);
                checkResults(res, conn);

                if(PQntuples(res)==0){
                    printf("La query non ha restituito nessun risultato\n");
                }
                else{
                     printquery(res);
                    PQclear(res);

                }
            }
            if(operazione==8){
                
                printf("\nScegliere tra le seguenti nazionalita'\n(US,IN,FR,GB,DE,KR,HK,JP,AU,IT)\n");
                char paese[10];
                scanf("%s",paese);

                paese[0]=toupper(paese[0]);
                paese[1]=toupper(paese[1]);

                char query[500];
                sprintf(query,"SELECT nazionalità, sum(costo_cessione) as costo_totale, cast(avg(costo_cessione) as numeric(10,2)) as costo_medio "
                                "FROM case_produzione "
                                "JOIN contratti_cessione ON produzione = nome "
                                "WHERE nazionalità = '%s' "
                                "group by nazionalità "
                                "ORDER BY costo_totale DESC;",paese);

                PGresult *res;
                res = PQexec(conn, query);
                checkResults(res, conn);

                if(PQntuples(res)==0){
                    printf("La query non ha restituito nessun risultato\n");
                }
                else{
                     printquery(res);
                    PQclear(res);

                }

            }
            printf("\n");
        } while (operazione != 0);

        printf("Arrivederci!");

        PQfinish(conn);
    }

    return 0;
}