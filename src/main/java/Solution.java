public class Solution {

    private final GraphDatabase graphDatabase = GraphDatabase.createDatabase();

    public void databaseStatistics() {
        System.out.println(graphDatabase.runCypher("CALL db.labels()"));
        System.out.println(graphDatabase.runCypher("CALL db.relationshipTypes()"));
    }

    public void runAllTests() {
        System.out.println(findActorByName("Emma Watson"));
//        System.out.println(findMovieByTitleLike("Star Wars"));
//        System.out.println(findRatedMoviesForUser("maheshksp"));
//        System.out.println(findCommonMoviesForActors("Emma Watson", "Daniel Radcliffe"));
//        System.out.println(findMovieRecommendationForUser("emileifrem"));
//        addActor("Dakota Johnson");
//        addMovie("50 Shades Of Grey");
//        addActsInRelation("Dakota Johnson", "50 Shades Of Grey");
//        System.out.println(testFindMoviesActorPlayedIn("Dakota Johnson"));
//        System.out.println(testFindActorsWhoPlayedInMovie("50 Shades Of Grey"));
//        setPropertiesForActor("Dakota Johnson", "4.10.1989", "Austin");
//        System.out.println(findActorByName("Dakota Johnson"));
//        System.out.println(findActorsWhoPlayedInAtLeastSixMovies());
//        System.out.println(countAvgAppearancesForActorsWhoPlayedInAtLeast7Movies());
//        System.out.println(findActorsWhoWereDirectors());
//        System.out.println(findFriendsWhoRatedFilmForUser("adilfulara"));
//        System.out.println(findPathsBetweenActors("Kevin Bacon", "Angelina Jolie"));
//        System.out.println(findShortestPath("Kevin Bacon", "Angelina Jolie"));
    }

    private String findActorByName(final String actorName) {
        return graphDatabase.runCypher(String.format(
                "MATCH (a:Actor {name: '%s'}) RETURN a", actorName));
    }

    private String findMovieByTitleLike(final String movieName) {
        return graphDatabase.runCypher(String.format(
                "MATCH (m:Movie) " +
                        "WHERE m.title CONTAINS '%s' RETURN m.title",
                movieName));
    }

    private String findRatedMoviesForUser(final String userLogin) {
        return graphDatabase.runCypher(String.format(
                "MATCH (m:Movie)<-[:RATED]-(u:User {login: '%s'}) " +
                        "RETURN m.title",
                userLogin));
    }

    private String findCommonMoviesForActors(String actorOne, String actorTwo) {
        return graphDatabase.runCypher(String.format(
                "MATCH (a:Actor {name: '%s'})-[:ACTS_IN]->" +
                        "(m:Movie)<-[:ACTS_IN]-(b:Actor {name: '%s'}) " +
                        "RETURN m.title",
                actorOne, actorTwo));
    }

    private String findMovieRecommendationForUser(final String userLogin) {
        return graphDatabase.runCypher(String.format(
                "MATCH (u:User {login: '%s'})-[:RATED]->(m1:Movie)<-[:ACTS_IN]-(a:Actor)-[:ACTS_IN]-(m2:Movie) " +
                        "WHERE m1<>m2 RETURN distinct m2.title LIMIT 10",
                userLogin));
    }


    // stworzyc dwa nowe wezly reprezentujace film oraz aktora, nastepnie stworzyc relacje ich łacząca (np. ACTS_IN)
    private void addActor(final String actorName) {
        graphDatabase.runCypher(String.format(
                "CREATE (a:Actor {name: '%s'})",
                actorName));
    }

    private void addMovie(final String movieName) {
        graphDatabase.runCypher(String.format(
                "CREATE (m:Movie {title: '%s'})",
                movieName));
    }

    private void addActsInRelation(final String actorName, final String movieName) {
        graphDatabase.runCypher(String.format(
                "MATCH (a:Actor {name: '%s'}),(m:Movie {title: '%s'})" +
                    "CREATE (a)-[:ACTS_IN]->(m)", actorName, movieName));
    }


    // ustawic zapytaniem pozostale wlasciwosci nowo dodanego wezla reprezentujacego aktora (np. birthplace, birthdate)
    private void setPropertiesForActor(final String actorName, final String birthDate, final String birthPlace) {
        graphDatabase.runCypher(String.format(
                "MATCH (a:Actor {name: '%s'})" +
                "SET a.birthdate = '%s', a.birthplace = '%s'",
                actorName, birthDate, birthPlace));
    }


    // zapytanie o aktorow ktorzy grali w co najmniej 6 filmach (użyć collect i length)
    private String findActorsWhoPlayedInAtLeastSixMovies() {
        return graphDatabase.runCypher(
                "MATCH (a:Actor)-[:ACTS_IN]->(m:Movie) " +
                        "WITH collect(m) AS movies, a " +
                        "WHERE length(movies) >= 6 RETURN a.name");
    }


    // policzyc srednia wystapien w filmach dla grupy aktorow, ktorzy wystapili w co najmniej 7 filmach
    private String countAvgAppearancesForActorsWhoPlayedInAtLeast7Movies() {
        return graphDatabase.runCypher(
                "MATCH (a:Actor)-[:ACTS_IN]->(m:Movie) " +
                        "WITH collect(m) AS movies, a " +
                        "WHERE length(movies) >= 7 RETURN avg(length(movies))");
    }


    // wyswietlic aktorow, ktorzy zagrali w co najmniej pieciu filmach i wyrezyserowali co najmniej
    // jeden film (z uzyciem WITH), posortowac ich wg liczby wystapien w filmach
    private String findActorsWhoWereDirectors() {
        return graphDatabase.runCypher(
                "MATCH (m2:Movie)<-[:DIRECTED]-(a:Actor)-[:ACTS_IN]->(m1:Movie)" +
                        "WITH collect(m1) AS movies_played, a " +
                        "WHERE length(movies_played) >= 5 RETURN a.name " +
                        "ORDER BY length(movies_played)");
    }


    // zapytanie o znajomych wybranego usera ktorzy ocenili film na co najmniej trzy gwiazdki
    // (wyswietlic znajomego, tytul, liczbe gwiazdek)
    private String findFriendsWhoRatedFilmForUser(final String userLogin) {
        return graphDatabase.runCypher(String.format(
                "MATCH (m:Movie)<-[r:RATED]-(f:User)<-[:FRIEND]-(u:User {login: '%s'}) " +
                        "WHERE r.stars >= 3 " +
                        "RETURN f.login, m.title, r.stars",
                userLogin));
    }


    // zapytanie o sciezki miedzy wybranymi aktorami (np.2), w sciezkach maja nie znajdowac sie filmy
    private String findPathsBetweenActors(String actorName1, String actorName2) {
        return graphDatabase.runCypher(String.format(
                "MATCH paths=(a1:Actor {name: '%s'})-[*..5]-(a2:Actor {name: '%s'}) " +
                        "WITH a1, a2, FILTER(n in NODES(paths) WHERE NOT (n:Movie)) AS p " +
                        "RETURN p", actorName1, actorName2));

    }


    // porownac czas wykonania zapytania o wybranego aktora bez oraz z indeksem w bazie
    // nalozonym na atrybut name, dokonac porównania dla zapytania shortestPath pomiedzy dwoma wybranymi aktorami
    private String findShortestPath(String actorName1, String actorName2) {
        return graphDatabase.runCypher(String.format(
                "PROFILE MATCH path=shortestPath((a1:Actor {name: '%s'})-[*]-(a2:Actor {name: '%s'})) " +
                        "RETURN path",
                actorName1, actorName2));
    }


    private String testFindMoviesActorPlayedIn(String actorName) {
        return graphDatabase.runCypher(String.format(
                "MATCH (a:Actor {name: '%s'})-[:ACTS_IN]->(m:Movie) " +
                        "RETURN m.title", actorName));
    }

    private String testFindActorsWhoPlayedInMovie(String movieName) {
        return graphDatabase.runCypher(String.format(
                "MATCH (a:Actor)-[:ACTS_IN]->(m:Movie {title: '%s'}) " +
                        "RETURN a.name", movieName));
    }

    public String createIndexOnActorName(){
        return graphDatabase.runCypher("CREATE INDEX ON :Actor(name)");
    }

    public String dropIndexOnActorName(){
        return graphDatabase.runCypher("DROP INDEX ON :Actor(name)");
    }
}
