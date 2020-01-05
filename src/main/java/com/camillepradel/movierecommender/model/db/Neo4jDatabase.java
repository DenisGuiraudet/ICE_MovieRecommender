package com.camillepradel.movierecommender.model.db;

import com.camillepradel.movierecommender.model.Genre;
import com.camillepradel.movierecommender.model.Movie;
import com.camillepradel.movierecommender.model.Rating;

import static org.neo4j.driver.v1.Values.parameters;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.types.Node;
import org.neo4j.driver.v1.Record;

public class Neo4jDatabase extends AbstractDatabase {

	Driver driver;
	Session session;
	
	// db connection info
    String url = "bolt://localhost:7687" ;
    String login = "neo4j";
    String password = "password";
	
	public Neo4jDatabase() {
		try {
        	driver = GraphDatabase.driver( url, AuthTokens.basic( login, password ) );
        	session = driver.session();
        	
		} catch (Exception e) {
			System.out.println(e);
			driver.close();
		}
	}

    @Override
    public List<Movie> getAllMovies() {
        List<Movie> movies = new LinkedList<Movie>();
        
        StatementResult result = session.run(
    		"MATCH (m:Movie)-[mv:MovieGenre]->(g:Genre)"
    		+ " RETURN m, Collect(g);"
		);
        
        while (result.hasNext())
        {
            Record record = result.next();
            Node m = record.get("m").asNode();
            List<Object> gList = record.get("g").asList();

            List<Genre> genres = new LinkedList<Genre>();
            for(Object g : gList){
                Node g = ((Node)g);
            	genres.add(
        			new Genre(
                        g.get("id").asInt(),
                        g.get("name").asString(),
					)
    			);
        	}
            
            movies.add(
        		new Movie(
    				m.get("id").asInt(),
    				m.get("name").asString(),
    				genres
				)
    		);
        }
        return movies;
    }

    @Override
    public List<Movie> getMoviesRatedByUser(int userId) {
        List<Movie> movies = new LinkedList<Movie>();
    	
    	StatementResult result = session.run(
    		"MATCH (u:User)-[r:Rating]->(m:Movie)"
			+ " WHERE u.id = $user_id"
    		+ " RETURN m;",
    		parameters(
				"user_id", userId
			)
		);

        while (result.hasNext())
        {
            Record record = result.next();
            Node m = record.get("m").asNode();

            movies.add(
        		new Movie(
    				m.get("id").asInt(),
    				m.get("name").asString(),
    				Arrays.asList(new Genre[]{})
				)
    		);
        }
        return movies;
    }

    @Override
    public List<Rating> getRatingsFromUser(int userId) {
        List<Rating> ratings = new LinkedList<Rating>();
    	
    	StatementResult result = session.run(
    		"MATCH (u:User)-[r:Rating]->(m:Movie)"
			+ " WHERE u.id = $user_id"
    		+ " RETURN u, r, m;",
    		parameters(
				"user_id", userId
			)
		);

        while (result.hasNext())
        {
            Record record = result.next();
            Node u = record.get("u").asNode();
            Node r = record.get("r").asNode();
            Node m = record.get("m").asNode();

            ratings.add(
        		new Rating(
        			new Movie(
                        m.get("id").asInt(),
                        m.get("name").asString(),
    				    Arrays.asList(new Genre[]{})
					),
    				u.get("id").asInt(),
    				r.get("rating").asInt()
				)
    		);
        }
        return ratings;
    }

    @Override
    public void addOrUpdateRating(Rating rating) {
        StatementResult result = session.run(
    		"MATCH (u:User { id: $user_id })-[r:Rating]->(m:Movie { id: $movie_id })"
    		+ " RETURN r;",
    		parameters(
				"user_id", rating.userId,
                "movie_id", rating.movie.id
			)
		);

        long ts = System.currentTimeMillis() / 1000L;
        Date date = new Date(ts);

        if (result.hasNext()) {
            // EXISTS
            session.run(
                "MATCH (u:User { id: $user_id })-[r:Rating]->(m:Movie { id: $movie_id })"
                + " SET r.rating = $rating_value, r.date = $date;",
                parameters(
                    "user_id", rating.userId,
                    "movie_id", rating.movie.id,
                    "rating_value", rating.score,
                    "date", date.toString()
                )
            );
        } else {
            // DOESN'T EXISTS
            session.run(
                "MATCH (u:User),(m:Movie) " + 
                "WHERE u.id = $user_id AND m.id = $movie_id " + 
                "CREATE (u)-[r:Rating {"
                + "rating: $rating_value,"
                + "date: $date"
                + "}]->(m)",
                parameters(
                    "user_id", rating.userId,
                    "movie_id", rating.movie.id,
                    "rating_value", rating.score,
                    "date", date.toString()
                )
            );
        }
        
    }

    @Override
    public List<Rating> processRecommendationsForUser(int userId, int processingMode) {
        List<Rating> ratings = new LinkedList<Rating>();

        switch(processingMode) {
            case 1:
                StatementResult result = session.run(
                    "MATCH (target_user:User { id : $user_id })-[:Rating]->(m:Movie) <-[:Rating]-(other_user:User) " +
                    "WITH other_user, count(distinct m.title) AS num_common_movies, target_user " +
                    "ORDER BY num_common_movies DESC " +
                    "LIMIT 1 " +
                    "MATCH other_user-[rat_other_user:Rating]->(m2:Movie) " +
                    "WHERE NOT (target_user-[:Rating]->m2) " +
                    "RETURN m2 AS movie, " +
                    "rat_other_user AS rating, " +
                    "other_user.id AS user_id " +
                    "ORDER BY rat_other_user.note DESC",
                    parameters(
                        "user_id", userId
                    )
                );

                while (result.hasNext())
                {
                    Record record = result.next();
                    Node r = record.get("rating").asNode();
                    Node m = record.get("movie").asNode();

                    ratings.add(
                        new Rating(
                            new Movie(
                                m.get("id").asInt(),
                                m.get("name").asString(),
                                Arrays.asList(new Genre[]{})
                            ),
                            record.get("user_id").asInt(),
                            r.get("rating").asInt()
                        )
                    );
                }
                return ratings;
                break;
            case 2:
                StatementResult result = session.run(
                    "MATCH (target_user:User { id : $user_id })-[:Rating]->(m:Movie) <-[:Rating]-(other_user:User) " +
                    "WITH other_user, count(distinct m.title) AS num_common_movies, target_user " +
                    "ORDER BY num_common_movies DESC " +
                    "LIMIT 5 " +
                    "MATCH other_user-[rat_other_user:Rating]->(m2:Movie) " +
                    "WHERE NOT (target_user-[:Rating]->m2) " +
                    "RETURN m2 AS movie, " +
                    "rat_other_user AS rating, " +
                    "other_user.id AS user_id " +
                    "ORDER BY rat_other_user.note DESC",
                    parameters(
                        "user_id", userId
                    )
                );

                while (result.hasNext())
                {
                    Record record = result.next();
                    Node r = record.get("rating").asNode();
                    Node m = record.get("movie").asNode();

                    ratings.add(
                        new Rating(
                            new Movie(
                                m.get("id").asInt(),
                                m.get("name").asString(),
                                Arrays.asList(new Genre[]{})
                            ),
                            record.get("user_id").asInt(),
                            r.get("rating").asInt()
                        )
                    );
                }
                return ratings;
                break;
            case 3:
                break;
            default:
                return ratings;
                break;
        }
    }
}
