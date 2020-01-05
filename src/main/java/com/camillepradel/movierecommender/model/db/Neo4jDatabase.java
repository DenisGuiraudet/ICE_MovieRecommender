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
        // TODO: add query which
        //         - add rating between specified user and movie if it doesn't exist
        //         - update it if it does exist
    }

    @Override
    public List<Rating> processRecommendationsForUser(int userId, int processingMode) {
        // TODO: process recommendations for specified user exploiting other users ratings
        //       use different methods depending on processingMode parameter
        Genre genre0 = new Genre(0, "genre0");
        Genre genre1 = new Genre(1, "genre1");
        Genre genre2 = new Genre(2, "genre2");
        List<Rating> recommendations = new LinkedList<Rating>();
        String titlePrefix;
        if (processingMode == 0) {
            titlePrefix = "0_";
        } else if (processingMode == 1) {
            titlePrefix = "1_";
        } else if (processingMode == 2) {
            titlePrefix = "2_";
        } else {
            titlePrefix = "default_";
        }
        recommendations.add(new Rating(new Movie(0, titlePrefix + "Titre 0", Arrays.asList(new Genre[]{genre0, genre1})), userId, 5));
        recommendations.add(new Rating(new Movie(1, titlePrefix + "Titre 1", Arrays.asList(new Genre[]{genre0, genre2})), userId, 5));
        recommendations.add(new Rating(new Movie(2, titlePrefix + "Titre 2", Arrays.asList(new Genre[]{genre1})), userId, 4));
        recommendations.add(new Rating(new Movie(3, titlePrefix + "Titre 3", Arrays.asList(new Genre[]{genre0, genre1, genre2})), userId, 3));
        return recommendations;
    }
}
