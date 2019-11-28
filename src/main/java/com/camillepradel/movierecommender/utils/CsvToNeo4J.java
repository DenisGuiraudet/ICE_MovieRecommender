package com.camillepradel.movierecommender.utils;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;

import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Session;

import static org.neo4j.driver.v1.Values.parameters;

public class CsvToNeo4J {
	
	static final String pathMatthieu = "/Users/matthieu/Git";
	static final String pathDenis = "/home/denis/repo";
    static final String pathToCsvFiles = pathDenis + "/ICE_MovieRecommender/src/main/java/com/camillepradel/movierecommender/utils/";
    static final String usersCsvFile = pathToCsvFiles + "users.csv";
    static final String moviesCsvFile = pathToCsvFiles + "movies.csv";
    static final String genresCsvFile = pathToCsvFiles + "genres.csv";
    static final String movGenreCsvFile = pathToCsvFiles + "mov_genre.csv";
    static final String ratingsCsvFile = pathToCsvFiles + "ratings.csv";
    static final String friendsCsvFile = pathToCsvFiles + "friends.csv";
    static final String cvsSplitBy = ",";
    
    private static Driver driver;

    private static void commitUsers(Session session) {
        System.out.println(usersCsvFile);

        try (BufferedReader br = new BufferedReader(new FileReader(usersCsvFile))) {

        	String line;
            br.readLine(); // skip first line
            while ((line = br.readLine()) != null) {
                String[] values = line.split(cvsSplitBy);
                
                session.run(
            		"CREATE (u:User {"
        	    		+ "id: $id,"
        	    		+ "age: $age,"
        	    		+ "sex: $sex,"
        	    		+ "occupation: $occupation,"
        	    		+ "zip: $zip"
        	    		+ "})",
            		parameters(
        				"id", Integer.parseInt(values[0]),
        				"age", Integer.parseInt(values[1]),
        				"sex", values[2],
        				"occupation", values[3],
        				"zip", values[4]
        			)
        		);
            }
            
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void commitMovies(Session session) {
        System.out.println(moviesCsvFile);
        
        try (BufferedReader br = new BufferedReader(new FileReader(moviesCsvFile))) {

        	String line;
            br.readLine(); // skip first line
            while ((line = br.readLine()) != null) {
                String[] values = line.split(cvsSplitBy);
                
                int movieId = Integer.parseInt(values[0]);
                String title = String.join(",", Arrays.copyOfRange(values, 1, values.length - 1));
                String date = values[values.length - 1];
                
                session.run(
            		"CREATE (m:Movie {"
        	    		+ "id: $id,"
        	    		+ "title: $title,"
        	    		+ "date: $date"
        	    		+ "})",
            		parameters(
        				"id", movieId,
        				"title", title,
        				"date", date
        			)
        		);
            }
            
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void commitGenres(Session session) {
        System.out.println(genresCsvFile);
        
        try (BufferedReader br = new BufferedReader(new FileReader(genresCsvFile))) {

        	String line;
            br.readLine(); // skip first line
            while ((line = br.readLine()) != null) {
                String[] values = line.split(cvsSplitBy);

                String name = values[0];
                int genreId = Integer.parseInt(values[1]);
                
                session.run(
            		"CREATE (g:Genre {"
        	    		+ "name: $name,"
        	    		+ "id: $id"
        	    		+ "})",
            		parameters(
        				"name", name,
        				"id", genreId
        			)
        		);
            }
            
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void commitMovieGenre(Session session) {
        System.out.println(movGenreCsvFile);
        
        try (BufferedReader br = new BufferedReader(new FileReader(movGenreCsvFile))) {

        	String line;
            br.readLine(); // skip first line
            while ((line = br.readLine()) != null) {
                String[] values = line.split(cvsSplitBy);

                int movieId = Integer.parseInt(values[0]);
                int genreId = Integer.parseInt(values[1]);
                
                session.run(
            		"MATCH (m:Movie),(g:Genre) " + 
            		"WHERE m.id = $movie_id AND g.id = $genre_id " + 
            		"CREATE (m)-[mv:MovieGenre]->(g)",
            		parameters(
        				"movie_id", movieId,
        				"genre_id", genreId
        			)
        		);
            }
            
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void commitRatings(Session session)  {
        System.out.println(ratingsCsvFile);
        
        try (BufferedReader br = new BufferedReader(new FileReader(ratingsCsvFile))) {

        	String line;
            br.readLine(); // skip first line
            while ((line = br.readLine()) != null) {
                String[] values = line.split(cvsSplitBy);
                
                int userId = Integer.parseInt(values[0]);
                int movieId = Integer.parseInt(values[1]);
                int ratingValue = Integer.parseInt(values[2]);
                String date = values[3];
                
                session.run(
            		"MATCH (u:User),(m:Movie) " + 
            		"WHERE u.id = $user_id AND m.id = $movie_id " + 
            		"CREATE (u)-[r:Rating {"
            		+ "rating: $rating_value,"
            		+ "date: $date"
            		+ "}]->(m)",
            		parameters(
        				"user_id", userId,
        				"movie_id", movieId,
        				"rating_value", ratingValue,
        				"date", date
        			)
        		);
            }
            
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void commitFriends(Session session) {
        System.out.println(friendsCsvFile);
        
        try (BufferedReader br = new BufferedReader(new FileReader(friendsCsvFile))) {

        	String line;
            br.readLine(); // skip first line
            while ((line = br.readLine()) != null) {
                String[] values = line.split(cvsSplitBy);

                int user1Id = Integer.parseInt(values[0]);
                int user2Id = Integer.parseInt(values[1]);
                
                session.run(
            		"MATCH (u1:user),(u2:User) " + 
            		"WHERE u1.id = $u1_id AND u2.id = $u2_id " + 
            		"CREATE (u1)-[f:Friend]->(u2)",
            		parameters(
        				"u1_id", user1Id,
        				"u2_id", user2Id
        			)
        		);
            }
            
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        // load JDBC driver
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

        // db connection info
        String url = "bolt://localhost:7687";
        String login = "neo4j";
        String password = "password";
        
        try {
        	driver = GraphDatabase.driver( url, AuthTokens.basic( login, password ) );
        	Session session = driver.session();
        	
        	// clean database
        	session.run(
        		"MATCH (n) " + 
        		"DETACH DELETE n"
    		);
            
        	// commit in database
            commitUsers(session);
            commitMovies(session);
            commitGenres(session);
            commitMovieGenre(session);
            commitRatings(session);
            commitFriends(session);
        	
		} catch (Exception e) {
			System.out.println(e);
			driver.close();
		}

        System.out.println("done");
    }

}
