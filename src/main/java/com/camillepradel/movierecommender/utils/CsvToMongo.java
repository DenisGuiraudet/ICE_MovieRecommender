package com.camillepradel.movierecommender.utils;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Date;
import java.util.Arrays;

import org.bson.Document;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.MongoCredential;
import com.mongodb.MongoClientOptions;


public class CsvToMongo {
	
	static final String pathMatthieu = "/Users/matthieu/Git";
    static final String pathToCsvFiles = pathMatthieu + "/ICE_MovieRecommender/src/main/java/com/camillepradel/movierecommender/utils/";
    static final String usersCsvFile = pathToCsvFiles + "users.csv";
    static final String moviesCsvFile = pathToCsvFiles + "movies.csv";
    static final String genresCsvFile = pathToCsvFiles + "genres.csv";
    static final String movGenreCsvFile = pathToCsvFiles + "mov_genre.csv";
    static final String ratingsCsvFile = pathToCsvFiles + "ratings.csv";
    static final String friendsCsvFile = pathToCsvFiles + "friends.csv";
    static final String cvsSplitBy = ",";
	
	private static void commitUsers(MongoDatabase database) {
	    System.out.println(usersCsvFile);
	    database.getCollection("users").drop();
	    database.createCollection("users");
	    MongoCollection<Document> collectionUsers = database.getCollection("users");	    
	    try {
			BufferedReader br = new BufferedReader(new FileReader(usersCsvFile));
            try {
    			String line;
				br.readLine();
				while ((line = br.readLine()) != null) {
				    String[] values = line.split(cvsSplitBy);
				    Document person = new Document("id", Integer.parseInt(values[0]))
				            .append("age", Integer.parseInt(values[1]))
				            .append("sex", values[2])
				            .append("occupation", values[3])
				            .append("zip", values[4]);
				    collectionUsers.insertOne(person);
				}
				System.out.println("done");
			} catch (IOException e1) {
				e1.printStackTrace();
			}

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}
	
	private static void commitMovies(MongoDatabase database) {
	    System.out.println(moviesCsvFile);
	    database.getCollection("movies").drop();
	    database.createCollection("movies");
	    MongoCollection<Document> collectionMovie = database.getCollection("movies");	    
	    try {
			BufferedReader br = new BufferedReader(new FileReader(moviesCsvFile));
            try {
    			String line;
				br.readLine();
				while ((line = br.readLine()) != null) {
				    String[] values = line.split(cvsSplitBy);
				    int movieId = Integer.parseInt(values[0]);
                    String title = String.join(",", Arrays.copyOfRange(values, 1, values.length - 1));
                    Date date = new Date(Long.parseLong(values[values.length - 1]) * 1000);
				    Document movie = new Document("id", movieId)
				            .append("title", title)
				            .append("date", date);
				    collectionMovie.insertOne(movie);
				}
				System.out.println("done");
			} catch (IOException e1) {
				e1.printStackTrace();
			}

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}
	
	private static void commitGenre(MongoDatabase database) {
	    System.out.println(genresCsvFile);
	    database.getCollection("genres").drop();
	    database.createCollection("genres");
	    MongoCollection<Document> collectionGenre = database.getCollection("genres");	    
	    try {
			BufferedReader br = new BufferedReader(new FileReader(genresCsvFile));
            try {
    			String line;
				br.readLine();
				while ((line = br.readLine()) != null) {
				    String[] values = line.split(cvsSplitBy);
				    String name = values[0];
                    int genreId = Integer.parseInt(values[1]);
				    Document genre = new Document("name", name)
				            .append("id", genreId);
				    collectionGenre.insertOne(genre);
				}
				System.out.println("done");
			} catch (IOException e1) {
				e1.printStackTrace();
			}

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}
	
	private static void commitMovieGenre(MongoDatabase database) {
	    System.out.println(movGenreCsvFile);
	    database.getCollection("movie_genre").drop();
	    database.createCollection("movie_genre");
	    MongoCollection<Document> collectionMovieGenre = database.getCollection("movie_genre");	    
	    try {
			BufferedReader br = new BufferedReader(new FileReader(movGenreCsvFile));
            try {
    			String line;
				br.readLine();
				while ((line = br.readLine()) != null) {
				    String[] values = line.split(cvsSplitBy);
				    int movieId = Integer.parseInt(values[0]);
                    int genreId = Integer.parseInt(values[1]);
				    Document movieGenre = new Document("movie_id", movieId)
				            .append("genre_id", genreId);
				    collectionMovieGenre.insertOne(movieGenre);
				}
				System.out.println("done");
			} catch (IOException e1) {
				e1.printStackTrace();
			}

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}
	
	private static void commitRatings(MongoDatabase database) {
	    System.out.println(ratingsCsvFile);
	    database.getCollection("ratings").drop();
	    database.createCollection("ratings");
	    MongoCollection<Document> collectionMovieGenre = database.getCollection("ratings");	    
	    try {
			BufferedReader br = new BufferedReader(new FileReader(ratingsCsvFile));
            try {
    			String line;
				br.readLine();
				while ((line = br.readLine()) != null) {
				    String[] values = line.split(cvsSplitBy);
				    int userId = Integer.parseInt(values[0]);
                    int movieId = Integer.parseInt(values[1]);
                    int ratingValue = Integer.parseInt(values[2]);
                    Date date = new Date(Long.parseLong(values[3]) * 1000);
				    Document movieGenre = new Document("user_id", userId)
				            .append("movie_id", movieId)
				            .append("rating", ratingValue)
				            .append("date", date);
				    collectionMovieGenre.insertOne(movieGenre);
				}
				System.out.println("done");
			} catch (IOException e1) {
				e1.printStackTrace();
			}

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}
	
	private static void commitFriends(MongoDatabase database) {
	    System.out.println(friendsCsvFile);
	    database.getCollection("friends").drop();
	    database.createCollection("friends");
	    MongoCollection<Document> collectionMovieGenre = database.getCollection("friends");	    
	    try {
			BufferedReader br = new BufferedReader(new FileReader(friendsCsvFile));
            try {
    			String line;
				br.readLine();
				while ((line = br.readLine()) != null) {
				    String[] values = line.split(cvsSplitBy);
                    int user1Id = Integer.parseInt(values[0]);
                    int user2Id = Integer.parseInt(values[1]);
				    Document friends = new Document("user1_id", user1Id)
				            .append("user2_id", user2Id);
				    collectionMovieGenre.insertOne(friends);
				}
				System.out.println("done");
			} catch (IOException e1) {
				e1.printStackTrace();
			}

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}



	public static void main(String[] args) {
		MongoClient mongoClient = new MongoClient(new MongoClientURI("mongodb://localhost:27017"));
		MongoDatabase database = mongoClient.getDatabase("mongo");
		commitUsers(database);
		commitMovies(database);
		commitGenre(database);
		commitMovieGenre(database);
		commitRatings(database);
		commitFriends(database);
	}

}
