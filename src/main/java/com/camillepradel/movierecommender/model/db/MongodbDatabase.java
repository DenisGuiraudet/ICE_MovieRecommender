package com.camillepradel.movierecommender.model.db;

import com.camillepradel.movierecommender.model.Genre;
import com.camillepradel.movierecommender.model.Movie;
import com.camillepradel.movierecommender.model.Rating;
import com.mongodb.Block;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Sorts;
import com.mongodb.client.model.UpdateOptions;

import static com.mongodb.client.model.Filters.*;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;

import org.bson.Document;

public class MongodbDatabase extends AbstractDatabase {
	
	private MongoDatabase database;
	
	public MongodbDatabase() {
		MongoClient mongoClient = new MongoClient();
    	this.database = mongoClient.getDatabase("mongo");
	}

    @Override
    public List<Movie> getAllMovies() {
    	MongoCollection<Document> collection = this.database.getCollection("movies");
        List<Movie> movies = new LinkedList<Movie>();

    	collection.find().forEach((Block<Document>) actualMovie -> {
    		int id = Integer.parseInt(actualMovie.get("id").toString());
        	String titre = actualMovie.get("title").toString();      	
        	List<Genre> genresMovie = getListeGenre(id);
        	
        	movies.add(new Movie(id,titre,genresMovie));
    	});
        return movies;
    }
    
    public List<Genre> getListeGenre(int idMovieSearched) {
    	MongoCollection<Document> collection = this.database.getCollection("movie_genre");
		List<Genre> genres = new LinkedList<Genre>();
    	collection.find(eq("movie_id", idMovieSearched)).forEach((Block<Document>) movieGenreArray -> {

    		int idGenre = Integer.parseInt(movieGenreArray.get("genre_id").toString());
        	
    		MongoCollection<Document> genreCollection = database.getCollection("genres");
        	genreCollection.find(eq("id", idGenre)).forEach((Block<Document>) actualGenre -> {
        		String name = actualGenre.get("name").toString();
                int genreId = Integer.parseInt(actualGenre.get("id").toString());
            	
                genres.add(new Genre(genreId, name));
        	});
    	});
    		
        
        return genres;
    }

    @Override
    public List<Movie> getMoviesRatedByUser(int userId) {
        // TODO: write query to retrieve all movies rated by user with id userId
    	List<Movie> movies = new LinkedList<Movie>();
    	
    	MongoCollection<Document> collectionRating = this.database.getCollection("ratings");
    	collectionRating.find(eq("user_id", userId)).forEach((Block<Document>) userRating -> {
    		int movieID = Integer.parseInt(userRating.get("movie_id").toString());
    		MongoCollection<Document> collectionMovie = this.database.getCollection("movies");

    		collectionMovie.find(eq("id", movieID)).forEach((Block<Document>) actualMovie -> {
        		int id = Integer.parseInt(actualMovie.get("id").toString());
            	String titre = actualMovie.get("title").toString();      	
            	System.out.println(titre);
            	List<Genre> genresMovie = getListeGenre(id);
            	
            	movies.add(new Movie(id,titre,genresMovie));
        	});

    	});
        return movies;
    }
    

    @Override
    public List<Rating> getRatingsFromUser(int userId) {
        // TODO: write query to retrieve all ratings from user with id userId
        List<Rating> ratings = new LinkedList<Rating>();
    	
    	MongoCollection<Document> collectionRating = this.database.getCollection("ratings");
    	collectionRating.find(eq("user_id", userId)).forEach((Block<Document>) userRating -> {
    		int movieID = Integer.parseInt(userRating.get("movie_id").toString());
    		MongoCollection<Document> collectionMovie = this.database.getCollection("movies");

    		collectionMovie.find(eq("id", movieID)).forEach((Block<Document>) actualMovie -> {
        		int id = Integer.parseInt(actualMovie.get("id").toString());
            	String titre = actualMovie.get("title").toString();      	
            	System.out.println(titre);
            	List<Genre> genresMovie = getListeGenre(id);
            	int score = Integer.parseInt(userRating.get("rating").toString());
            	Movie userMovie = new Movie(id,titre,genresMovie);
            	ratings.add(new Rating(userMovie, userId, score));

        	});

    	});
        return ratings;
    }

    @Override
    public void addOrUpdateRating(Rating rating) {
	    MongoCollection<Document> collectionRating = this.database.getCollection("ratings");	    
	    int userId = rating.getUserId();
        int movieId = rating.getMovieId();
        int ratingValue = rating.getScore();
        long ts = System.currentTimeMillis() / 1000L;
        Date date = new Date(ts);
	    Document movieRating = new Document("user_id", userId)
	            .append("movie_id", movieId)
	            .append("rating", ratingValue)
	            .append("date", date);
	    UpdateOptions options = new UpdateOptions().upsert(true);
	    collectionRating.updateOne(Filters.and(eq("user_id", userId),eq("movie_id",movieId)),
	    		new Document("$set", movieRating),options);

	    // TODO: add query which
        //         - add rating between specified user and movie if it doesn't exist
        //         - update it if it does exist
    }

    @Override
    public List<Rating> processRecommendationsForUser(int userId, int processingMode) {
        // TODO: process recommendations for specified user exploiting other users ratings
        //       use different methods depending on processingMode parameter
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
        this.getUsersIdCloseToUser(2);
        return recommendations;
    }    
    
    public int getUsersIdCloseToUser(int userId) {
    	List<List<Rating>> closestUser = new LinkedList<List<Rating>>();
    	int nbrOfMovieInCommon = 0;
		List<Rating> ratingsUser = this.getRatingsFromUser(userId);
	    MongoCollection<Document> collectionRating = this.database.getCollection("ratings");	    
    	collectionRating.find().forEach((Block<Document>) userRating -> {
    		int actualUserId = Integer.parseInt(userRating.get("user_id").toString());
    		if(actualUserId != userId) {
    			int tempNbrMovieInCommon = 0;
    			List<Rating> ratingsActualUser = this.getRatingsFromUser(actualUserId);
    			for(int i = 0; i < ratingsActualUser.size(); i ++) {
    				//if()
    			}
    			
    		}
    	});
    	return 0;
    }
}
