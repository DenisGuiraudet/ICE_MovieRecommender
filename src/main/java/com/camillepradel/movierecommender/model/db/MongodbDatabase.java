package com.camillepradel.movierecommender.model.db;

import com.camillepradel.movierecommender.model.Genre;
import com.camillepradel.movierecommender.model.Movie;
import com.camillepradel.movierecommender.model.Rating;
import com.mongodb.Block;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Sorts;
import static com.mongodb.client.model.Filters.*;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import org.bson.Document;

public class MongodbDatabase extends AbstractDatabase {

    @Override
    public List<Movie> getAllMovies() {
    	MongoClient mongoClient = new MongoClient();
    	MongoDatabase database = mongoClient.getDatabase("mongo");
    	MongoCollection<Document> collection = database.getCollection("movies");
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
    	MongoClient mongoClient = new MongoClient();
    	MongoDatabase database = mongoClient.getDatabase("mongo");
    	MongoCollection<Document> collection = database.getCollection("movie_genre");
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
    	MongoClient mongoClient = new MongoClient();
    	MongoDatabase database = mongoClient.getDatabase("mongo");
    	
    	MongoCollection<Document> collectionRating = database.getCollection("ratings");
    	collectionRating.find(eq("user_id", userId)).forEach((Block<Document>) userRating -> {
    		System.out.println(Integer.parseInt(userRating.get("rating").toString()));
    		int movieID = Integer.parseInt(userRating.get("movie_id").toString());
    		MongoCollection<Document> collectionMovie = database.getCollection("movies");

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
        Genre genre0 = new Genre(0, "genre0");
        Genre genre1 = new Genre(1, "genre1");
        ratings.add(new Rating(new Movie(0, "Titre 0", Arrays.asList(new Genre[]{genre0, genre1})), userId, 3));
        ratings.add(new Rating(new Movie(2, "Titre 2", Arrays.asList(new Genre[]{genre1})), userId, 4));
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
