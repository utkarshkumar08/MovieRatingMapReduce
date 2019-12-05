# MovieRatingMapReduce
MR Jobs to list out highest rated movie in each genre from Movielens dataset. 
Dataset Used - https://grouplens.org/datasets/movielens/

Our proposed solution architecture uses 3 MR jobs which are connected in a chain and executed on a Hadoop cluster with 1 master node and 4 slave nodes. Also, executed these MR jobs on AWS Elastic Map Reduce service.

First Job:
	2 Mapping Functions and 1 Reducer function
	Mapping Function 1:
		Input : Ratings.csv
		Key - Value pair : MovieID - Rating
	Mapping Function 2:
		Input : Movies.csv
		Key - Value pair : MovieID - Title,Genre LIst
	Reducer Function:
		Output Key - Value pair : MovieID - Title,Average Rating,Genre List

Second Job:
	1 Mapping function and 1 Reducer function 
	Mapping Function :
		Input : Output from the first job. 
		Performs data cleanup by removing NaN values from Average rating value 
Reducer Function :
	A movie can have multiple Genres. This function splits the Genre field
	Output Key - Value pair : MovieID - Title,Average Rating,Genre

Third Job:
	1 Mapping function and 1 Reducer function
	Mapping function:
		Input : Output from second job
		Output Key - Value pair : Genre - Title, Average Rating
Reducer function:
Calculates the Highest rated movie from each genre.
Output Key - Value pair : Genre - Title of Highest rated movie, Rating 


