import pandas as pd
import logging

# Define constants
COLUMN_ID = "id"
COLUMN_VOTE_AVERAGE = "vote_average"
COLUMN_GENRES = "genres"
COLUMN_RELEASE_DATE = "release_date"


class MovieDataSet:
    """
    MovieDataSet wraps a pandas DataFrame with additional functionality.

    This class wraps a pandas DataFrame and adds custom methods to enhance its functionality.
    """

    def __init__(self, filepath_or_buffer):
        """
        Initializes MovieDataSet object.

        Parameters:
            filepath_or_buffer: File path or object to read.
        """
        try:
            self.df = pd.read_csv(filepath_or_buffer)
        except FileNotFoundError as e:
            logging.error(f"File not found: {e.filename}")
            raise
        except PermissionError as e:
            logging.error(f"Permission denied: {e.filename}")
            raise
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)
        self.logger.addHandler(logging.StreamHandler())

    def _log_error_and_return(self, message):
        """
        Log error message and return None.

        Parameters:
            message (str): Error message to log.
        """
        self.logger.error(message)
        return None

    def get_unique_movies_count(self, column=COLUMN_ID):
        """
        Count the number of unique movies in the dataset.

        Parameters:
            column (str, optional): The column containing the unique identifier for each movie.
                Defaults to 'id'.
        """
        if column not in self.df.columns:
            return self._log_error_and_return(f"'{column}' column not found.")

        unique_movies = self.df[column].nunique()
        self.logger.info(f"Number of unique movies: {unique_movies}")
        return unique_movies

    def get_average_rating_by_column(self, column=COLUMN_VOTE_AVERAGE):
        """
        Calculate the average rating of all the movies.

        Parameters:
            column (str, optional): The column containing the ratings for each movie.
                Defaults to 'vote_average'.
        """
        if column not in self.df.columns:
            return self._log_error_and_return(f"Error: '{column}' column not found.")

        average = self.df[column].mean()
        self.logger.info(f"Average rating for all movies, based on the '{
                         column}': {average:.2f}")
        return average

    def get_movies_per_year(self, released_column=COLUMN_RELEASE_DATE):
        """
        Print the number of movies released in each year.

        Parameters:
            released_column (str, optional): Name of the column which contains the information for movie release dates.
                Defaults to 'release_date'.
        """
        if released_column not in self.df.columns:
            return self._log_error_and_return(f"Error: '{released_column}' column not found.")

        # Extract year from release_date column
        self.df['release_year'] = pd.to_datetime(
            self.df[released_column]).dt.year

        # Count occurrences of films for each year
        films_per_year = self.df['release_year'].value_counts().sort_index()

        # Print the number of films released in each year
        self.logger.info(f"Number of films released in each year:\n{
                         films_per_year}")
        return films_per_year

    def save_json(self, file_path, orient="records"):
        """
        Save the DataFrame to a JSON file.

        Parameters:
            file_path (str): The path to the JSON file to save.
            orient (str, optional): The format of the JSON file.
                Defaults to 'records'.
        """
        try:
            self.df.to_json(file_path, orient=orient)
            self.logger.info(f"File {file_path} successfully written.")
        except Exception as e:
            self.logger.error(f"Error writing to file: {e}")
            raise
