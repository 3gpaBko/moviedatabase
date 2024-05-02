import pandas as pd
import numpy as np
import ast
import logging

# Define constants
COLUMN_ID = "id"
COLUMN_VOTE_AVERAGE = "vote_average"
COLUMN_GENRES = "genres"
COLUMN_RELEASE_DATE = "release_date"


class MovieDataSet(pd.DataFrame):
    """
    MovieDataSet extends a pandas DataFrame with additional functionality.

    This class extends a pandas DataFrame and adds custom methods to enhance its functionality.
    """

    def __init__(self, data=None, index=None, columns=None, filepath_or_buffer=None, low_memory=False, encoding="utf-8"):
        """
        Initializes MovieDataSet object.

        Parameters:
            data: Data as DataFrame, dict, or list of dicts.
            index: Index.
            columns: Columns.
            filepath_or_buffer: File path or object to read.
            low_memory (bool, optional): Whether to use low memory mode while reading the CSV file.
                Defaults to False.
            encoding (str, optional): The encoding of the file.
                Defaults to "utf-8".
        """
        if filepath_or_buffer:
            try:
                super().__init__(pd.read_csv(
                    filepath_or_buffer, low_memory=low_memory, encoding=encoding))
            except FileNotFoundError as e:
                logging.error(f"File not found: {e.filename}")
                raise
            except PermissionError as e:
                logging.error(f"Permission denied: {e.filename}")
                raise
        else:
            super().__init__(data=data, index=index, columns=columns)

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
        if column not in self.columns:
            return self._log_error_and_return(f"'{column}' column not found.")

        unique_movies = self[column].nunique()
        # self.logger.info(f"Number of unique movies: {unique_movies}")
        return unique_movies

    def get_average_rating_by_column(self, column=COLUMN_VOTE_AVERAGE):
        """
        Calculate the average rating of all the movies.

        Parameters:
            column (str, optional): The column containing the ratings for each movie.
                Defaults to 'vote_average'.
        """
        if column not in self.columns:
            return self._log_error_and_return(f"Error: '{column}' column not found.")

        average = self[column].mean()
        # self.logger.info(f"Average rating for all movies, based on the '{
        #                  column}': {average:.2f}")
        return average

    def get_movies_per_year(self, released_column=COLUMN_RELEASE_DATE):
        """
        Print the number of movies released in each year.

        Parameters:
            released_column (str, optional): Name of the column which contains the information for movie release dates.
                Defaults to 'release_date'.
        """
        if released_column not in self.columns:
            return self._log_error_and_return(f"Error: '{released_column}' column not found.")

        # Extract year from release_date column
        self['release_year'] = pd.to_datetime(
            self[released_column]).dt.strftime("%Y")

        # Count occurrences of films for each year
        films_per_year = self['release_year'].value_counts().sort_index()

        # Print the number of films released in each year
        # self.logger.info(f"Number of films released in each year:\n{
        #                  films_per_year}")
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
            self.to_json(file_path, orient=orient)
            self.logger.info(f"File {file_path} successfully written.")
        except Exception as e:
            self.logger.error(f"Error writing to file: {e}")
            raise

    def _parsing_stringified_json(self, cell):
        """
        Parse stringified JSON in a DataFrame cell.

        Args:
            cell (str): The cell containing the stringified JSON.

        Returns:
            Parsed JSON or np.nan if the cell is empty or NaN.
        """
        cell = ast.literal_eval(cell)
        if cell == [] or (isinstance(cell, float) and np.isnan(cell)):
            return np.nan
        else:
            return cell

    def clean_dataset(self, drop_columns=["homepage", "poster_path", "video", "imdb_id", "overview", "original_title",
                                          "spoken_languages", "tagline", "adult", "belongs_to_collection", "status", "runtime", "production_companies", "production_countries"]):
        """
        Cleans the dataset by performing various operations.

        Parameters:
            drop_columns (list(str), optional): List of name of the columns which will be remobed entierly.
                Defaults to ["homepage", "poster_path", "video", "imdb_id", "overview", "original_title",
                            "spoken_languages", "tagline", "adult", "belongs_to_collection", "status",
                            "runtime", "production_companies", "production_countries"].
        Returns:
            MovieDataSet: The cleaned DataFrame.
        """

        try:
            self.logger.info(f"Removing columns: {drop_columns} ...")
            self.drop(drop_columns, axis=1, inplace=True)

            self.logger.info("Removing duplicated records...")
            self.drop_duplicates(keep='first', inplace=True)

            self.logger.info("Removing empty rows ...")
            self.dropna(how="all", inplace=True)

            self.logger.info("Replacing empty titles with NaN ...")
            self.dropna(subset=["title"], inplace=True)

            self.logger.info("Converting 'id' column to integer ...")
            self["id"] = pd.to_numeric(
                self['id'], errors='coerce', downcast="integer")

            self.logger.info("Converting 'popularity' column to float ...")
            self["popularity"] = pd.to_numeric(
                self['popularity'], errors='coerce', downcast="float")

            self.logger.info("Converting 'budget' column to float ...")
            self["budget"] = pd.to_numeric(
                self['budget'], errors='coerce', downcast="float")

            self.logger.info("Replacing empty 'release_date' with NaN ...")
            self.dropna(subset=["release_date"], inplace=True)

            self.logger.info(
                "Replacing empty 'original_language' with NaN ...")
            self.dropna(subset=["original_language"], inplace=True)

            self.logger.info("Converting 'release_date' values to datetime")
            self['release_date'] = pd.to_datetime(self['release_date'])

            self.logger.info(
                "Creating column 'release_year' with year property of 'release_date' in string format ...")
            self['release_year'] = self['release_date'].dt.strftime("%Y")

            self.logger.info("Parsing content of the 'genres' column")
            self['genres'] = self['genres'].apply(
                self._parsing_stringified_json)

            self.logger.info("Information of final dataset")
            self.info()

            return self
        except Exception as e:
            self.logger.error(
                f"An error occurred while cleaning the dataset: {e}")
