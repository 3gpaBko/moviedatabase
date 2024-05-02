import logging
from movieDataSet import MovieDataSet

if __name__ == "__main__":
    #   Logging configuration
    logging.basicConfig(level=logging.INFO)

    #   Loading Initial dataset
    df = MovieDataSet(
        filepath_or_buffer="movies_metadata.csv", low_memory=False)

    #   Clean dataset
    df = df.clean_dataset()

    #   Display unique movies count
    df.get_unique_movies_count()

    #   Display average rating by column
    df.get_average_rating_by_column()

    #   Display count of movies per year
    df.get_movies_per_year()

    #   Export dataset as JSON file
    df.save_json("cleaned_dataset.json", orient="records")
