import peachbox
import tasks.importer
import tasks.streams
import tasks.batch_views


movies_app = peachbox.App("MovieReviews")
#movies_app.importers = [processors.ImportReviews]


# Set up the data warehouse with local file system and path
# for tutorial
import os.path
dwh = peachbox.DWH.Instance()
dwh.fs = peachbox.fs.Local()
dwh.fs.dwh_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'dwh')

# Example for scheduler
# every_ten_minutes = TimedEvent("EveryTenMinutes")
# start_import = ConditionalEvent("StartImport")
# start_import.subscribes("EveryTenMinutes")
# start_import.subscribes("ImportMovieReviewsFinished")
# Event("ImportMovieReviewsStart").subscribes("StartImport")



# Import data into master data set 
import_movie_reviews = tasks.importer.ImportMovieReviews()

# Example of streaming task
# review_by_user_real_time = tasks.streams.MovieReviews()
# review_by_user_real_time.execute()

# Example of batch view
#r = tasks.batch_views.ReviewsByGender().execute() #{'smallest_key':904608000, 'biggest_key':909792000})

# Define events and dependencies
start_importer    = peachbox.scheduler.ConditionalEvent("ImportMovieReviewsStart")

every_5_minutes   = peachbox.scheduler.PeriodicEvent("Every5Minutes", 30)
importer_finished = peachbox.scheduler.Event("ImportMovieReviewsFinished")
start_importer.subscribe(every_5_minutes)
start_importer.subscribe(importer_finished)

movies_app.scheduler.subscribe(task=import_movie_reviews, event=start_importer)

movies_app.run()

