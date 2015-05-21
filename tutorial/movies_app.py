import peachbox
import tasks.importer
import tasks.streams
import tasks.batch_views


#movies_app = peachbox.App()
#movies_app.importers = [processors.ImportReviews]
#movies_app.run()


# Set up the data warehouse with local file system and path
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


# Example of master data set import
# review_by_user = tasks.importer.ImportMovieReviews()
# review_by_user.execute()

# Example of streaming task
# review_by_user_real_time = tasks.streams.MovieReviews()
# review_by_user_real_time.execute({'path':'/'})

# Example of batch view
tasks.batch_views.ReviewByGender().execute({'smallest_key':904608000})


