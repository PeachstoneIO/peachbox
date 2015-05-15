import peachbox
import tasks.importer

#movies_app = peachbox.App()
#movies_app.importers = [processors.ImportReviews]
#movies_app.run()


# Set up the data warehouse with local file system and path
dwh = peachbox.DWH.Instance()
dwh.fs = peachbox.fs.Local()
dwh.fs.dwh_path = 'dwh'

review_by_user = tasks.importer.ImportMovieReviews()
review_by_user.execute()

every_ten_minutes = TimedEvent("EveryTenMinutes")

start_import = ConditionalEvent("StartImport")

start_import.subscribes("EveryTenMinutes")
start_import.subscribes("ImportMovieReviewsFinished")

Event("ImportMovieReviewsStart").subscribes("StartImport")



movies_app.scheduler
