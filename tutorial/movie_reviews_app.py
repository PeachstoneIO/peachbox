#!/usr/bin/env python
import peachbox
import tasks.importer
import tasks.streams
import tasks.batch_views


movies_app = peachbox.App("MovieReviews")


# Set up the data warehouse with local file system and path
# for tutorial
import os.path
dwh = peachbox.DWH.Instance()
dwh.fs = peachbox.fs.Local()
dwh.fs.dwh_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'dwh')


## Import data into master data set 
import_movie_reviews = tasks.importer.ImportMovieReviews()

## Batch view 
reviews_batch_view = tasks.batch_views.Reviews()

reviews_shottime_view = tasks.batch_views.Reviews()


## Example of streaming task
## Is just executed, runs permanently
reviews_real_time_view = tasks.streams.Reviews()
reviews_real_time_view.execute()

# Define events and dependencies
start_importer    = peachbox.scheduler.ConditionalEvent("ImportMovieReviewsStart")
every_5_minutes   = peachbox.scheduler.PeriodicEvent("Every5Minutes", 3*60)
importer_finished = peachbox.scheduler.Event("ImportMovieReviewsFinished")
start_importer.subscribe(every_5_minutes)
start_importer.subscribe(importer_finished)

movies_app.scheduler.subscribe(task=import_movie_reviews, event=start_importer)
movies_app.scheduler.subscribe(task=reviews_batch_view, event=importer_finished)

movies_app.run()

