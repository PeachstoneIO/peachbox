#!/usr/bin/env python

# general
from os import path
# peachbox
import peachbox
# tutorial
from tasks import importer, streams, batch_views

movies_app = peachbox.App("MovieReviews")

# Set up the data warehouse with local file system and path
# for tutorial
dwh = peachbox.DWH.Instance()
dwh.fs = peachbox.fs.Local()
dwh.fs.dwh_path = path.join(path.dirname(path.realpath(__file__)), 'dwh')


## Import data into master data set 
import_movie_reviews  = importer.ImportMovieReviews()

## Batch view 
reviews_batch_view    = batch_views.Reviews()
reviews_shottime_view = batch_views.Reviews()


## Example of streaming task
## Is just executed, runs permanently
reviews_real_time_view = streams.Reviews()
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

