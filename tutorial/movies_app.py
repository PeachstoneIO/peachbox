import peachbox
import processors


movies_app = peachbox.App()

movies_app.importers = [processors.ImportReviews]

movies_app.run()


